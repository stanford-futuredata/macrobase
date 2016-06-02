package macrobase.ingest;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;
import java.util.List;


public class DiskCachingIngester extends DataIngester {
    private static final Logger log = LoggerFactory.getLogger(DiskCachingIngester.class);

    private final String fileDir;
    private DataIngester innerIngester;
    private MBStream<Datum> output;

    public DiskCachingIngester(MacroBaseConf conf, DataIngester innerIngester) throws ConfigurationException, IOException {
        super(conf);
        this.innerIngester = innerIngester;

        fileDir = conf.getString(MacroBaseConf.DB_CACHE_DIR);
        File cacheDir = new File(fileDir);
        if (!cacheDir.exists()) {
            cacheDir.mkdirs();
        }
    }

    @Override
    public String getBaseQuery() {
        return innerIngester.getBaseQuery();
    }

    @Override
    public MBStream<Datum> getStream() throws Exception {
        if(output == null) {
            initialize();
        }

        return output;
    }


    private static class CachedData {
        private DatumEncoder encoder;
        private List<Datum> data;

        public CachedData() {
        }

        public CachedData(DatumEncoder encoder, List<Datum> data) {
            this.encoder = encoder;
            this.data = data;
        }

        public DatumEncoder getEncoder() {
            return encoder;
        }

        public List<Datum> getData() {
            return data;
        }
    }


    private void initialize() throws Exception {
        if(output == null) {
            List<Datum> data;
            data = readInData();
            if (data == null || data.size() == 0) {
		data = innerIngester.getStream().drain();
                log.info("Writing out loaded data...");
                writeOutData(data);
                log.info("...done writing!");
            }
	    output = new MBStream<>();
	    output.add(data);
        }
    }

    private String convertFileName(Integer timeColumn,
                                   List<String> attributes,
                                   List<String> lowMetrics,
                                   List<String> highMetrics,
                                   List<String> contextualDiscreteAttributes,
                                   List<String> contextualDoubleAttributes,
                                   String baseQuery) {
        String timeColumnName = conf.getEncoder().getAttributeName(timeColumn);
        int hashCode = String.format("T-%s::A-%s::L%s::H%s::CDis%s::CDou%s::BQ%s",
                timeColumnName,
                attributes.toString(),
                lowMetrics.toString(),
                highMetrics.toString(),
                contextualDiscreteAttributes.toString(),
                contextualDoubleAttributes.toString(),
                baseQuery).replace(" ", "_").hashCode();
        return Integer.toString(hashCode);
    }

    private void writeOutData(List<Datum> data) throws IOException {
        CachedData d = new CachedData(conf.getEncoder(), data);

        OutputStream outputStream = new SnappyOutputStream(new BufferedOutputStream(new FileOutputStream(
                fileDir + "/" + convertFileName(timeColumn,
                        attributes,
                        lowMetrics,
                        highMetrics,
                        contextualDiscreteAttributes,
                        contextualDoubleAttributes,
                        innerIngester.getBaseQuery()))), 16384);

        Kryo kryo = new Kryo();
        Output output = new Output(outputStream);
        kryo.writeObject(output, d);
        output.close();
    }

    private List<Datum> readInData() throws IOException {
        File f = new File(fileDir + "/" + convertFileName(timeColumn,
                attributes,
                lowMetrics,
                highMetrics,
                contextualDiscreteAttributes,
                contextualDoubleAttributes,
                this.innerIngester.getBaseQuery()));
        if (!f.exists()) {
            log.info("Data did not exist; going to read from SQL.");
            return null;
        }

        log.info("On-disk cache exists; loading...");
        InputStream inputStream = new SnappyInputStream(new BufferedInputStream(new FileInputStream(f), 16384));

        Kryo kryo = new Kryo();
        Input input = new Input(inputStream);
        CachedData cachedData = kryo.readObject(input, CachedData.class);
        log.info("...loaded!");

        conf.getEncoder().copy(cachedData.getEncoder());
        return cachedData.getData();
    }

}
