package macrobase.ingest;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.*;
import java.util.ArrayList;
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
    public MBStream<Datum> getStream() throws Exception {
        if (output == null) {
            initialize();
        }

        return output;
    }

    private void initialize() throws Exception {
        if (output == null) {
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
                                   List<String> metrics,
                                   String baseQuery) {
        String timeColumnName = conf.getEncoder().getAttributeName(timeColumn);
        int hashCode = String.format("T-%s::A-%s::M%s::BQ%s",
                                     timeColumnName,
                                     attributes.toString(),
                                     metrics.toString(),
                                     baseQuery).replace(" ", "_").hashCode();
        return Integer.toString(hashCode);
    }

    private void writeOutData(List<Datum> data) throws IOException {
        OutputStream outputStream = new SnappyOutputStream(new BufferedOutputStream(new FileOutputStream(
                fileDir + "/" + convertFileName(timeColumn,
                                                attributes,
                                                metrics,
                                                conf.getString(MacroBaseConf.BASE_QUERY,
                                                               conf.getString(MacroBaseConf.QUERY_NAME,
                                                                              "cachedQuery"))))), 16384);

        Kryo kryo = new Kryo();
        Output output = new Output(outputStream);
        kryo.writeObject(output, conf.getEncoder());

        final int BATCHSIZE = conf.getInt(MacroBaseConf.DB_CACHE_CHUNK_SIZE,
                                          MacroBaseDefaults.DB_CACHE_CHUNK_SIZE);
        List<List<Datum>> batches = new ArrayList<>();

        if (data.size() > BATCHSIZE) {
            int idx = 0;
            while (idx != data.size()) {
                List<Datum> batch = new ArrayList<>();
                for (int j = 0; j < BATCHSIZE && idx != data.size(); ++j) {
                    batch.add(data.get(idx));
                    idx++;
                }
                batches.add(batch);
            }
        } else {
            batches.add(data);
        }

        kryo.writeObject(output, batches.size());
        for (List<Datum> batch : batches) {
            kryo.writeClassAndObject(output, batch);
        }

        output.close();
    }

    private List<Datum> readInData() throws IOException {
        File f = new File(fileDir + "/" + convertFileName(timeColumn,
                                                          attributes,
                                                          metrics,
                                                          conf.getString(MacroBaseConf.BASE_QUERY,
                                                                         conf.getString(MacroBaseConf.QUERY_NAME,
                                                                                        "cachedQuery"))));
        if (!f.exists()) {
            log.info("Data did not exist; going to read from SQL.");
            return null;
        }

        log.info("On-disk cache exists; loading...");
        InputStream inputStream = new SnappyInputStream(new BufferedInputStream(new FileInputStream(f), 16384));

        Kryo kryo = new Kryo();
        Input input = new Input(inputStream);

        DatumEncoder cachedEncoder = kryo.readObject(input, DatumEncoder.class);

        Integer numBatches = kryo.readObject(input, Integer.class);
        List<Datum> output = null;

        for (int i = 0; i < numBatches; ++i) {
            List<Datum> fromDisk = (List<Datum>) kryo.readClassAndObject(input);
            if (output == null) {
                output = fromDisk;
            } else {
                output.addAll(fromDisk);
            }
        }

        log.info("...loaded!");

        conf.getEncoder().copy(cachedEncoder);
        return output;
    }

}
