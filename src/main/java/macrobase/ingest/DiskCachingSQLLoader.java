package macrobase.ingest;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import macrobase.ingest.transform.DataTransformation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.SQLException;
import java.util.List;


public abstract class DiskCachingSQLLoader extends SQLLoader {
    private static final Logger log = LoggerFactory.getLogger(DiskCachingSQLLoader.class);

    private final String fileDir;

    public DiskCachingSQLLoader(MacroBaseConf conf) throws ConfigurationException, SQLException {
        super(conf);

        fileDir = conf.getString(MacroBaseConf.DB_CACHE_DIR);
        File cacheDir = new File(fileDir);
        if (!cacheDir.exists()) {
            cacheDir.mkdirs();
        }
    }

    private static class CachedData {
        private DatumEncoder encoder;
        private List<Datum> data;

        public CachedData() {}

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

    private String convertFileName(List<String> attributes,
                                   List<String> lowMetrics,
                                   List<String> highMetrics,
                                   String baseQuery) {
        int hashCode = String.format("A-%s::L%s::H%s::BQ%s",
                             attributes.toString(),
                             lowMetrics.toString(),
                             highMetrics.toString(),
                             baseQuery).replace(" ", "_").hashCode();
         return Integer.toString(hashCode);
    }

    private void writeOutData(DatumEncoder encoder,
                              List<String> attributes,
                              List<String> lowMetrics,
                              List<String> highMetrics,
                              String baseQuery,
                              List<Datum> data) throws IOException {
        CachedData d = new CachedData(encoder, data);

        OutputStream outputStream = new SnappyOutputStream(
                new BufferedOutputStream(new FileOutputStream(fileDir + "/" + convertFileName(attributes,
                                                                                              lowMetrics,
                                                                                              highMetrics,
                                                                                              baseQuery))), 16384);

        Kryo kryo = new Kryo();
        Output output = new Output(outputStream);
        kryo.writeObject(output, d);
        output.close();
    }

    private List<Datum> readInData(DatumEncoder encoder,
                                   List<String> attributes,
                                   List<String> lowMetrics,
                                   List<String> highMetrics,
                                   String baseQuery) throws IOException {
        File f = new File(fileDir + "/" + convertFileName(attributes,
                                                          lowMetrics,
                                                          highMetrics,
                                                          baseQuery));
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

        encoder.copy(cachedData.getEncoder());
        return cachedData.getData();
    }

    @Override
    public List<Datum> getData(DatumEncoder encoder) throws SQLException, IOException {
        List<Datum> data = readInData(encoder, attributes, lowMetrics, highMetrics, baseQuery);
        if (data != null) {
            return data;
        }

        data = super.getData(encoder);
        log.info("Writing out loaded data...");
        writeOutData(encoder, attributes, lowMetrics, highMetrics, baseQuery, data);
        log.info("...done writing!");

        return data;
    }
}
