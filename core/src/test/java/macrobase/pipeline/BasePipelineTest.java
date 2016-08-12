package macrobase.pipeline;

import macrobase.analysis.stats.*;
import macrobase.conf.MacroBaseConf;
import macrobase.ingest.CSVIngester;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import static org.junit.Assert.assertTrue;

public class BasePipelineTest {
    public static int countLines(String filename) throws IOException {
        InputStream is = new BufferedInputStream(new FileInputStream(filename));
        try {
            byte[] c = new byte[1024];
            int count = 0;
            int readChars = 0;
            boolean empty = true;
            while ((readChars = is.read(c)) != -1) {
                empty = false;
                for (int i = 0; i < readChars; ++i) {
                    if (c[i] == '\n') {
                        ++count;
                    }
                }
            }
            return (count == 0 && !empty) ? 1 : count;
        } finally {
            is.close();
        }
    }

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testConstructors() throws Exception {
        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.ATTRIBUTES, new ArrayList<>());
        conf.set(MacroBaseConf.METRICS, new ArrayList<>());
        conf.set(MacroBaseConf.BASE_QUERY, "SELECT * FROM test;");

        conf.set(MacroBaseConf.DATA_LOADER_TYPE,
                 MacroBaseConf.DataIngesterType.CSV_LOADER);
        conf.set(MacroBaseConf.CSV_INPUT_FILE, folder.newFile("dummy").getAbsolutePath());

        assertTrue(conf.constructIngester() instanceof CSVIngester);

        conf.set(MacroBaseConf.TRANSFORM_TYPE,
                 MacroBaseConf.TransformType.MAD);
        assertTrue(conf.constructTransform() instanceof MAD);

        conf.set(MacroBaseConf.TRANSFORM_TYPE,
                 MacroBaseConf.TransformType.MCD);
        assertTrue(conf.constructTransform() instanceof MinCovDet);

        conf.set(MacroBaseConf.TRANSFORM_TYPE,
                 MacroBaseConf.TransformType.ZSCORE);
        assertTrue(conf.constructTransform() instanceof ZScore);

        conf.set(MacroBaseConf.TRANSFORM_TYPE,
                 "macrobase.pipeline.MockTransform");
        assertTrue(conf.constructTransform() instanceof MockTransform);

    }

}
