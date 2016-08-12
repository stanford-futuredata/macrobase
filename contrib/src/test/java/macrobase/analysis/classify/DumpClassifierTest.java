package macrobase.analysis.classify;

import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DumpClassifierTest {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testDumpSmall() throws Exception {
        MacroBaseConf conf = new MacroBaseConf();
        ArrayList<Integer> attrs = new ArrayList<>();
        attrs.add(1);
        attrs.add(2);
        OutlierClassifier dummy = new OutlierClassifier() {

            MBStream<OutlierClassificationResult> result = new MBStream<>();

            @Override
            public MBStream<OutlierClassificationResult> getStream() throws Exception {

                return result;
            }

            @Override
            public void initialize() throws Exception {

            }

            @Override
            public void consume(List<Datum> records) throws Exception {
                for(Datum r : records) {
                    result.add(new OutlierClassificationResult(r, true));
                }
            }

            @Override
            public void shutdown() throws Exception {

            }
        };

        File f = folder.newFile("testDumpSmall");

        DumpClassifier dumper = new DumpClassifier(conf, dummy, f.getAbsolutePath());
        List<Datum> data = new ArrayList<>();
        data.add(new Datum(attrs, new ArrayRealVector()));
        data.add(new Datum(attrs, new ArrayRealVector()));
        dumper.consume(data);

        List<OutlierClassificationResult> results = dumper.getStream().drain();
        dumper.shutdown();

        assertEquals(results.size(), data.size());
        OutlierClassificationResult first = results.get(0);
        assertEquals(true, first.isOutlier());
        assertEquals(1, first.getDatum().attributes().get(0).intValue());

        Path filePath = Paths.get(dumper.getFilePath());
        assertTrue(Files.exists(filePath));
        Files.deleteIfExists(Paths.get(dumper.getFilePath()));
    }
}
