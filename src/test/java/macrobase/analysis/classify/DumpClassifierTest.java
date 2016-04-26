package macrobase.analysis.classify;

import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DumpClassifierTest {

    @Test
    public void testDumpSmall() throws IOException {
        MacroBaseConf conf = new MacroBaseConf();
        ArrayList<Integer> attrs = new ArrayList<>();
        attrs.add(1);
        attrs.add(2);
        OutlierClassifier dummy = new OutlierClassifier() {
            int num = 2;

            @Override
            public boolean hasNext() {
                return num>0;
            }

            @Override
            public OutlierClassificationResult next() {
                num--;
                return new OutlierClassificationResult(
                        new Datum(attrs, new ArrayRealVector()),
                        true
                );
            }
        };

        DumpClassifier dumper = new DumpClassifier(conf, dummy, "testDumpSmall");
        assertTrue(dumper.hasNext());
        OutlierClassificationResult first = dumper.next();
        assertEquals(true, first.isOutlier());
        assertEquals(1, first.getDatum().getAttributes().get(0).intValue());
        while (dumper.hasNext()) {
            dumper.next();
        }

        Path filePath = Paths.get(dumper.getFilePath());
        assertTrue(Files.exists(filePath));
        Files.deleteIfExists(Paths.get(dumper.getFilePath()));
    }
}
