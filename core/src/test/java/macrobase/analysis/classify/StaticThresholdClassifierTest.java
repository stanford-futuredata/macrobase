package macrobase.analysis.classify;

import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;


public class StaticThresholdClassifierTest {
    @Test
    public void simpleTest() {
        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.OUTLIER_STATIC_THRESHOLD, 49.99);

        List<Datum> data = new ArrayList<>();
        for(double i = 0; i < 100; ++i) {
            data.add(new Datum(new ArrayList<>(), i));
        }

        StaticThresholdClassifier sc = new StaticThresholdClassifier(conf);
        sc.initialize();
        sc.consume(data);
        List<OutlierClassificationResult> results = sc.getStream().drain();
        sc.shutdown();
        
        int outlierCount = 0;
        for(OutlierClassificationResult oc : results) {
            if(oc.isOutlier()) {
                assertTrue(oc.getDatum().metrics().getNorm() > 49.99);
                outlierCount++;
            } else {
                assertTrue(oc.getDatum().metrics().getNorm() < 49.99);
            }
        }

        assertEquals(outlierCount, 50);
    }
}
