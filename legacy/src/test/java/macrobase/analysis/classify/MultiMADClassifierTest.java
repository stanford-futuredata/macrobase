package macrobase.analysis.classify;

import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.datamodel.Datum;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;


public class MultiMADClassifierTest {
    @Test
    public void simpleTest() {
        List<Datum> data = new ArrayList<>();
        for(double i = 0; i < 100; ++i) {
            data.add(new Datum(new ArrayList<>(), i, (i+10)%100));
        }

        MultiMADClassifier mad = new MultiMADClassifier(10);
        mad.initialize();
        mad.consume(data);
        List<OutlierClassificationResult> results = mad.getStream().drain();
        mad.shutdown();
        
        int outlierCount = 0;
        for(OutlierClassificationResult oc : results) {
            if(oc.isOutlier()) {
                assertTrue(oc.getDatum().metrics().getEntry(0) >= 98 ||
                    oc.getDatum().metrics().getEntry(0) <= 1 ||
                    oc.getDatum().metrics().getEntry(1) >= 98 ||
                    oc.getDatum().metrics().getEntry(1) <= 1);
                outlierCount++;
            } else {
                assertTrue(oc.getDatum().metrics().getEntry(0) < 98 && 
                    oc.getDatum().metrics().getEntry(0) > 1 &&
                    oc.getDatum().metrics().getEntry(1) < 98 && 
                    oc.getDatum().metrics().getEntry(1) > 1);
            }
        }

        assertEquals(outlierCount, 8);
    }
}
