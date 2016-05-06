package macrobase.analysis.contextualoutlier;

import static junit.framework.TestCase.assertEquals;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import macrobase.analysis.result.OutlierClassificationResult;
import org.apache.commons.math3.linear.ArrayRealVector;
import org.junit.Test;

import macrobase.analysis.classify.OutlierClassifier;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;

public class ContextualOutlierDetectorTest {

    @Test
    public void testContextualDiscreteAttribute() throws Exception {
        //construct a contextual outlier detector
        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.LOW_METRICS, new ArrayList<>());
        conf.set(MacroBaseConf.HIGH_METRICS, Arrays.asList("A1"));
        conf.set(MacroBaseConf.TRANSFORM_TYPE, "MAD");
        List<String> contextualDiscreteAttributes = new ArrayList<String>();
        contextualDiscreteAttributes.add("C1_Discrete");
        List<String> contextualDoubleAttributes = new ArrayList<String>();
        conf.set(MacroBaseConf.CONTEXTUAL_DISCRETE_ATTRIBUTES, contextualDiscreteAttributes);
        conf.set(MacroBaseConf.CONTEXTUAL_DOUBLE_ATTRIBUTES, contextualDoubleAttributes);
        conf.set(MacroBaseConf.CONTEXTUAL_DENSECONTEXTTAU, 0.4);
        conf.set(MacroBaseConf.CONTEXTUAL_NUMINTERVALS, 10);
        conf.set(MacroBaseConf.OUTLIER_STATIC_THRESHOLD, 3.0);
        conf.getEncoder().recordAttributeName(1, "A1");
        conf.getEncoder().recordAttributeName(2, "C1_Discrete");
        ContextualOutlierDetector contextualDetector = new ContextualOutlierDetector(conf);
        List<Datum> data = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            double[] sample = new double[1];
            sample[0] = i;
            Integer[] c1 = new Integer[1];
            if (i < 5) {
                c1[0] = 1;
            } else if (i >= 5 && i < 50) {
                c1[0] = 2;
            } else {
                c1[0] = 1;
            }
            data.add(new Datum(new ArrayList<>(), new ArrayRealVector(sample),
                               new ArrayList<Integer>(Arrays.asList(c1)),
                               new ArrayRealVector()));
        }
        Map<Context, List<OutlierClassificationResult>> context2Outliers = contextualDetector.searchContextualOutliers(data);
        assertEquals(context2Outliers.size(), 1);
        for (Context context : context2Outliers.keySet()) {
            List<Interval> intervals = context.getIntervals();
            assertEquals(intervals.size(), 1);
            assertEquals(intervals.get(0).getColumnName(), "C1_Discrete");
            assertEquals(intervals.get(0) instanceof IntervalDiscrete, true);
            assertEquals(((IntervalDiscrete) intervals.get(0)).getValue(), 1);
        }
    }

    @Test
    public void testContextualDoubleAttribute() throws Exception {
        //construct a contextual outlier detector
        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.LOW_METRICS, new ArrayList<>());
        conf.set(MacroBaseConf.HIGH_METRICS, Arrays.asList("A1"));
        conf.set(MacroBaseConf.TRANSFORM_TYPE, "MAD");
        conf.set(MacroBaseConf.OUTLIER_STATIC_THRESHOLD, 3.0);
        List<String> contextualDiscreteAttributes = new ArrayList<String>();
        List<String> contextualDoubleAttributes = new ArrayList<String>();
        contextualDoubleAttributes.add("C1_Double");
        conf.set(MacroBaseConf.CONTEXTUAL_DISCRETE_ATTRIBUTES, contextualDiscreteAttributes);
        conf.set(MacroBaseConf.CONTEXTUAL_DOUBLE_ATTRIBUTES, contextualDoubleAttributes);
        conf.set(MacroBaseConf.CONTEXTUAL_DENSECONTEXTTAU, 0.4);
        conf.set(MacroBaseConf.CONTEXTUAL_NUMINTERVALS, 10);
        ContextualOutlierDetector contextualDetector = new ContextualOutlierDetector(conf);
        List<Datum> data = new ArrayList<>();
        for (int i = 0; i < 100; ++i) {
            double[] sample = new double[1];
            sample[0] = i;
            double[] c1 = new double[1];
            if (i < 5) {
                c1[0] = 1;
            } else if (i >= 5 && i < 50) {
                c1[0] = 100;
            } else {
                c1[0] = 1;
            }
            data.add(new Datum(new ArrayList<>(), new ArrayRealVector(sample),
                               new ArrayList<Integer>(),
                               new ArrayRealVector(c1)));
        }
        Map<Context, List<OutlierClassificationResult>> context2Outliers = contextualDetector.searchContextualOutliers(data);
        assertEquals(context2Outliers.size(), 1);
        for (Context context : context2Outliers.keySet()) {
            List<Interval> intervals = context.getIntervals();
            assertEquals(intervals.size(), 1);
            assertEquals(intervals.get(0).getColumnName(), "C1_Double");
            assertEquals(intervals.get(0) instanceof IntervalDouble, true);
            assertEquals(((IntervalDouble) intervals.get(0)).getMin(), 1.0);
            assertEquals(((IntervalDouble) intervals.get(0)).getMax(), 10.9);
        }
    }

    @Test
    public void testTwoAttributesContext() throws Exception {
        //construct a contextual outlier detector
        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.LOW_METRICS, new ArrayList<>());
        conf.set(MacroBaseConf.HIGH_METRICS, Arrays.asList("A1"));
        conf.set(MacroBaseConf.TRANSFORM_TYPE, "MAD");
        conf.set(MacroBaseConf.OUTLIER_STATIC_THRESHOLD, 3.0);
        List<String> contextualDiscreteAttributes = new ArrayList<String>();
        contextualDiscreteAttributes.add("C1_Discrete");
        List<String> contextualDoubleAttributes = new ArrayList<String>();
        contextualDoubleAttributes.add("C2_Double");
        conf.set(MacroBaseConf.CONTEXTUAL_DISCRETE_ATTRIBUTES, contextualDiscreteAttributes);
        conf.set(MacroBaseConf.CONTEXTUAL_DOUBLE_ATTRIBUTES, contextualDoubleAttributes);
        conf.set(MacroBaseConf.CONTEXTUAL_DENSECONTEXTTAU, 0.3);
        conf.set(MacroBaseConf.CONTEXTUAL_NUMINTERVALS, 10);
        ContextualOutlierDetector contextualDetector = new ContextualOutlierDetector(conf);
        List<Datum> data = new ArrayList<>();
        for (int i = 0; i < 120; ++i) {
            double[] sample = new double[1];
            sample[0] = i;
            Integer[] c1 = new Integer[1];
            if (i < 80) {
                c1[0] = 1;
            } else {
                c1[0] = 2;
            }
            double[] c2 = new double[1];
            if (i < 40) {
                c2[0] = 1.0;
            } else if (i >= 40 && i < 79) {
                c2[0] = 100.0;
            } else if (i >= 79 && i < 80) {
                c2[0] = 1.0;
            } else {
                c2[0] = 1.0;
            }
            data.add(new Datum(new ArrayList<>(), new ArrayRealVector(sample),
                               new ArrayList<Integer>(Arrays.asList(c1)),
                               new ArrayRealVector(c2)));
        }
        Map<Context, List<OutlierClassificationResult>> context2Outliers = contextualDetector.searchContextualOutliers(data);
        assertEquals(context2Outliers.size(), 1);
        for (Context context : context2Outliers.keySet()) {
            List<Interval> intervals = context.getIntervals();
            assertEquals(intervals.size(), 2);
            assertEquals(intervals.get(0).getColumnName(), "C1_Discrete");
            assertEquals(intervals.get(0) instanceof IntervalDiscrete, true);
            assertEquals(((IntervalDiscrete) intervals.get(0)).getValue(), 1);
            assertEquals(intervals.get(1).getColumnName(), "C2_Double");
            assertEquals(intervals.get(1) instanceof IntervalDouble, true);
            assertEquals(((IntervalDouble) intervals.get(1)).getMin(), 1.0);
            assertEquals(((IntervalDouble) intervals.get(1)).getMax(), 10.9);
        }
    }

    @Test
    public void testTwoAttributesContext2() throws Exception {
        //construct a contextual outlier detector
        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.LOW_METRICS, new ArrayList<>());
        conf.set(MacroBaseConf.HIGH_METRICS, Arrays.asList("A1"));
        conf.set(MacroBaseConf.TRANSFORM_TYPE, "MAD");
        conf.set(MacroBaseConf.OUTLIER_STATIC_THRESHOLD, 3.0);
        List<String> contextualDiscreteAttributes = new ArrayList<String>();
        contextualDiscreteAttributes.add("C1_Discrete");
        List<String> contextualDoubleAttributes = new ArrayList<String>();
        contextualDoubleAttributes.add("C2_Double");
        conf.set(MacroBaseConf.CONTEXTUAL_DISCRETE_ATTRIBUTES, contextualDiscreteAttributes);
        conf.set(MacroBaseConf.CONTEXTUAL_DOUBLE_ATTRIBUTES, contextualDoubleAttributes);
        conf.set(MacroBaseConf.CONTEXTUAL_DENSECONTEXTTAU, 0.3);
        conf.set(MacroBaseConf.CONTEXTUAL_NUMINTERVALS, 10);
        conf.set(MacroBaseConf.CONTEXTUAL_MAX_PREDICATES, 1);
        ContextualOutlierDetector contextualDetector = new ContextualOutlierDetector(conf);
        List<Datum> data = new ArrayList<>();
        for (int i = 0; i < 120; ++i) {
            double[] sample = new double[1];
            sample[0] = i;
            Integer[] c1 = new Integer[1];
            if (i < 80) {
                c1[0] = 1;
            } else {
                c1[0] = 2;
            }
            double[] c2 = new double[1];
            if (i < 40) {
                c2[0] = 1.0;
            } else if (i >= 40 && i < 79) {
                c2[0] = 100.0;
            } else if (i >= 79 && i < 80) {
                c2[0] = 1.0;
            } else {
                c2[0] = 1.0;
            }
            data.add(new Datum(new ArrayList<>(), new ArrayRealVector(sample),
                               new ArrayList<Integer>(Arrays.asList(c1)),
                               new ArrayRealVector(c2)));
        }
        Map<Context, List<OutlierClassificationResult>> context2Outliers = contextualDetector.searchContextualOutliers(data);
        assertEquals(context2Outliers.size(), 0);
    }

    @Test
    public void testContextualGivenOutliers() throws Exception {
        //construct a contextual outlier detector
        MacroBaseConf conf = new MacroBaseConf();
        conf.set(MacroBaseConf.LOW_METRICS, new ArrayList<>());
        conf.set(MacroBaseConf.HIGH_METRICS, Arrays.asList("A1"));
        conf.set(MacroBaseConf.TRANSFORM_TYPE, "MAD");
        List<String> contextualDiscreteAttributes = new ArrayList<String>();
        contextualDiscreteAttributes.add("C1_Discrete");
        List<String> contextualDoubleAttributes = new ArrayList<String>();
        conf.set(MacroBaseConf.CONTEXTUAL_DISCRETE_ATTRIBUTES, contextualDiscreteAttributes);
        conf.set(MacroBaseConf.CONTEXTUAL_DOUBLE_ATTRIBUTES, contextualDoubleAttributes);
        conf.set(MacroBaseConf.CONTEXTUAL_DENSECONTEXTTAU, 0.4);
        conf.set(MacroBaseConf.CONTEXTUAL_NUMINTERVALS, 10);
        conf.set(MacroBaseConf.OUTLIER_STATIC_THRESHOLD, 3.0);
        conf.getEncoder().recordAttributeName(1, "A1");
        conf.getEncoder().recordAttributeName(2, "C1_Discrete");
        ContextualOutlierDetector contextualDetector = new ContextualOutlierDetector(conf);
        List<Datum> data = new ArrayList<>();
        List<Datum> inputOutliers = new ArrayList<Datum>();
        for (int i = 0; i < 100; ++i) {
            double[] sample = new double[1];
            sample[0] = i;
            Integer[] c1 = new Integer[1];
            if (i < 5) {
                c1[0] = 1;
            } else if (i >= 5 && i < 50) {
                c1[0] = 2;
            } else {
                c1[0] = 1;
            }
            ArrayList<Integer> contexutalDiscreteIntegers = new ArrayList<Integer>();
            contexutalDiscreteIntegers.add(c1[0]);
            Datum datum = new Datum(new ArrayList<>(), new ArrayRealVector(sample),
                                    contexutalDiscreteIntegers,
                                    new ArrayRealVector());
            data.add(datum);
            if (i < 3) {
                inputOutliers.add(datum);
            }
        }
        Map<Context, List<OutlierClassificationResult>> context2Outliers = contextualDetector.searchContextGivenOutliers(data,
                                                                                                         inputOutliers);
        assertEquals(context2Outliers.size(), 1);
        for (Context context : context2Outliers.keySet()) {
            List<Interval> intervals = context.getIntervals();
            assertEquals(intervals.size(), 1);
            assertEquals(intervals.get(0).getColumnName(), "C1_Discrete");
            assertEquals(intervals.get(0) instanceof IntervalDiscrete, true);
            assertEquals(((IntervalDiscrete) intervals.get(0)).getValue(), 1);
        }
    }
}