package macrobase.analysis.pipeline.operator;

import com.google.common.collect.Lists;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.datamodel.Datum;
import org.junit.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;


public class MBGroupByTest {
    @Test
    public void simpleTest() throws Exception {
        MBGroupBy groupBy = new MBGroupBy(Lists.newArrayList(0, 1),
                                          () -> new FeatureTransform() {
                                              MBStream<Datum> output = new MBStream<>();

                                              @Override
                                              public void initialize() throws Exception {

                                              }

                                              @Override
                                              public void consume(List<Datum> records) throws Exception {
                                                  double sum = 0;

                                                  for(Datum d : records) {
                                                      sum += d.metrics().getNorm();
                                                  }

                                                  output.add(new Datum(records.get(0).attributes(), sum));
                                              }

                                              @Override
                                              public void shutdown() throws Exception {

                                              }

                                              @Override
                                              public MBStream<Datum> getStream() throws Exception {
                                                  return output;
                                              }
                                          }
                                          );

        Map<List<Integer>, Double> groupSums = new HashMap<>();
        List<Datum> testData = new ArrayList<>();
        for(int group = 0; group < 10; ++group) {
            List<Integer> groupList = Lists.newArrayList(group, group+1);
            double sum = 0;
            for(int i = 0; i < 10; ++i) {
                testData.add(new Datum(groupList, group+i));
                sum += group+i;
            }

            groupSums.put(groupList, sum);
        }

        groupBy.consume(testData);
        List<Datum> transformed = groupBy.getStream().drain();
        assertEquals(groupSums.size(), transformed.size());

        for(Datum t : transformed) {
            assertTrue(groupSums.containsKey(t.attributes()));
            assertEquals(groupSums.get(t.attributes()), t.metrics().getNorm(), 0.0);
        }
    }
}
