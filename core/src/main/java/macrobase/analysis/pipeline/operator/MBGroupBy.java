package macrobase.analysis.pipeline.operator;

import macrobase.analysis.pipeline.stream.MBMultiInputStream;
import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.datamodel.Datum;
import macrobase.util.CheckedSupplier;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class MBGroupBy extends MBOperator<Datum, Datum> {
    private final List<Integer> groupByColumn;
    private final CheckedSupplier<FeatureTransform> aggregator;

    Map<List<Integer>, FeatureTransform> transformMap = new HashMap<>();

    private final MBMultiInputStream<Datum> outputStream = new MBMultiInputStream<>();

    public MBGroupBy(List<Integer> groupByColumn,
                     CheckedSupplier<FeatureTransform> aggregator) {
        this.groupByColumn = groupByColumn;
        this.aggregator = aggregator;
    }

    @Override
    public void initialize() throws Exception {

    }

    @Override
    public void consume(List<Datum> records) throws Exception {
        // for each group id, accumulates the data matching the group
        Map<List<Integer>, List<Datum>> dataMap = new HashMap<>();

        for(Datum d : records) {
            List<Integer> groupValues = new ArrayList<>(groupByColumn.size());
            for(Integer group : groupByColumn) {
                groupValues.add(d.attributes().get(group));
            }

            List<Datum> t = dataMap.get(groupValues);
            if(t == null) {
                t = new ArrayList<>();
                dataMap.put(groupValues, t);
                FeatureTransform ft = aggregator.get();
                transformMap.put(groupValues, ft);
                outputStream.addStream(ft.getStream());
            }

            t.add(d);
        }

        for(Map.Entry<List<Integer>, List<Datum>> entry : dataMap.entrySet()) {
            transformMap.get(entry.getKey()).consume(entry.getValue());
        }
    }

    @Override
    public void shutdown() throws Exception { }

    @Override
    public MBStream<Datum> getStream() throws Exception {
        return outputStream;
    }
}
