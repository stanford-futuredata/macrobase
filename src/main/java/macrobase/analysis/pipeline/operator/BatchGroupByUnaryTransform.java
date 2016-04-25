package macrobase.analysis.pipeline.operator;

import com.google.common.collect.Lists;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.analysis.transform.UnaryFeatureTransform;
import macrobase.datamodel.Datum;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

public class BatchGroupByUnaryTransform implements Iterator<FeatureTransform> {
    private final Iterator<Datum> input;
    private final int groupByAttributeNumber;
    private final Function<Iterator<Datum>, UnaryFeatureTransform>  featureTransformClass;

    private final Map<Integer, List<Datum>> transformMap = new HashMap<>();

    private final Iterator<UnaryFeatureTransform> outputFeatureTransforms;

    public BatchGroupByUnaryTransform(Iterator<Datum> input,
                                      int groupByAttributeNumber,
                                      Function<Iterator<Datum>, UnaryFeatureTransform> ft) throws IllegalAccessException, InstantiationException {
        this.input = input;
        this.groupByAttributeNumber = groupByAttributeNumber;
        this.featureTransformClass = ft;

        List<Datum> data = Lists.newArrayList(input);
        for(Datum d : data) {
            int groupNo = d.getAttributes().get(groupByAttributeNumber);

            List<Datum> t = transformMap.get(groupNo);
            if(t == null) {
                t = new ArrayList<>();
                transformMap.put(groupNo, t);
            }

            t.add(d);
        }

        List<UnaryFeatureTransform> output = new ArrayList<>();
        for(List<Datum> datumList : transformMap.values()) {
            output.add(ft.apply(datumList.iterator()));
        }

        outputFeatureTransforms = output.iterator();
    }

    @Override
    public boolean hasNext() {
        return outputFeatureTransforms.hasNext();
    }

    @Override
    public FeatureTransform next() {
        return outputFeatureTransforms.next();
    }
}
