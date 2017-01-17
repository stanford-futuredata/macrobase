package macrobase.analysis.transform;

import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.datamodel.Datum;

import java.util.ArrayList;
import java.util.List;

public abstract class SlidingWindowTransform extends FeatureTransform {
    protected int windowSize;
    protected long slideSize;
    protected int timeColumn;
    protected long windowStart = -1;

    protected MBStream<Datum> output = new MBStream<>();
    protected List<Datum> currWindow = new ArrayList<>();

    protected boolean datumInRange(Datum d, long start, int size) {
        return d.getTime(timeColumn) - start < size;
    }
}
