package macrobase.ingest;

import com.google.common.collect.Lists;
import macrobase.datamodel.Datum;

import java.util.Iterator;
import java.util.List;

public class TimedBatchIngest implements Iterator<Datum> {
    private long finishTimeMs;
    private final Iterator<Datum> input;
    private Iterator<Datum> output;

    public long getFinishTimeMs() {
        return finishTimeMs;
    }

    public TimedBatchIngest(Iterator<Datum> input) {
        this.input = input;
    }


    @Override
    public boolean hasNext() {
        return this.input.hasNext() || (output != null && output.hasNext());
    }

    @Override
    public Datum next() {
        if(output == null) {
            List<Datum> drained = Lists.newArrayList(input);
            output = drained.iterator();
            finishTimeMs = System.currentTimeMillis();
        }

        return output.next();
    }
}
