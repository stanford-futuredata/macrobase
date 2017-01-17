package macrobase.analysis.pipeline.stream;

import macrobase.datamodel.Datum;

import java.util.List;

public class TimeDatumStream extends MBStream<Datum> {
    private int timeColumn;

    public TimeDatumStream(int timeColumn) {
        super();
        this.timeColumn = timeColumn;
    }

    public TimeDatumStream(List<Datum> data, int timeColumn) {
        super(data);
        this.timeColumn = timeColumn;
    }

    private long getDatumTime(int i) {
        return output.get(i).getTime(timeColumn);
    }

    public List<Datum> drainDuration(long duration) {
        int i = 0;
        long startTime = getDatumTime(i);
        while (i < output.size() && getDatumTime(i) - startTime < duration) {
            i ++;
        }
        return drain(i);
    }

    public Datum peek() {
        return output.get(output.size() - 1);
    }
}