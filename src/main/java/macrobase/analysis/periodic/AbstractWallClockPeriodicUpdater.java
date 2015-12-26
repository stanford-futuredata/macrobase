package macrobase.analysis.periodic;

/**
 * Created by pbailis on 12/24/15.
 */
abstract class AbstractWallClockPeriodicUpdater extends AbstractPeriodicUpdater {
    private long prevPeriodMillis;
    private final long periodMs;

    public AbstractWallClockPeriodicUpdater(long startTime,
                                            long periodMs) {
        this.prevPeriodMillis = startTime;
        this.periodMs = periodMs;
    }

    protected long getCurrentPeriodTime() {
        return prevPeriodMillis;
    }


    @Override
    boolean needsPeriodAdvance(long wallTime, long numTuples) {
        return prevPeriodMillis + periodMs < wallTime;
    }

    @Override
    void periodAdvanced() {
        prevPeriodMillis += periodMs;
    }

}
