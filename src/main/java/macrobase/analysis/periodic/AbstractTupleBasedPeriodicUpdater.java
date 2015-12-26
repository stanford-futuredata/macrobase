package macrobase.analysis.periodic;

/**
 * Created by pbailis on 12/24/15.
 */
abstract class AbstractTupleBasedPeriodicUpdater extends AbstractPeriodicUpdater {

    private long prevPeriodTuples;
    private final long tuplesPerPeriod;

    public AbstractTupleBasedPeriodicUpdater(long tuplesPerPeriod) {
        this.tuplesPerPeriod = tuplesPerPeriod;
    }

    protected long getCurrentTupleCount() {
        return prevPeriodTuples;
    }

    @Override
    boolean needsPeriodAdvance(long wallTime, long numTuples) {
        return prevPeriodTuples + tuplesPerPeriod < numTuples;
    }

    @Override
    void periodAdvanced() {
        prevPeriodTuples += tuplesPerPeriod;
    }
}
