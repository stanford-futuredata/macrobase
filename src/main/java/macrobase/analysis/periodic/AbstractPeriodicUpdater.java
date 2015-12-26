package macrobase.analysis.periodic;


public abstract class AbstractPeriodicUpdater {
    abstract boolean needsPeriodAdvance(long wallTime, long numTuples);
    abstract void periodAdvanced();
    abstract void updatePeriod();

    public void updateIfNecessary(long curTime, long curTupleNo) {
        while(needsPeriodAdvance(curTime, curTupleNo)) {
            periodAdvanced();
            updatePeriod();
        }
    }
}