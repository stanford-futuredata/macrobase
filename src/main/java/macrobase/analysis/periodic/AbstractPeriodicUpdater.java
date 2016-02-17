package macrobase.analysis.periodic;


public abstract class AbstractPeriodicUpdater {
    abstract boolean needsPeriodAdvance(long wallTime, long numTuples);
    abstract void periodAdvanced();
    abstract void updatePeriod();

    public boolean updateIfNecessary(long curTime, long curTupleNo) {
    	boolean updateOccurred = false;
        while(needsPeriodAdvance(curTime, curTupleNo)) {
            periodAdvanced();
            updatePeriod();
            updateOccurred = true;
        }
        return updateOccurred;
    }
}