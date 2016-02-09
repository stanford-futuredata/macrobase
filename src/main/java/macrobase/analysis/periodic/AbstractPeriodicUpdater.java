package macrobase.analysis.periodic;


public abstract class AbstractPeriodicUpdater {
    abstract boolean needsPeriodAdvance(long wallTime, long numTuples);
    abstract void periodAdvanced();
    abstract void updatePeriod(Object additionalData);

    public boolean updateIfNecessary(long curTime, long curTupleNo, Object additionalData) {
    	boolean updateOccurred = false;
        while(needsPeriodAdvance(curTime, curTupleNo)) {
            periodAdvanced();
            updatePeriod(additionalData);
            updateOccurred = true;
        }
        return updateOccurred;
    }
}