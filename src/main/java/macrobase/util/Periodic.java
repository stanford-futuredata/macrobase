package macrobase.util;

import macrobase.conf.MacroBaseConf;

public class Periodic {
    private double previousPeriod;
    private int numCalls;
    private final MacroBaseConf.PeriodType periodType;
    private final double periodLength;
    private final Runnable task;
    private double elapsed;

    public Periodic(MacroBaseConf.PeriodType periodType,
                    double periodLength,
                    Runnable task) {
        this.periodType = periodType;
        this.periodLength = periodLength;
        this.task = task;

        if(periodType == MacroBaseConf.PeriodType.TIME_BASED) {
            previousPeriod = System.currentTimeMillis();
        }
    }

    public void runIfNecessary () {
        numCalls++;

        if(periodLength < 0) {
            return;
        }

        elapsed = periodType == MacroBaseConf.PeriodType.TIME_BASED ?
                System.currentTimeMillis() : numCalls;

        while(previousPeriod + periodLength < elapsed) {
            task.run();
            previousPeriod += periodLength;
        }
    }
}
