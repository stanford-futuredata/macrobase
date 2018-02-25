package msolver.data;

public class ExponentialData extends MomentData {
    private final double[] ranges = {
            5.0366845333122605e-07,
            15.619130152703306,
            -14.501347616032602,
            2.7484964548137394
    };
    private final double[] powerSums = {
            1000000.0,
            998677.78490783856,
            1991896.3142772249,
            5947596.181134684,
            23651528.771513801,
            117830113.75301118,
            710308609.25475609,
            5073006703.31318,
            42241259442.990211,
            403047330063.94159,
            4314998960419.9683,
            50641655821410.031,
            637282955148883.38
    };
    private final double[] logSums = {
            1000000.0,
            -578739.68503790628,
            1983294.7107239107,
            -5476131.5807481054,
            23699783.595155567,
            -118308097.22723247,
            712931526.56406581,
            -4933389056.9205084,
            38572700905.764816,
            -334201709511.9444,
            3161470899883.4248,
            -32231476977189.141,
            350149425729588.94
    };

    @Override
    public double[] getPowerSums() {
        return powerSums;
    }

    @Override
    public double getMin() {
        return ranges[0];
    }

    @Override
    public double getMax() {
        return ranges[1];
    }

    @Override
    public double[] getLogSums() {
        return logSums;
    }

    @Override
    public double getLogMin() {
        return ranges[2];
    }

    @Override
    public double getLogMax() {
        return ranges[3];
    }
}

