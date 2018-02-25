package msolver.data;

public class GaussianData extends MomentData {
    private final double[] ranges = {
            -5.4426943895132043, 5.0389474541327557,
            0,1
    };
    private final double[] powerSums = {
            10000000.0,
            480.5458937862864,
            9993547.2435967941,
            309.37531648859698,
            29973127.286717705,
            -9041.491442481145,
            150000244.00952214,
            -444367.57808807673,
            1053293551.5678303,
            -13936938.368433975,
            9539780309.9934578,
            -386976102.34488362,
            105935145427.16087,
            -10292879169.101974,
            1391938729272.7761,
            -270802772130.85544,
            21042777928938.496,
            -7160080883740.7246,
            357414134285331.12,
            -191665951342369.09
    };
    private final double[] logSums = {
            10000000.0
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
