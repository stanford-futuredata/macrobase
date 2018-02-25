package msolver.data;

public class RetailQuantityLogData extends MomentData {
    public static final double[] powerSums = {
            531285.0,
            733706.08385088702,
            1803377.9327264477,
            5313341.6192785092,
            18079041.058607381,
            69790609.377532199,
            302287816.89519858,
            1456494992.1214058,
            7765406800.8637161,
            45788298431.40226,
            299590569873.87885,
            2181621707739.7278,
            17627303232908.797,
            156164602838914.75,
            1489418691939729.2
    };

    @Override
    public double[] getPowerSums() {
        return powerSums;
    }

    @Override
    public double getMin() {
        return 0;
    }

    @Override
    public double getMax() {
        return 11.302142703354239;
    }
}
