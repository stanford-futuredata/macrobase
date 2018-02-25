package msolver.data;

public class RetailQuantityData extends MomentData{
    public static final double[] powerSums = {
            531285.0,
            5660981.0,
            13127647799.0,
            943385744203541.0,
            7.3401290527335825e+19,
            5.7374634895753686e+24,
            4.4941878460622702e+29,
            3.5267533869172936e+34,
            2.7724146420399472e+39,
            2.1831198887574202e+44,
            1.7219100191391005e+49,
            1.3602936186335558e+54,
            1.0762640535486096e+59,
            8.5279096877163735e+63,
            6.7666981261637846e+68,
            5.3764281895692519e+73,
            4.2772622266819037e+78,
            3.4069441013233427e+83,
            2.7168368273312946e+88,
            2.1688733753934956e+93
    };

    public static final double[] logSums = {
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
            1489418691939729.2,
            14997093766426690.0,
            1.5677925320389693e+17,
            1.6804693592838444e+18,
            1.831003035492703e+19,
            2.0164791544383e+20
    };

    @Override
    public double[] getPowerSums() {
        return powerSums;
    }

    @Override
    public double[] getLogSums() {
        return logSums;
    }

    @Override
    public double getMin() {
        return 1.0;
    }

    @Override
    public double getMax() {
        return 80995.0;
    }

    @Override
    public double getLogMin() {
        return 0.0;
    }

    @Override
    public double getLogMax() {
        return Math.log(80995.0);
    }
}
