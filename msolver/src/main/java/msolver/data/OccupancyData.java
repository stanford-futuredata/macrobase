package msolver.data;

public class OccupancyData extends MomentData {
    private final double min = 412.75;
    private final double max = 2076.5;
    private final double logMin = 6.0228420828002376;
    private final double logMax = 7.6384390630708081;
    private final double[] powerSums = {
            20560.0,
            14197775.359523809,
            11795382081.900866,
            11920150330935.938,
            14243310876969824.0,
            1.9248869180998238e+19,
            2.8335762132634282e+22,
            4.431640701816542e+25,
            7.2509584910158713e+28,
            1.2290081330972746e+32,
            2.1433360706825834e+35,
            3.8263457725342386e+38,
            6.9641284233810108e+41,
            1.287891117361348e+45,
            2.4132657512596994e+48,
            4.5712141086232246e+51,
            8.7361384845196883e+54,
            1.6818212554569329e+58,
            3.2572457284172447e+61,
            6.3398052560875453e+64
    };
    private final double[] logSums = {
            20560.0,
            132778.81355561133,
            860423.75561972987,
            5595528.9043199299,
            36524059.16578535,
            239323723.78677931,
            1574401576.9855776,
            10399585507.478024,
            68980678228.532593,
            459495821550.01648,
            3073979747643.9238,
            20653745268445.156,
            139372854449999.69,
            944566287701071.0,
            6429026416774866.0,
            43943128435886808.0,
            3.0160302130365139e+17,
            2.0784407797638454e+18,
            1.4379655766584013e+19,
            9.9865203720404238e+19
    };

    @Override
    public double[] getPowerSums() {
        return powerSums;
    }

    @Override
    public double getMin() {
        return min;
    }

    @Override
    public double getMax() {
        return max;
    }

    @Override
    public double[] getLogSums() {
        return logSums;
    }

    @Override
    public double getLogMin() {
        return logMin;
    }

    @Override
    public double getLogMax() {
        return logMax;
    }
}
