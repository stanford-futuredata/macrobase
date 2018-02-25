package msolver.data;

public class HepData extends MomentData {
    private final double[] range = {
            -1.960548996925354,
            4.378281593322753
    };
    private final double[] powerSums = {
            10500000.0,
            171590.10344086567,
            10600692.901931819,
            3656024.1204772266,
            27999983.535580769,
            26085580.4679562,
            112571037.57246359,
            178035035.71434641,
            597052564.3216269,
            1272051754.8467662,
            3862170673.2579589,
            9736800905.3333244,
            28892120151.48772,
            80400830561.547516,
            241656673866.27899,
            717077710760.47144,
            2216395701960.5981,
            6900059165428.8955,
            22051018347144.91,
            71474655616939.391
   };
    private final double[] logSums = {1.0};

    @Override
    public double[] getPowerSums() {
        return powerSums;
    }

    @Override
    public double getMin() {
        return range[0];
    }

    @Override
    public double getMax() {
        return range[1];
    }

    @Override
    public double[] getLogSums() {
        return logSums;
    }

    @Override
    public double getLogMin() {
        return 0.0;
    }

    @Override
    public double getLogMax() {
        return 0.0;
    }
}

