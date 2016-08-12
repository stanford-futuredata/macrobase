package macrobase.analysis.stats;

import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.transform.FeatureTransform;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import org.apache.commons.math3.distribution.MultivariateNormalDistribution;
import org.apache.commons.math3.linear.*;

import java.util.List;


public class RandomProjection extends FeatureTransform {
    private boolean hasConsumed;
    private long randomSeed;
    private RealVector metricVector;
    private RealVector transformedVector;
    private int n;
    private int k;
    private RealVector mean;
    private RealVector covV;
    private DiagonalMatrix covM;
    private MultivariateNormalDistribution mnd;
    private RealMatrix randomProjectionMatrix;

    private final MBStream<Datum> output = new MBStream<>();

    public RandomProjection(MacroBaseConf conf){
        this.k = conf.getInt(MacroBaseConf.RANDOM_PROJECTION_K, MacroBaseDefaults.RANDOM_PROJECTION_K);
        this.randomSeed = conf.getRandom().nextLong(); // set MacroBaseConf.RANDOM_SEED to seed rng for the seed...
        this.hasConsumed = false;
    }

    @Override
    public void consume(List<Datum> records) throws Exception {
        if (!hasConsumed) {
            n = records.get(0).metrics().getDimension();
            mean = new ArrayRealVector(n);
            covV = new ArrayRealVector(n, 1d/n);
            covM = new DiagonalMatrix(covV.toArray());
            mnd = new MultivariateNormalDistribution(mean.toArray(), covM.getData());
            mnd.reseedRandomGenerator(randomSeed);
            randomProjectionMatrix = new BlockRealMatrix(mnd.sample(k));
            hasConsumed = true;
        }
        for (Datum d: records){
            metricVector = d.metrics();
            transformedVector = randomProjectionMatrix.operate(metricVector);
            output.add(new Datum(d,transformedVector));
        }
    }

    @Override
    public MBStream<Datum> getStream() throws Exception  {
        return output;
    }

    @Override
    public void initialize() throws Exception {

    }

    @Override
    public void shutdown() throws Exception {

    }
}
