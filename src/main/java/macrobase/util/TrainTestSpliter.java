package macrobase.util;

import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

public class TrainTestSpliter {
    private static final Logger log = LoggerFactory.getLogger(TrainTestSpliter.class);
    private final List<Datum> trainData;
    private final List<Datum> testData;

    public TrainTestSpliter(List<Datum> data, double trainRatio, Random rand) {

        List<Datum> trainingData = new ArrayList<>((int) (data.size() * trainRatio));
        List<Datum> testData = new ArrayList<>((int) (data.size() * (1 - trainRatio)));

        log.debug("nextDouble() {}", rand.nextDouble());
        for (Datum d : data) {
            if (rand.nextDouble() < trainRatio ) {
                trainingData.add(d);
            } else {
                testData.add(d);
            }
        }
        log.debug("training points = {}", trainingData.size());
        log.debug("test points = {}", testData.size());
        this.trainData = trainingData;
        this.testData = testData;
    }

    public List<Datum> getTrainData() {
        return trainData;
    }

    public List<Datum> getTestData() {
        return testData;
    }
}
