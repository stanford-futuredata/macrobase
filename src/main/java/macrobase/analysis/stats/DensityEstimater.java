package macrobase.analysis.stats;

import macrobase.datamodel.Datum;

import java.util.List;

public interface DensityEstimater {
    double density(Datum datum);

    void train(List<Datum> data);
}
