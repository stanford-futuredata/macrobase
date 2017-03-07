package macrobase.analysis.summary;

import macrobase.datamodel.Row;

import java.util.Map;

public class OutlierGroup {
    private final long numOutliers;
    private final long numInliers;
    private final Map<String, String> archetype;

    public OutlierGroup(Map<String, String> archetype,
                        long numInliers,
                        long numOutliers) {
        this.archetype = archetype;
        this.numInliers = numInliers;
        this.numOutliers = numOutliers;
    }

    public Map<String, String> getArchetype() {
        return archetype;
    }

    public double getNumOutliers() {
        return numOutliers;
    }

    public double getNumInliers() {
        return numInliers;
    }
}
