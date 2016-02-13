package macrobase.runtime.standalone;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import macrobase.analysis.outlier.KDE;
import macrobase.ingest.DataLoader;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Created by pbailis on 12/24/15.
 */
public class BaseStandaloneConfiguration extends Configuration {

    public enum DetectorType {
        MAD,
        MCD,
        ZSCORE,
        KDE
    }

    public enum DataLoaderType {
        CSV_LOADER,
        SQL_LOADER
    }

    public enum DataTransform {
        ZERO_TO_ONE_SCALE,
        IDENTITY
    }

    @JsonProperty
    private DetectorType outlierDetectorType;

    private KDE.Bandwidth kernelBandwidth;

    @NotEmpty
    private String taskName;

    @NotEmpty
    private String dbUrl;

    private String dbUser;
    private String dbPassword;

    private DataLoaderType dataLoaderType;

    private DataTransform dataTransform;

    @NotEmpty
    private List<String> targetAttributes;

    private List<String> auxiliaryAttributes;

    private List<String> targetHighMetrics;

    private List<String> targetLowMetrics;

    private Double zScore;

    private Double targetPercentile;

    @NotNull
    private Boolean usePercentile;

    @NotNull
    private Boolean useDiskCache;

    private String diskCacheDirectory;

    @NotNull
    private Boolean useZScore;

    @NotNull
    private Double minSupport;

    @NotNull
    private Double minInlierRatio;

    @NotEmpty
    private String baseQuery;
    
    @NotNull
    private Double alphaMCD;
    
    @NotNull
    private Double stoppingDeltaMCD;

    private String storeScoreDistribution;  // Output scores to the following file.

    @JsonProperty
    public String getTaskName() {
        return taskName;
    }

    @JsonProperty
    public KDE.Bandwidth getKernelBandwidth() { return kernelBandwidth;}

    @JsonProperty
    public List<String> getAuxiliaryAttributes() {
        return auxiliaryAttributes;
    }

    @JsonProperty
    public List<String> getTargetHighMetrics() {
        return targetHighMetrics;
    }

    @JsonProperty
    public List<String> getTargetLowMetrics() {
        return targetLowMetrics;
    }

    @JsonProperty
    public String getBaseQuery() {
        return baseQuery;
    }

    @JsonProperty
    public String getDbUrl() {
        return dbUrl;
    }

    @JsonProperty
    public String getDbUser() {
        return dbUser;
    }

    @JsonProperty
    public String getDbPassword() {
        return dbPassword;
    }

    @JsonProperty
    public List<String> getTargetAttributes() {
        return targetAttributes;
    }

    @JsonProperty
    public double getzScore() {
        return zScore;
    }

    @JsonProperty
    public double getTargetPercentile() {
        return targetPercentile;
    }

    @JsonProperty
    public double getMinSupport() {
        return minSupport;
    }

    @JsonProperty
    public DataLoaderType getDataLoaderType() { return dataLoaderType; }

    @JsonProperty
    public DetectorType getDetectorType() { return outlierDetectorType; }

    @JsonProperty
    public DataTransform getDataTransform() { return dataTransform; }

    @JsonProperty
    public double getMinInlierRatio() {
        return minInlierRatio;
    }

    @JsonProperty
    public boolean usePercentile() {
        return usePercentile;
    }

    @JsonProperty
    public boolean useZScore() {
        return useZScore;
    }

    @JsonProperty
    public double getAlphaMCD() {
    	return alphaMCD;
    }

    @JsonProperty
    public double getStoppingDeltaMCD() {
    	return stoppingDeltaMCD;
    }

    @JsonProperty
    public Boolean useDiskCache() { return useDiskCache; }

    @JsonProperty
    public String getDiskCacheDirectory() { return diskCacheDirectory; }

    @JsonProperty
    public String getStoreScoreDistribution() {
        return storeScoreDistribution;
    }
}
