package macrobase.runtime.standalone;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import org.hibernate.validator.constraints.NotEmpty;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * Created by pbailis on 12/24/15.
 */
public class BaseStandaloneConfiguration extends Configuration {
    @NotEmpty
    private String taskName;

    @NotEmpty
    private String dbUrl;

    @NotEmpty
    private List<String> targetAttributes;

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

    @JsonProperty
    public String getTaskName() {
        return taskName;
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
}
