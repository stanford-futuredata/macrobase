package macrobase.runtime.standalone;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.List;

/**
 * Created by pbailis on 12/24/15.
 */
public class StandaloneConfiguration extends Configuration {
    @NotEmpty
    private String taskName;

    @NotEmpty
    private String dbUrl;

    @NotEmpty
    private List<String> targetAttributes;

    private List<String> targetHighMetrics;

    private List<String> targetLowMetrics;

    @NotEmpty
    private String baseQuery;

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
}
