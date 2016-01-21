package macrobase.runtime;

import com.google.common.collect.Lists;
import io.dropwizard.Configuration;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.hibernate.validator.constraints.NotEmpty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

public class ServerConfiguration extends Configuration {
    public static class PreLoadData {
        @NotEmpty
        private String baseQuery;

        @NotEmpty
        private String dbUrl;

        private String dbUser;

        private String dbPassword;

        @NotEmpty
        private List<String> targetAttributes;

        public PreLoadData(String baseQuery, String dbUrl, List<String> targetAttributes, List<String> targetHighMetrics, List<String> targetLowMetrics, String dbUser, String dbPassword) {
            this.baseQuery = baseQuery;
            this.dbUrl = dbUrl;
            this.targetAttributes = targetAttributes;
            this.targetHighMetrics = targetHighMetrics;
            this.targetLowMetrics = targetLowMetrics;
            this.dbUser = dbUser;
            this.dbPassword = dbPassword;
        }

        private List<String> targetHighMetrics;
        private List<String> targetLowMetrics;

        @JsonProperty
        public String getBaseQuery() {
            return baseQuery;
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
        public String getDbUrl() {
            return dbUrl;
        }

        @JsonProperty
        public List<String> getTargetAttributes() {
            return targetAttributes;
        }

        @JsonProperty
        public List<String> getTargetHighMetrics() {
            return targetHighMetrics;
        }

        @JsonProperty
        public List<String> getTargetLowMetrics() {
            return targetLowMetrics;
        }

        public PreLoadData() {};
    }

    @JsonProperty
    private List<PreLoadData> preLoad;

    private boolean doPreLoadData;


    @JsonProperty
    public List<PreLoadData> getPreLoad() {
        return preLoad;
    }

    @JsonProperty
    public boolean doPreLoadData() {
        return doPreLoadData;
    }

}
