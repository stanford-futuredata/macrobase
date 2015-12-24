package macrobase.runtime.server.standalone;

import com.fasterxml.jackson.annotation.JsonProperty;
import io.dropwizard.Configuration;
import org.hibernate.validator.constraints.NotEmpty;

/**
 * Created by pbailis on 12/24/15.
 */
public class StandaloneConfiguration extends Configuration {
    @NotEmpty
    private String taskName;

    @JsonProperty
    public String getTaskName() {
        return taskName;
    }
}
