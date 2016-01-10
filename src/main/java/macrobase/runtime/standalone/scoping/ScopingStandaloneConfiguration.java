package macrobase.runtime.standalone.scoping;

import java.util.List;

import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.NotEmpty;

import com.fasterxml.jackson.annotation.JsonProperty;

import macrobase.runtime.standalone.BaseStandaloneConfiguration;

public class ScopingStandaloneConfiguration extends BaseStandaloneConfiguration {

	 @NotEmpty
	 private List<String> scopingAttributes;
	 
	 @JsonProperty
     public List<String> getScopingAttributes() {
        return scopingAttributes;
	 }
	 
	 @NotNull
	 private Double minScopingSupport;
	 
	 @JsonProperty
     public double getMinScopingSupport() {
        return minScopingSupport;
     }
}
