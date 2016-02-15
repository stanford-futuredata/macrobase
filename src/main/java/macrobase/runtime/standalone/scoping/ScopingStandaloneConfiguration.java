package macrobase.runtime.standalone.scoping;

import java.util.List;

import javax.validation.constraints.NotNull;

import org.hibernate.validator.constraints.NotEmpty;

import com.fasterxml.jackson.annotation.JsonProperty;

import macrobase.runtime.standalone.BaseStandaloneConfiguration;

public class ScopingStandaloneConfiguration extends BaseStandaloneConfiguration {

	
	 private List<String> categoricalAttributes;
	 
	 @JsonProperty
     public List<String> getCategoricalAttributes() {
        return categoricalAttributes;
	 }
	 
	 private List<String> numericalAttributes;
	 
	 @JsonProperty
     public List<String> getNumericalAttributes() {
        return numericalAttributes;
	 }
	 
	 private Integer numInterval;
	 
	 @JsonProperty
     public int getNumInterval() {
        return numInterval;
     }
	 
	 @NotNull
	 private Double minFrequentSubSpaceRatio;
	 
	 @JsonProperty
     public double getMinFrequentSubSpaceRatio() {
        return minFrequentSubSpaceRatio;
     }
	 
	 @NotNull
	 private Double maxSparseSubSpaceRatio;
	 
	 @JsonProperty
     public double getMaxSparseSubSpaceRatio() {
        return maxSparseSubSpaceRatio;
     }
	 
	 @NotNull
	 private Integer maxScopeDimensions;
	 
	 public int getMaxScopeDimensions(){
		 return maxScopeDimensions;
	 }
}
