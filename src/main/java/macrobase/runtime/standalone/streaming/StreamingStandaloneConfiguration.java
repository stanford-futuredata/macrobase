package macrobase.runtime.standalone.streaming;

import com.fasterxml.jackson.annotation.JsonProperty;
import macrobase.runtime.standalone.BaseStandaloneConfiguration;

import javax.validation.constraints.NotNull;

public class StreamingStandaloneConfiguration extends BaseStandaloneConfiguration {
    private Integer inputReservoirSize;

    private Integer scoreReservoirSize;

    @NotNull
    private Integer summaryRefreshPeriod;

    @NotNull
    private Boolean useRealTimePeriod;

    @NotNull
    private Boolean useTupleCountPeriod;

    @NotNull
    private Double decayRate;

    @NotNull
    private Integer modelRefreshPeriod;

    @NotNull
    private Integer warmupCount;

    @NotNull
    private Integer inlierItemSummarySize;

    @NotNull
    private Integer outlierItemSummarySize;

    @JsonProperty
    public Integer getInputReservoirSize() {
        return inputReservoirSize;
    }

    @JsonProperty
    public Integer getScoreReservoirSize() {
        return scoreReservoirSize;
    }

    @JsonProperty
    public Integer getSummaryRefreshPeriod() {
        return summaryRefreshPeriod;
    }

    @JsonProperty
    public double getDecayRate() { return decayRate; }

    @JsonProperty
    public Boolean useRealTimePeriod() { return useRealTimePeriod; }

    @JsonProperty
    public Boolean useTupleCountPeriod() {
        return useTupleCountPeriod;
    }

    @JsonProperty
    public Integer getModelRefreshPeriod() {
        return modelRefreshPeriod;
    }

    @JsonProperty
    public int getWarmupCount() {
        return warmupCount;
    }

    @JsonProperty
    public Integer getInlierItemSummarySize() { return inlierItemSummarySize; }

    @JsonProperty
    public Integer getOutlierItemSummarySize() { return outlierItemSummarySize; }
}
