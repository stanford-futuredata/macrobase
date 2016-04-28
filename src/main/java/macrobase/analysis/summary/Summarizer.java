package macrobase.analysis.summary;

import macrobase.analysis.pipeline.operator.MBOperator;
import macrobase.analysis.result.OutlierClassificationResult;

/**
 * Consumes OutlierClassification Result tuple-at-a-time, but returns summaries
 # when there has been an update.
 */

public interface Summarizer extends MBOperator<OutlierClassificationResult, Summary> { }
