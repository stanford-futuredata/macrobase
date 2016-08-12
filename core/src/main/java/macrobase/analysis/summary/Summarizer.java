package macrobase.analysis.summary;

import macrobase.analysis.pipeline.operator.MBOperator;
import macrobase.analysis.result.OutlierClassificationResult;

/**
 * Consumes OutlierClassification Result tuple-at-a-time, but returns summaries
 # when requested via `summarize()`.
 */

public abstract class Summarizer extends MBOperator<OutlierClassificationResult, Summary> {
  public abstract  Summarizer summarize();
}
