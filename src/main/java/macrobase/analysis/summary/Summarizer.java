package macrobase.analysis.summary;

import macrobase.analysis.pipeline.operator.MBConsumer;
import macrobase.analysis.pipeline.operator.MBProducer;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.analysis.classify.OutlierClassifier;
import macrobase.conf.MacroBaseConf;
import macrobase.ingest.DatumEncoder;

/**
 * Consumes OutlierClassification Result tuple-at-a-time, but returns summaries
 # when there has been an update.
 */

public interface Summarizer extends MBConsumer<OutlierClassificationResult>, MBProducer<Summary> { }
