package macrobase.analysis.classify;

import macrobase.analysis.pipeline.operator.MBConsumer;
import macrobase.analysis.pipeline.operator.MBProducer;
import macrobase.analysis.result.OutlierClassificationResult;

import macrobase.datamodel.Datum;

public interface OutlierClassifier extends MBConsumer<Datum>, MBProducer<OutlierClassificationResult> { }
