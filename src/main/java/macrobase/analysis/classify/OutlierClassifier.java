package macrobase.analysis.classify;

import macrobase.analysis.pipeline.operator.MBOperator;
import macrobase.analysis.result.OutlierClassificationResult;

import macrobase.datamodel.Datum;

public interface OutlierClassifier extends MBOperator<Datum, OutlierClassificationResult> { }
