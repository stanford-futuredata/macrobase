package macrobase.analysis.classify;

import macrobase.analysis.pipeline.operator.MBOperator;
import macrobase.analysis.result.OutlierClassificationResult;

import macrobase.datamodel.Datum;

public abstract class OutlierClassifier extends MBOperator<Datum, OutlierClassificationResult> { }
