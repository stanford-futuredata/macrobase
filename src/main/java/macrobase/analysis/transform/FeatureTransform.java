package macrobase.analysis.transform;

import macrobase.analysis.pipeline.operator.MBOperator;
import macrobase.datamodel.Datum;

public interface FeatureTransform extends MBOperator<Datum, Datum> {}
