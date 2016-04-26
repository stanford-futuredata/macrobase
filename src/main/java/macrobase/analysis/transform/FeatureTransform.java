package macrobase.analysis.transform;

import macrobase.analysis.pipeline.operator.MBConsumer;
import macrobase.analysis.pipeline.operator.MBProducer;
import macrobase.datamodel.Datum;

public interface FeatureTransform extends MBConsumer<Datum>, MBProducer<Datum> { }
