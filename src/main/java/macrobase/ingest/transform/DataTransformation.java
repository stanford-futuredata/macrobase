package macrobase.ingest.transform;

import macrobase.datamodel.Datum;

import java.util.List;

/**
 * Abstract class for doing data transformation.
 * It is used to transform data before training a model on it.
 */
public abstract class DataTransformation {

    public abstract void transform(List<Datum> data);
}
