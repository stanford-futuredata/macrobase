package macrobase.ingest;

import macrobase.conf.ConfigurationException;
import macrobase.conf.MacroBaseConf;
import macrobase.conf.MacroBaseDefaults;
import macrobase.datamodel.Datum;
import macrobase.ingest.result.ColumnValue;
import macrobase.ingest.result.RowSet;
import macrobase.ingest.result.Schema;
import macrobase.ingest.transform.DataTransformation;
import macrobase.ingest.transform.IdentityTransformation;
import macrobase.ingest.transform.ZeroToOneLinearTransformation;
import macrobase.runtime.resources.RowSetResource;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public abstract class DataLoader {
    protected final MacroBaseConf conf;

    protected final String timeColumn;
    protected final List<String> attributes;
    protected final List<String> lowMetrics;
    protected final List<String> highMetrics;
    protected final List<String> auxiliaryAttributes;
    protected final DataTransformation dataTransformation;

    public DataLoader(MacroBaseConf conf) throws ConfigurationException {
        this.conf = conf;

        timeColumn = conf.getString(MacroBaseConf.TIME_COLUMN, MacroBaseDefaults.TIME_COLUMN);
        attributes = conf.getStringList(MacroBaseConf.ATTRIBUTES);
        lowMetrics = conf.getStringList(MacroBaseConf.LOW_METRICS);
        highMetrics = conf.getStringList(MacroBaseConf.HIGH_METRICS);
        auxiliaryAttributes = conf.getStringList(MacroBaseConf.AUXILIARY_ATTRIBUTES, new ArrayList<>());
        MacroBaseConf.DataTransformType dataTransformType = conf.getDataTransform();

        if (dataTransformType == MacroBaseConf.DataTransformType.ZERO_TO_ONE_SCALE) {
            dataTransformation = new ZeroToOneLinearTransformation();
        } else if (dataTransformType == MacroBaseConf.DataTransformType.IDENTITY) {
            dataTransformation = new IdentityTransformation();
        } else {
            throw new ConfigurationException(String.format("no known data transformation %s", dataTransformType));
        }
    }

    public abstract Schema getSchema(String baseQuery)
            throws SQLException, IOException;

    public abstract List<Datum> getData(DatumEncoder encoder)
            throws SQLException, IOException;
    
    public abstract List<Datum> getData(DatumEncoder encoder, 
    		List<String> contextualDiscreteAttributes,
    		List<String> contextualDoubleAttributes)
            throws SQLException, IOException;


    public abstract RowSet getRows(String baseQuery,
                                   List<RowSetResource.RowSetRequest.RowRequestPair> preds,
                                   int limit,
                                   int offset)
            throws SQLException, IOException;
}
