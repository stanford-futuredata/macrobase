package edu.stanford.futuredata.macrobase.pipeline;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameLoader;
import edu.stanford.futuredata.macrobase.util.MacrobaseException;

import java.util.Map;

public class PipelineUtils {
    public static DataFrame loadDataFrame(
            String inputURI,
            Map<String, Schema.ColType> colTypes
    ) throws Exception {
        if(inputURI.substring(0, 3).equals("csv")) {
            CSVDataFrameLoader loader = new CSVDataFrameLoader(inputURI.substring(6));
            loader.setColumnTypes(colTypes);
            DataFrame df = loader.load();
            return df;
        } else {
            throw new MacrobaseException("Unsupported URI");
        }
    }
}
