package edu.stanford.futuredata.macrobase.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameLoader;
import edu.stanford.futuredata.macrobase.ingest.RESTDataFrameLoader;
import edu.stanford.futuredata.macrobase.util.MacrobaseException;

import java.util.Map;

public class PipelineUtils {
    public static DataFrame loadDataFrame(
            String inputURI,
            Map<String, Schema.ColType> colTypes
    ) throws Exception {
        return PipelineUtils.loadDataFrame(
                inputURI, colTypes, null, null, false
        );
    }

    public static DataFrame loadDataFrame(
            String inputURI,
            Map<String, Schema.ColType> colTypes,
            Map<String, String> restHeader,
            Map<String, Object> jsonBody,
            boolean usePost
    ) throws Exception {
        if(inputURI.startsWith("csv")) {
            CSVDataFrameLoader loader = new CSVDataFrameLoader(inputURI.substring(6)); // length of "csv://"
            DataFrame df = loader.setColumnTypes(colTypes).load();
            return df;
        } else if (inputURI.startsWith("http")){
            ObjectMapper mapper = new ObjectMapper();
            String bodyString = mapper.writeValueAsString(jsonBody);

            RESTDataFrameLoader loader = new RESTDataFrameLoader(
                    inputURI,
                    restHeader
            );
            loader.setUsePost(usePost);
            loader.setJsonBody(bodyString);
            loader.setColumnTypes(colTypes);
            DataFrame df = loader.load();
            return df;
        } else {
            throw new MacrobaseException("Unsupported URI");
        }
    }

    public static Pipeline createPipeline(
            PipelineConfig conf
    ) throws MacrobaseException {
        String pipelineName = conf.get("pipeline");
        switch (pipelineName) {
            case "BasicBatchPipeline": {
                return new BasicBatchPipeline(conf);
            }
            case "CubePipeline": {
                return new CubePipeline(conf);
            }
            default: {
                throw new MacrobaseException("Bad Pipeline");
            }
        }
    }
}
