package edu.stanford.futuredata.macrobase.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameParser;
import edu.stanford.futuredata.macrobase.ingest.RESTDataFrameLoader;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;

import java.util.Map;
import java.util.List;

public class PipelineUtils {
    public static DataFrame loadDataFrame(
            String inputURI,
            Map<String, Schema.ColType> colTypes,
            List<String> requiredColumns
    ) throws Exception {
        return PipelineUtils.loadDataFrame(
                inputURI, colTypes, null, null, false,
                requiredColumns
        );
    }

    public static DataFrame loadDataFrame(
            String inputURI,
            Map<String, Schema.ColType> colTypes,
            Map<String, String> restHeader,
            Map<String, Object> jsonBody,
            boolean usePost,
            List<String> requiredColumns
    ) throws Exception {
        if(inputURI.startsWith("csv")) {
            // take off "csv://" from inputURI
            CSVDataFrameParser loader = new CSVDataFrameParser(inputURI.substring(6), requiredColumns);
            loader.setColumnTypes(colTypes);
            DataFrame df = loader.load();
            return df;
        } else if (inputURI.startsWith("http")){
            ObjectMapper mapper = new ObjectMapper();
            String bodyString = mapper.writeValueAsString(jsonBody);

            RESTDataFrameLoader loader = new RESTDataFrameLoader(
                    inputURI,
                    restHeader,
                    requiredColumns
            );
            loader.setUsePost(usePost);
            loader.setJsonBody(bodyString);
            loader.setColumnTypes(colTypes);
            DataFrame df = loader.load();
            return df;
        } else {
            throw new MacroBaseException("Unsupported URI");
        }
    }

    public static Pipeline createPipeline(
            PipelineConfig conf
    ) throws MacroBaseException {
        String pipelineName = conf.get("pipeline");
        switch (pipelineName) {
            case "BasicBatchPipeline": {
                return new BasicBatchPipeline(conf);
            }
            case "CubePipeline": {
                return new CubePipeline(conf);
            }
            default: {
                throw new MacroBaseException("Bad Pipeline");
            }
        }
    }
}
