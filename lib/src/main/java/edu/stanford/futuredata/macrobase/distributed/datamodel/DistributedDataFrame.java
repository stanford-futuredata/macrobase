package edu.stanford.futuredata.macrobase.distributed.datamodel;

import edu.stanford.futuredata.macrobase.datamodel.Schema;
import org.apache.spark.api.java.JavaPairRDD;

public class DistributedDataFrame {
    public Schema schema;
    public JavaPairRDD<String[], double[]> dataFrameRDD;

    public DistributedDataFrame(Schema schema, JavaPairRDD<String[], double[]> dataFrameRDD) {
        this.schema = schema;
        this.dataFrameRDD = dataFrameRDD;
    }

}
