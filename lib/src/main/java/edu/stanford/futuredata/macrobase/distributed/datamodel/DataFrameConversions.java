package edu.stanford.futuredata.macrobase.distributed.datamodel;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import org.apache.spark.SparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import scala.collection.JavaConversions;
import scala.collection.Seq;
import scala.reflect.ClassTag;

import java.util.ArrayList;
import java.util.List;

public class DataFrameConversions {

    public static Dataset<Row> singleNodeDataFrameToSparkDataFrame(DataFrame df, SparkSession spark) {

        Schema schema = df.getSchema();
        List<StructField> fields = new ArrayList<>();
        for (String stringColName : schema.getColumnNamesByType(Schema.ColType.STRING)) {
            StructField field = DataTypes.createStructField(stringColName, DataTypes.StringType, true);
            fields.add(field);
        }
        for (String doubleColName : schema.getColumnNamesByType(Schema.ColType.DOUBLE)) {
            StructField field = DataTypes.createStructField(doubleColName, DataTypes.DoubleType, true);
            fields.add(field);
        }

        List<Row> dataFrameAsSparkRows = new ArrayList<>();
        for (int i = 0; i < df.getNumRows(); i++) {
            List<Object> rowList = new ArrayList<>();
            for (String[] column : df.getStringCols()) {
                rowList.add(column[i]);
            }
            for (double[] column : df.getDoubleCols()) {
                rowList.add(column[i]);
            }
            dataFrameAsSparkRows.add(RowFactory.create(rowList.toArray()));
        }
        Seq<Row> dataFrameAsSparkRowsSeq = JavaConversions.asScalaBuffer(dataFrameAsSparkRows).toSeq();
        ClassTag<Row> tag = scala.reflect.ClassTag$.MODULE$.apply(Row.class);
        return spark.createDataFrame(spark.sparkContext().parallelize(dataFrameAsSparkRowsSeq,
                spark.sparkContext().defaultParallelism(), tag), DataTypes.createStructType(fields));
    }
}
