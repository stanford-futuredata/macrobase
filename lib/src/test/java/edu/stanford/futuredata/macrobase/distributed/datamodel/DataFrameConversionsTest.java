package edu.stanford.futuredata.macrobase.distributed.datamodel;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameParser;
import edu.stanford.futuredata.macrobase.ingest.DataFrameLoader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static edu.stanford.futuredata.macrobase.distributed.datamodel.DataFrameConversions.singleNodeDataFrameToSparkDataFrame;
import static junit.framework.TestCase.assertEquals;

public class DataFrameConversionsTest {

    @Test
    public void testSingleNodeDataFrameToSparkDataFrame() throws Exception {
        SparkSession spark = SparkSession
                .builder()
                .master("local[4]")
                .appName("macrobase-sql-spark")
                .getOrCreate();

        Map<String, Schema.ColType> colTypes = new HashMap<>();
        colTypes.put("usage", Schema.ColType.DOUBLE);

        DataFrameLoader loader = new CSVDataFrameParser("src/test/resources/tiny.csv",
                Arrays.asList("usage", "location", "version"))
                .setColumnTypes(colTypes);
        DataFrame df = loader.load();

        Dataset<Row> sparkDataset = singleNodeDataFrameToSparkDataFrame(df, spark);

        List<Row> collectedResult = sparkDataset.collectAsList();

        assertEquals(3, collectedResult.size());

        assertEquals("27", collectedResult.get(0).getString(0));
        assertEquals("USA", collectedResult.get(0).getString(1));
        assertEquals(2.0, collectedResult.get(0).getDouble(2));
        assertEquals("27", collectedResult.get(1).getString(0));
        assertEquals("CAN", collectedResult.get(1).getString(1));
        assertEquals(3.1, collectedResult.get(1).getDouble(2));
        assertEquals("28", collectedResult.get(2).getString(0));
        assertEquals("USA", collectedResult.get(2).getString(1));
        assertEquals(4.0, collectedResult.get(2).getDouble(2));

        spark.stop();
    }
}
