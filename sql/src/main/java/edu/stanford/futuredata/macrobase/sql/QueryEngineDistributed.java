package edu.stanford.futuredata.macrobase.sql;

import com.google.common.base.Joiner;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema.ColType;
import edu.stanford.futuredata.macrobase.distributed.analysis.summary.aplinearDistributed.APLOutlierSummarizerDistributed;
import edu.stanford.futuredata.macrobase.sql.tree.*;
import edu.stanford.futuredata.macrobase.util.MacroBaseException;
import edu.stanford.futuredata.macrobase.util.MacroBaseSQLException;

import java.util.*;
import java.util.stream.Collectors;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static edu.stanford.futuredata.macrobase.distributed.datamodel.DataFrameConversions.singleNodeDataFrameToSparkDataFrame;

class QueryEngineDistributed {

    private static final Logger log = LoggerFactory.getLogger(QueryEngineDistributed.class.getSimpleName());

    private final int numPartitions;
    private final SparkSession spark;

    QueryEngineDistributed(SparkSession spark, int numPartitions) {
        this.spark = spark;
        this.numPartitions = numPartitions;
    }

    /**
     * Top-level method for importing tables from CSV files into MacroBase SQL
     *
     * @return A DataFrame that contains the data loaded from the CSV file
     * @throws MacroBaseSQLException if there's an error parsing the CSV file
     */
    Dataset<Row> importTableFromCsv(ImportCsv importStatement) throws MacroBaseSQLException {
        // Increase the number of partitions to ensure enough memory for parsing overhead
        int increasedPartitions = numPartitions * 10;
        final String fileName = importStatement.getFilename();
        final String tableName = importStatement.getTableName().toString();
        final Map<String, ColType> schema = importStatement.getSchema();
        try {
            // Distribute and parse
            JavaRDD<String> fileRDD = spark.sparkContext().textFile(fileName, increasedPartitions).toJavaRDD();
            // Extract the header
            CsvParserSettings headerSettings = new CsvParserSettings();
            headerSettings.getFormat().setLineSeparator("\n");
            headerSettings.setMaxCharsPerColumn(16384);
            CsvParser headerParser = new CsvParser(headerSettings);
            String[] header = headerParser.parseLine(fileRDD.first());
            // Remove the header
            fileRDD = fileRDD.mapPartitionsWithIndex(
                    (Integer index, Iterator<String> iter) -> {
                        if (index == 0) {
                            iter.next();
                        }
                        return iter;
                    }, true
            );
            JavaRDD<String> repartitionedRDD = fileRDD.repartition(increasedPartitions);
            Map<Integer, ColType> indexToColTypeMap = new HashMap<>();
            for (int i = 0; i < header.length; i++) {
                if (schema.containsKey(header[i])) {
                    indexToColTypeMap.put(i, schema.get(header[i]));
                }
            }

            // Create a schema from the header for the eventual Spark Dataframe.
            List<StructField> fields = new ArrayList<>();
            for (int i = 0 ; i < header.length; i ++) {
                if (schema.containsKey(header[i])) {
                    String fieldName = header[i];
                    if (schema.get(fieldName) == ColType.STRING) {
                        StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
                        fields.add(field);
                    } else if (schema.get(fieldName) == ColType.DOUBLE) {
                        StructField field = DataTypes.createStructField(fieldName, DataTypes.DoubleType, true);
                        fields.add(field);
                    } else {
                        throw new MacroBaseSQLException("Only strings and doubles supported in schema");
                    }
                }
            }

            JavaRDD<Row> datasetRowRDD = repartitionedRDD.mapPartitions(
                    (Iterator<String> iter) -> {
                        CsvParserSettings settings = new CsvParserSettings();
                        settings.getFormat().setLineSeparator("\n");
                        settings.setMaxCharsPerColumn(16384);
                        CsvParser csvParser = new CsvParser(settings);
                        List<Row> parsedRows = new ArrayList<>();
                        while(iter.hasNext()) {
                            String row = iter.next();
                            String[] record = csvParser.parseLine(row);
                            List<Object> rowList = new ArrayList<>();
                            for (int i = 0; i < record.length; i++) {
                                if (indexToColTypeMap.containsKey(i)) {
                                    if (indexToColTypeMap.get(i) == ColType.STRING) {
                                        rowList.add(record[i]);
                                    } else if (indexToColTypeMap.get(i) == ColType.DOUBLE) {
                                        try {
                                            rowList.add(Double.parseDouble(record[i]));
                                        } catch (NumberFormatException e) {
                                            rowList.add(Double.NaN);
                                        }
                                    } else {
                                        throw new MacroBaseSQLException("Only strings and doubles supported in schema");
                                    }
                                }
                            }
                            parsedRows.add(RowFactory.create(rowList.toArray()));
                        }
                        return parsedRows.iterator();
                    }, true
            );
            // Create the Spark Dataset from the RDD of rows and the schema.
            Dataset<Row> df = spark.createDataFrame(datasetRowRDD, DataTypes.createStructType(fields));
            // Register the Dataset by name so Spark SQL commands recognize it.
            df.createOrReplaceTempView(tableName);
            return df;
        } catch (Exception e) {
            throw new MacroBaseSQLException(e.getMessage());
        }
    }

    /**
     * Top-level method for executing a SQL query in MacroBase SQL
     *
     * @return A DataFrame corresponding to the results of the query
     * @throws MacroBaseException If there's an error -- syntactic or logical -- processing the
     * query, an exception is thrown
     */
    Dataset<Row> executeQuery(Query query) throws MacroBaseException {
        QueryBody queryBody = query.getQueryBody();
        if (queryBody instanceof QuerySpecification) {
            // If the query is pure SQL (without MBSQL commands) just execute it
            // using Spark-SQL.
            QuerySpecification querySpec = (QuerySpecification) queryBody;
            String sqlString = SqlFormatter.formatSql(query, Optional.empty());
            log.debug(querySpec.toString());
            return spark.sql(sqlString);
        } else if (queryBody instanceof DiffQuerySpecification) {
            // If the query is a Diff (and possibly Split) operator from MBSQL, process it.
            DiffQuerySpecification diffQuery = (DiffQuerySpecification) queryBody;
            log.debug(diffQuery.toString());
            return executeDiffQuerySpec(diffQuery);
        }
        throw new MacroBaseSQLException(
                "query of type " + queryBody.getClass().getSimpleName() + " not yet supported");
    }

    /**
     * Execute a DIFF query, a query that's specific to MacroBase SQL (i.e., a query that may
     * contain DIFF and SPLIT operators).
     *
     * @return A DataFrame containing the results of the query
     * @throws MacroBaseException If there's an error -- syntactic or logical -- processing the
     * query, an exception is thrown
     */
    private Dataset<Row> executeDiffQuerySpec(final DiffQuerySpecification diffQuery)
            throws MacroBaseException {
        final String outlierColName = "outlier_col";
        final Dataset<Row> outliersDF;
        final Dataset<Row> inliersDF;

        // First, partition the dataset into inlier and outliers.
        if (diffQuery.hasTwoArgs()) {
            // Case 1: Two separate subqueries
            final TableSubquery first = diffQuery.getFirst().get();
            final TableSubquery second = diffQuery.getSecond().get();

            // execute subqueries
             outliersDF = executeQuery(first.getQuery());
             inliersDF = executeQuery(second.getQuery());
        } else {
            // Case 2: A single SPLIT (...) WHERE ... query
            final SplitQuery splitQuery = diffQuery.getSplitQuery().get();
            String whereString = SqlFormatter.formatSql(splitQuery.getWhereClause(), Optional.empty());
            String relationString;

            final Relation inputRelation = splitQuery.getInputRelation();
            if (inputRelation instanceof TableSubquery) {
                final Query subquery = ((TableSubquery) inputRelation).getQuery();
                relationString = SqlFormatter.formatSql(subquery, Optional.empty());
            } else {
                // instance of Table
                relationString = "SELECT * FROM " + ((Table) inputRelation).getName().toString();
            }
            String outliersString = relationString + " WHERE " + whereString;
            String inliersString = relationString + " WHERE NOT " + whereString;
            outliersDF = spark.sql(outliersString);
            inliersDF = spark.sql(inliersString);
        }

        // Next, identify the attribute columns to examine.
        List<String> explainCols = diffQuery.getAttributeCols().stream()
                .map(Identifier::getValue)
                .collect(Collectors.toList());
        if ((explainCols.size() == 1) && explainCols.get(0).equals("*")) {
            throw new MacroBaseSQLException("No ON * yet");
        }

        // Make sure all attribute columns are valid.
        for (String explainCol: explainCols) {
            if (!Arrays.asList(outliersDF.columns()).contains(explainCol))
                throw new MacroBaseSQLException(
                        "ON " + Joiner.on(", ").join(explainCols) + " not present in table");
        }

        final double minRatioMetric = diffQuery.getMinRatioExpression().getMinRatio();
        final double minSupport = diffQuery.getMinSupportExpression().getMinSupport();

        // Execute diff
        final APLOutlierSummarizerDistributed summarizer = new APLOutlierSummarizerDistributed(true);
        summarizer.setMinSupport(minSupport);
        summarizer.setMinRatioMetric(minRatioMetric);
        summarizer.setOutlierColumn(outlierColName);
        summarizer.setAttributes(explainCols);
        summarizer.setNumPartitions(numPartitions);

        try {
            summarizer.process(outliersDF, inliersDF);
        } catch (Exception e) {
            e.printStackTrace();
        }
        final DataFrame resultDf = summarizer.getResults().toDataFrame(explainCols);
        resultDf.renameColumn("outliers", "outlier_count");
        resultDf.renameColumn("count", "total_count");

        // Convert the Diff result into a Spark Dataset
        Dataset<Row> explanationDataset = singleNodeDataFrameToSparkDataFrame(resultDf, spark);

        // Run the remainder of the diff query (SELECT and a possible WHERE) on the diff result.
        explanationDataset.createOrReplaceTempView("__RESERVEDDIFFQUERYTEMPVIEW__");
        String outerQuery = SqlFormatter.formatSql(diffQuery.getSelect(), Optional.empty())
                + " FROM __RESERVEDDIFFQUERYTEMPVIEW__";
        if (diffQuery.getWhere().isPresent()) {
            outerQuery = outerQuery + " WHERE " + SqlFormatter.formatSql(diffQuery.getWhere().get(), Optional.empty());
        }

        return spark.sql(outerQuery);
    }
}
