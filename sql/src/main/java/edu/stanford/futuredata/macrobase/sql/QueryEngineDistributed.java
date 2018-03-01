package edu.stanford.futuredata.macrobase.sql;

import com.google.common.base.Joiner;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import edu.stanford.futuredata.macrobase.analysis.MBFunction;
import edu.stanford.futuredata.macrobase.analysis.summary.aplinear.APLExplanation;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema.ColType;
import edu.stanford.futuredata.macrobase.distributed.analysis.summary.aplinearDistributed.APLOutlierSummarizerDistributed;
import edu.stanford.futuredata.macrobase.sql.tree.*;
import edu.stanford.futuredata.macrobase.sql.tree.LogicalBinaryExpression.Type;
import edu.stanford.futuredata.macrobase.sql.tree.SortItem.Ordering;
import edu.stanford.futuredata.macrobase.util.MacrobaseException;
import edu.stanford.futuredata.macrobase.util.MacrobaseSQLException;

import java.util.*;
import java.util.function.DoublePredicate;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;

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

    QueryEngineDistributed(SparkSession spark) {
        this.spark = spark;
        numPartitions = 4; // TODO: add configuration parameter for numPartitions
    }

    /**
     * Top-level method for importing tables from CSV files into MacroBase SQL
     *
     * @return A DataFrame that contains the data loaded from the CSV file
     * @throws MacrobaseSQLException if there's an error parsing the CSV file
     */
    Dataset<Row> importTableFromCsv(ImportCsv importStatement) throws MacrobaseSQLException {
        final String fileName = importStatement.getFilename();
        final String tableName = importStatement.getTableName().toString();
        final Map<String, ColType> schema = importStatement.getSchema();
        try {
            // Distribute and parse
            JavaRDD<String[]> datasetRDD = spark.sparkContext().textFile(fileName, numPartitions).toJavaRDD().mapPartitions(
                    (Iterator<String> iter) -> {
                        CsvParserSettings settings = new CsvParserSettings();
                        settings.getFormat().setLineSeparator("\n");
                        settings.setMaxCharsPerColumn(16384);
                        CsvParser csvParser = new CsvParser(settings);
                        List<String[]> parsedRows = new ArrayList<>();
                        while(iter.hasNext()) {
                            String row = iter.next();
                            String[] parsedRow = csvParser.parseLine(row);
                            parsedRows.add(parsedRow);
                        }
                        return parsedRows.iterator();
                    }, true
            );
            // Extract the header
            String[] header = datasetRDD.first();
            Map<Integer, ColType> indexToColTypeMap = new HashMap<>();
            for (int i = 0; i < header.length; i++) {
                if (schema.containsKey(header[i])) {
                    indexToColTypeMap.put(i, schema.get(header[i]));
                }
            }
            // Remove the header
            datasetRDD = datasetRDD.mapPartitionsWithIndex(
                    (Integer index, Iterator<String[]> iter) -> {
                        if (index == 0) {
                            iter.next();
                            iter.remove();
                        }
                        return iter;
                    }, true
            );
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
                        throw new MacrobaseSQLException("Only strings and doubles supported in schema");
                    }
                }
            }
            JavaRDD<Row> datasetRowRDD = datasetRDD.map((String[] record) ->
            {
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
                            throw new MacrobaseSQLException("Only strings and doubles supported in schema");
                        }
                    }
                }
                return RowFactory.create(rowList.toArray());
            });
            Dataset<Row> df = spark.createDataFrame(datasetRowRDD, DataTypes.createStructType(fields));
            df.createOrReplaceTempView(tableName);
            return df;
        } catch (Exception e) {
            throw new MacrobaseSQLException(e.getMessage());
        }
    }

    /**
     * Top-level method for executing a SQL query in MacroBase SQL
     *
     * @return A DataFrame corresponding to the results of the query
     * @throws MacrobaseException If there's an error -- syntactic or logical -- processing the
     * query, an exception is thrown
     */
    Dataset<Row> executeQuery(Query query) throws MacrobaseException {
        QueryBody queryBody = query.getQueryBody();
        if (queryBody instanceof QuerySpecification) {
            QuerySpecification querySpec = (QuerySpecification) queryBody;
            String sqlString = SqlFormatter.formatSql(query, Optional.empty());
            log.debug(querySpec.toString());
            return spark.sql(sqlString);

        } else if (queryBody instanceof DiffQuerySpecification) {
            DiffQuerySpecification diffQuery = (DiffQuerySpecification) queryBody;
            log.debug(diffQuery.toString());
            return executeDiffQuerySpec(diffQuery);
        }
        throw new MacrobaseSQLException(
                "query of type " + queryBody.getClass().getSimpleName() + " not yet supported");
    }

    /**
     * Execute a DIFF query, a query that's specific to MacroBase SQL (i.e., a query that may
     * contain DIFF and SPLIT operators).
     *
     * @return A DataFrame containing the results of the query
     * @throws MacrobaseException If there's an error -- syntactic or logical -- processing the
     * query, an exception is thrown
     */
    private Dataset<Row> executeDiffQuerySpec(final DiffQuerySpecification diffQuery)
            throws MacrobaseException {
        final String outlierColName = "outlier_col";
        final Dataset<Row> outliersDF;
        final Dataset<Row> inliersDF;

        if (diffQuery.hasTwoArgs()) {
            // case 1: two separate subqueries
            final TableSubquery first = diffQuery.getFirst().get();
            final TableSubquery second = diffQuery.getSecond().get();

            // execute subqueries
             outliersDF = executeQuery(first.getQuery());
             inliersDF = executeQuery(second.getQuery());
        } else {
            // case 2: single SPLIT (...) WHERE ... query
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

        List<String> explainCols = diffQuery.getAttributeCols().stream()
                .map(Identifier::getValue)
                .collect(Collectors.toList());
        if ((explainCols.size() == 1) && explainCols.get(0).equals("*")) {
            throw new MacrobaseSQLException("No ON * yet");
        }

        for (String explainCol: explainCols) {
            if (!Arrays.asList(outliersDF.columns()).contains(explainCol))
                throw new MacrobaseSQLException(
                        "ON " + Joiner.on(", ").join(explainCols) + " not present in table");
        }

        // TODO: if an explainCol isn't in the SELECT clause, don't include it
        final double minRatioMetric = diffQuery.getMinRatioExpression().getMinRatio();
        final double minSupport = diffQuery.getMinSupportExpression().getMinSupport();

        // execute diff
        final APLOutlierSummarizerDistributed summarizer = new APLOutlierSummarizerDistributed();
        summarizer.setMinSupport(minSupport);
        summarizer.setMinRatioMetric(minRatioMetric);
        summarizer.setOutlierColumn(outlierColName);
        summarizer.setAttributes(explainCols);
        summarizer.setNumPartitions(numPartitions);

        try {
            summarizer.process(outliersDF, inliersDF);
        } catch (Exception e) {
            // TODO: get rid of this Exception
            e.printStackTrace();
        }
        final DataFrame resultDf = summarizer.getResults().toDataFrame(explainCols);
        resultDf.renameColumn("outliers", "outlier_count");
        resultDf.renameColumn("count", "total_count");

        Dataset<Row> explanationDataset = singleNodeDataFrameToSparkDataFrame(resultDf, spark);
        explanationDataset.createOrReplaceTempView("__RESERVEDDIFFQUERYTEMPVIEW__");

        String outerQuery = SqlFormatter.formatSql(diffQuery.getSelect(), Optional.empty())
                + " FROM __RESERVEDDIFFQUERYTEMPVIEW__";
        if (diffQuery.getWhere().isPresent()) {
            outerQuery = outerQuery + " WHERE " + SqlFormatter.formatSql(diffQuery.getWhere().get(), Optional.empty());
        }

        return spark.sql(outerQuery);
    }
}
