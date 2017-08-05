package edu.stanford.futuredata.macrobase.ingest;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Row;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;

import java.io.IOException;
import java.util.List;

public class CSVDataFrameWriter {
    public void writeToStream(DataFrame df, Appendable out) throws IOException {
        String[] columnNames = df.getSchema().getColumnNames().toArray(new String[0]);
        CSVPrinter printer = CSVFormat.DEFAULT.withHeader(columnNames).print(out);

        List<Row> rows = df.getRows();
        for (Row curRow : rows) {
            List<Object> rowValues = curRow.getVals();
            printer.printRecord(rowValues);
        }
        printer.close();
    }
}
