package edu.stanford.futuredata.macrobase.ingest;

import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Row;
import java.io.IOException;
import java.io.Writer;
import java.util.List;

public class CSVDataFrameWriter {
    public void writeToStream(DataFrame df, Writer out) throws IOException {
        String[] columnNames = df.getSchema().getColumnNames().toArray(new String[0]);
        CsvWriter writer = new CsvWriter(out, new CsvWriterSettings());
        writer.writeHeaders(columnNames);

        List<Row> rows = df.getRows();
        for (Row curRow : rows) {
            List<Object> rowValues = curRow.getVals();
            writer.writeRow(rowValues);
        }
        writer.close();
    }
}
