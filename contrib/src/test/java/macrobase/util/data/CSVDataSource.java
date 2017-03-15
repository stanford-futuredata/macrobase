package macrobase.util.data;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;

import java.io.FileReader;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

/**
 * Quickly load csvs for use in tests
 */
public class CSVDataSource implements DataSource {
    public String fileName;
    public List<Integer> columns;
    public int limit = Integer.MAX_VALUE;
    public boolean hasHeader = true;

    public CSVDataSource(String fileName, List<Integer> columns) {
        this.fileName = fileName;
        this.columns = columns;
    }

    public CSVDataSource(String fileName, int numColumns) {
        this(fileName, new ArrayList<>(numColumns));
        for (int i = 0; i < numColumns; i++) {
            columns.add(i);
        }
    }

    public CSVDataSource setHasHeader(boolean flag) {
        this.hasHeader = flag;
        return this;
    }

    public CSVDataSource setLimit(int limit) {
        this.limit = limit;
        return this;
    }

    @Override
    public List<double[]> get() throws Exception {
        Reader in = new FileReader(fileName);
        CSVFormat format = CSVFormat.RFC4180;
        if (hasHeader) {
            format = format.withHeader().withSkipHeaderRecord();
        }
        Iterable<CSVRecord> records = format.parse(in);

        int n = columns.size();
        ArrayList<double[]> results = new ArrayList<>();

        int rowCount = 0;
        for (CSVRecord row : records) {
            double[] dRow = new double[n];
            for (int i = 0; i < row.size(); i++) {
                int j = columns.indexOf(i);
                if (j != -1) {
                    dRow[j] = Double.parseDouble(row.get(i));
                }
            }
            results.add(dRow);
            rowCount++;
            if (rowCount >= limit) {
                break;
            }
        }

        in.close();
        return results;
    }
}
