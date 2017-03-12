package macrobase.ingest;

import macrobase.datamodel.DataFrame;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.*;

import static macrobase.datamodel.Schema.ColType;

/**
 * Not as robust as the standard csv loader but 10x faster
 */
public class FastDataFrameCSVLoader implements DataFrameCSVLoader{
    private String fileName;
    private Map<String, ColType> columnTypes;
    private char delim = ',';

    public FastDataFrameCSVLoader(String fileName){
        this.fileName = fileName;
        this.columnTypes = new HashMap<>();
    }
    public FastDataFrameCSVLoader setColumnTypes(Map<String, ColType> types) {
        this.columnTypes = types;
        return this;
    }

    public DataFrame load() throws Exception {
        BufferedReader reader = new BufferedReader(new FileReader(fileName));

        String[] header = reader.readLine().split(String.valueOf(delim));
        int numColumns = header.length;

        int[] headerTypeInts = new int[numColumns];
        int[] indexToSubindex = new int[numColumns];
        int[] numColumnsPerType = new int[ColType.values().length];
        for (int i = 0; i < numColumns; i++) {
            String columnName = header[i];
            ColType t = columnTypes.getOrDefault(columnName, ColType.STRING);
            int tInt = t.ordinal();
            headerTypeInts[i] = tInt;
            indexToSubindex[i] = numColumnsPerType[tInt];
            numColumnsPerType[tInt]++;
        }

        int stringTypeInt = ColType.STRING.ordinal();
        int doubleTypeInt = ColType.DOUBLE.ordinal();

        int maxArraySize = 1000;
        String[][] stringCols = new String[numColumnsPerType[stringTypeInt]][maxArraySize];
        double[][] doubleCols = new double[numColumnsPerType[doubleTypeInt]][maxArraySize];

        CharDoubleParser parser = new CharDoubleParser();
        long startTime = System.currentTimeMillis();
        String curLine = reader.readLine();
        int numRows = 0;
        while(curLine != null) {
            if (numRows >= maxArraySize) {
                maxArraySize *= 4;
                for (int i = 0; i < stringCols.length; i++) {
                    stringCols[i] = Arrays.copyOf(stringCols[i], maxArraySize);
                }
                for (int i = 0; i < doubleCols.length; i++) {
                    doubleCols[i] = Arrays.copyOf(doubleCols[i], maxArraySize);
                }
            }

            char[] lineChars = curLine.toCharArray();
            int c = 0;
            int start = 0;
            int end = 0;
            while(c < numColumns) {
                while(end < lineChars.length && lineChars[end] != delim) {
                    end++;
                }
                int tInt = headerTypeInts[c];
                if (tInt == stringTypeInt) {
                    stringCols[indexToSubindex[c]][numRows] = new String(lineChars, start, end-start);
                } else if (tInt == doubleTypeInt) {
                    doubleCols[indexToSubindex[c]][numRows] = parser.parseDouble(lineChars, start, end);
                } else {
                    throw new RuntimeException("Bad ColType");
                }

                c++;
                start = end+1;
                end = start;
            }
            curLine = reader.readLine();
            numRows++;
        }
        long elapsed = System.currentTimeMillis() - startTime;

        for (int i = 0; i < stringCols.length; i++) {
            stringCols[i] = Arrays.copyOf(stringCols[i], numRows);
        }
        for (int i = 0; i < doubleCols.length; i++) {
            doubleCols[i] = Arrays.copyOf(doubleCols[i], numRows);
        }
//        System.out.println("Loading file took: "+elapsed);

        startTime = System.currentTimeMillis();
        DataFrame df = new DataFrame();
        for (int c = 0; c < numColumns; c++) {
            int tInt = headerTypeInts[c];
            if (tInt == stringTypeInt) {
                df.addStringColumn(header[c], stringCols[indexToSubindex[c]]);
            } else if (tInt == doubleTypeInt) {
                df.addDoubleColumn(header[c], doubleCols[indexToSubindex[c]]);
            } else {
                throw new RuntimeException("Bad ColType");
            }
        }
        elapsed = System.currentTimeMillis() - startTime;
//        System.out.println("Loading rows took: "+elapsed);
        return df;
    }
}
