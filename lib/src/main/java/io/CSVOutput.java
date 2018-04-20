package io;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class CSVOutput {
    private String baseDir = "results";
    private boolean addTimeStamp = true;

    public CSVOutput() {}

    public void writeAllResults(
            List<Map<String, String>> results,
            String fileName
    ) throws Exception {
        long seconds = System.currentTimeMillis() / 1000;
        String fName = "";
        if (addTimeStamp) {
            fName = String.format(
                    "%s/%s_%d.csv",
                    baseDir,
                    fileName,
                    seconds
            );
        } else {
            fName = String.format(
                    "%s/%s.csv",
                    baseDir,
                    fileName
            );
        }
        PrintWriter out = new PrintWriter(new BufferedWriter(new FileWriter(fName)));

        List<String> keys = new ArrayList<>(results.get(0).keySet());
        out.println(String.join(",", keys));
        for (Map<String, String> row : results) {
            List<String> vals = new ArrayList<>(keys.size());
            for (String key : keys) {
                vals.add(row.get(key));
            }
            out.println(String.join(",", vals));
        }
        out.close();
    }

    public void setAddTimeStamp(boolean addTimeStamp) {
        this.addTimeStamp = addTimeStamp;
    }
}
