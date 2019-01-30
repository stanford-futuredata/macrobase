package edu.stanford.futuredata.macrobase.pipeline;

import spark.utils.IOUtils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

public class PipelineUtilsTest {
    @Test
    public void testJsonStreamReading() throws Exception{
        // Setup
        FileInputStream inputStream = new FileInputStream("demo/sample_inlinecsv.json");
        String jsonString = IOUtils.toString(inputStream);
        PipelineConfig conf = PipelineConfig.fromJsonString(jsonString);
        Map<String, Object> jsonBody = conf.get("jsonBody", null);
        
        // Reader
        InputStreamReader reader = PipelineUtils.GetStreamReaderFromString(jsonBody.get("content").toString());

        // Parse
        CsvParserSettings settings = new CsvParserSettings();
        settings.setLineSeparatorDetectionEnabled(true);
        CsvParser csvParser = new CsvParser(settings);
        List<String[]> results = csvParser.parseAll(reader);
        assertEquals(18, results.get(0).length);
        assertEquals(49, results.toArray().length);
    }

    @Test
    public void testJsonStreamReadingWindowsLineEndings() throws Exception{
        // Setup
        FileInputStream inputStream = new FileInputStream("demo/sample_inlinecsv_windows.json");
        String jsonString = IOUtils.toString(inputStream);
        PipelineConfig conf = PipelineConfig.fromJsonString(jsonString);
        Map<String, Object> jsonBody = conf.get("jsonBody", null);
        
        // Reader
        InputStreamReader reader = PipelineUtils.GetStreamReaderFromString(jsonBody.get("content").toString());

        // Parse
        CsvParserSettings settings = new CsvParserSettings();
        settings.setLineSeparatorDetectionEnabled(true);
        CsvParser csvParser = new CsvParser(settings);
        List<String[]> results = csvParser.parseAll(reader);
        assertEquals(18, results.get(0).length);
        assertEquals(49, results.toArray().length);
    }
}