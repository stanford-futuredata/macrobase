package macrobase.analysis.stats.kde;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.fasterxml.jackson.core.JsonFactory;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

public class TKDEConf {
    public static final String SUBKEY = "tkdeConf";
    // Classifier
    public double percentile = 0.01;
    // Quantile Estimation
    public int qSampleSize = 10000;
    public double qTolMultiplier = 0.01;
    public double qCutoffMultiplier = 1.5;
    // KDE
    public String kernel = "gaussian";
    public double bwMultiplier = 1.0;
    public boolean ignoreSelfScoring = false;
    public boolean calculateCutoffs = true;
    public double tolAbsolute = 0.0;
    public double cutoffHAbsolute = Double.MAX_VALUE;
    public double cutoffLAbsolute = 0.0;
    // Tree
    public int leafSize = 20;
    public boolean splitByWidth = true;
    // Grid
    public boolean useGrid = true;
    public List<Double> gridSizes = Arrays.asList(.8, .5);

    @Override
    public String toString() {
        return ToStringBuilder.reflectionToString(this, ToStringStyle.MULTI_LINE_STYLE);
    }

    public static TKDEConf load(String fileName) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(new File(fileName), TKDEConf.class);
    }

    public static TKDEConf parseYAML(String val) throws IOException {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        return mapper.readValue(val, TKDEConf.class);
    }
}
