package macrobase.analysis.classify;

import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.List;

/**
 * Transparently dumps classifier output by wrapping another classifier and copying its
 * results to a file.
 */
public class DumpClassifier extends OutlierClassifier {
    private static final Logger log = LoggerFactory.getLogger(DumpClassifier.class);
    protected final OutlierClassifier input;
    protected MacroBaseConf conf;
    private PrintWriter out;
    private int count;

    private MBStream<OutlierClassificationResult> outputStream = new MBStream<>();

    private String filepath;

    public DumpClassifier(MacroBaseConf conf, OutlierClassifier input) throws IOException {
        this(conf, input, "default");
    }

    public DumpClassifier(MacroBaseConf conf, OutlierClassifier input, String name) throws IOException {
        this.conf = conf;
        this.input = input;
	// TODO: output directory should be configurable
        filepath = String.format("%s-dumpClassifier.txt", name);
        out = new PrintWriter(new BufferedWriter(new FileWriter(filepath)));
    }

    public String getFilePath(){
        return filepath;
    }

    @Override
    public void initialize() throws Exception {

    }

    @Override
    public void consume(List<Datum> records) throws Exception {

        input.consume(records);
        List<OutlierClassificationResult> results = input.getStream().drain();

        for(OutlierClassificationResult res : results) {
            int flag = 0;
            if (res.isOutlier()) {
                flag = 1;
            }
            out.format("%d,%d\n", count, flag);
            count++;
        }

        outputStream.add(results);
    }

    @Override
    public void shutdown() throws Exception {
        out.close();
    }

    @Override
    public MBStream<OutlierClassificationResult> getStream() throws Exception {
        return outputStream;
    }
}
