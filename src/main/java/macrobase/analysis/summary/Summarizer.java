package macrobase.analysis.summary;

import macrobase.analysis.result.OutlierClassificationResult;
import macrobase.conf.MacroBaseConf;
import macrobase.ingest.DatumEncoder;

import java.util.Iterator;

/**
 * Consumes OutlierClassification Result tuple-at-a-time, but returns summaries
 # when there has been an update.
 */
public abstract class Summarizer implements Iterator<Summary> {

    protected final Iterator<OutlierClassificationResult> input;
    protected final DatumEncoder encoder;

    public
    Summarizer(MacroBaseConf conf, Iterator<OutlierClassificationResult> input) {
        this.input = input;
        this.encoder = conf.getEncoder();
    }

    @Override
    public abstract boolean hasNext();

    @Override
    public abstract Summary next();
}
