package macrobase.conf;

import macrobase.analysis.pipeline.stream.MBStream;
import macrobase.datamodel.Datum;
import macrobase.ingest.DataIngester;

class MockIngester extends DataIngester {
    public MockIngester(MacroBaseConf conf) throws ConfigurationException {
        super(conf);
    }

    @Override
    public MBStream<Datum> getStream() throws Exception {
        return null;
    }
}
