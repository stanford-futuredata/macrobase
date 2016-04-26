package macrobase.util;

import macrobase.conf.MacroBaseConf;
import macrobase.datamodel.Datum;

import java.util.List;

public class Drainer {
    public static List<Datum> drainIngest(MacroBaseConf conf) throws Exception {
        return conf.constructIngester().getStream().drain();
    }

}
