package macrobase.util.data;

import java.util.List;

/**
 * Need a way to load lists of data for use in tests without
 * going through heavy-weight macrobase infrastructure
 */
public interface DataSource {
    List<double[]> get() throws Exception;
}