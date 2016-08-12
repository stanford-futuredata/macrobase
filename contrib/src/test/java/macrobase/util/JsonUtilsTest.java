package macrobase.util;

import macrobase.diagnostics.JsonUtils;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileNotFoundException;
import java.io.UnsupportedEncodingException;

public class JsonUtilsTest {
    private static final Logger log = LoggerFactory.getLogger(JsonUtilsTest.class);

    @Test(expected=FileNotFoundException.class)
    public void unnecessaryTest() throws FileNotFoundException, UnsupportedEncodingException {
        double[] array = {1, 2, 2.4};
        JsonUtils.dumpAsJson(array, "nonExistingFolder/nonExistingDirectory");
    }

    @Test
    public void unnecessaryTestWithoutException() {
        double[] array = {1, 2, 2.4};
        JsonUtils.tryToDumpAsJson(array, "nonExistingFolder/nonExistingDirectory");
    }
}
