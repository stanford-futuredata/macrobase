package macrobase.diagnostics;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.io.UnsupportedEncodingException;

public class JsonUtils {

    public static void dumpAsJson(Object object, String filename) throws FileNotFoundException, UnsupportedEncodingException {

        Gson gson = new GsonBuilder()
                .enableComplexMapKeySerialization()
                .serializeNulls()
                .setPrettyPrinting()
                .setVersion(1.0)
                .create();
        final File dir = new File("target/scores");
        dir.mkdirs();
        PrintStream out = new PrintStream(new File(dir, filename), "UTF-8");
        out.println(gson.toJson(object));
    }

    public static void tryToDumpAsJson(Object object, String filename) {
        try {
            dumpAsJson(object, filename);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        }
    }
}
