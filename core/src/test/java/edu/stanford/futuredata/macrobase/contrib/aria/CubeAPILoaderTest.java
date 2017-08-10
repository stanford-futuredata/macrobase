package edu.stanford.futuredata.macrobase.contrib.aria;

import okhttp3.*;
import org.junit.Test;

import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLSession;
import java.nio.file.Files;
import java.nio.file.Paths;

import static org.junit.Assert.*;

public class CubeAPILoaderTest {
    @Test
    public void testPost() throws Exception {
        String uri = "https://skypedatadev2cbocus01.cloudapp.net/"
                + "v4/tenants/09d258bbd26d42bfb5aa2b9f616ce639/providers/Rta/cubes/"
                + "95edf1a90cfa42f2a3e80dafd3df4bd6/measures/Dropped records/series";
        String apiKeyFile = "src/test/resources/apikey.txt";
        String apiKey = Files.readAllLines(Paths.get(apiKeyFile)).get(0);
        String json_request_file = "src/test/resources/aria_cube_request.json";
        String json_body = String.join(
                "\n",
                Files.readAllLines(Paths.get(json_request_file))
        );

        OkHttpClient.Builder b = new OkHttpClient.Builder();
        b.hostnameVerifier(new HostnameVerifier() {
            @Override
            public boolean verify(String hostname, SSLSession session) {
                return true;
            }
        });
        OkHttpClient client = b.build();

        MediaType JSON = MediaType.parse("application/json; charset=utf-8");
        RequestBody body = RequestBody.create(JSON, json_body);
        Request request = new Request.Builder()
                .url(uri)
                .post(body)
                .addHeader("X-ApiKey", apiKey)
                .addHeader("cache-control", "no-cache")
                .build();
        Response response = client.newCall(request).execute();
        System.out.println(response.body().string());

    }

}