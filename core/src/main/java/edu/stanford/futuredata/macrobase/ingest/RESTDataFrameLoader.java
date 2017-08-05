package edu.stanford.futuredata.macrobase.ingest;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import okhttp3.*;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;

import java.io.IOException;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RESTDataFrameLoader implements DataFrameLoader{
    private String baseURL;
    private Map<String, String> headerParams;

    private boolean usePost = true;
    private String jsonBody;
    private Map<String, String> getParams;
    private Map<String, Schema.ColType> types;

    private OkHttpClient client;

    public RESTDataFrameLoader(
            String url,
            Map<String, String> headerParams
    ) {
        this.baseURL = url;
        this.headerParams = headerParams;

        OkHttpClient.Builder b = new OkHttpClient.Builder();
        b.hostnameVerifier((hostname, session) -> true);
        b.connectTimeout(20, TimeUnit.SECONDS);
        this.client = b.build();
    }
    public void setUsePost(boolean flag) {
        this.usePost = flag;
    }
    public void setJsonBody(String jsonBody) {
        this.jsonBody = jsonBody;
    }
    public void setGetParams(Map<String, String> getParams) {
        this.getParams = getParams;
    }

    private String postRequest() throws IOException {
        URL url = new URL(baseURL);
        MediaType JSON = MediaType.parse("application/json; charset=utf-8");
        RequestBody body = RequestBody.create(JSON, jsonBody);
        Request.Builder requestBuilder = new Request.Builder()
                .url(url)
                .post(body);
        for (String headerKey: headerParams.keySet()) {
            requestBuilder.addHeader(headerKey, headerParams.get(headerKey));
        }
        Request request = requestBuilder.build();
        Response response = this.client.newCall(request).execute();
        return response.body().string();
    }

    private String getRequest() throws IOException {
        URL url = new URL(baseURL);
        HttpUrl.Builder httpBuilder = HttpUrl.get(url).newBuilder();
        if (getParams != null) {
            for (String paramName : getParams.keySet()) {
                httpBuilder.addQueryParameter(paramName, getParams.get(paramName));
            }
        }
        HttpUrl fullURL = httpBuilder.build();

        Request.Builder requestBuilder = new Request.Builder()
                .url(fullURL)
                .get();
        for (String headerKey: headerParams.keySet()) {
            requestBuilder.addHeader(headerKey, headerParams.get(headerKey));
        }
        Request request = requestBuilder.build();
        Response response = this.client.newCall(request).execute();
        return response.body().string();
    }


    @Override
    public DataFrameLoader setColumnTypes(Map<String, Schema.ColType> types) {
        this.types = types;
        return this;
    }

    @Override
    public DataFrame load() throws Exception {
        String response;
        if (usePost) {
            response = postRequest();
        } else {
            response = getRequest();
        }

        CSVParser csvParser = CSVParser.parse(
                response,
                CSVFormat.DEFAULT.withHeader()
        );
        CSVDataFrameParser dfParser = new CSVDataFrameParser(csvParser);
        dfParser.setColumnTypes(types);
        return dfParser.load();
    }
}
