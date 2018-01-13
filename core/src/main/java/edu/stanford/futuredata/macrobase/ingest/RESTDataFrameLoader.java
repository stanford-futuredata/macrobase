package edu.stanford.futuredata.macrobase.ingest;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import okhttp3.*;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import javax.net.ssl.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.net.URL;
import java.security.cert.CertificateException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RESTDataFrameLoader implements DataFrameLoader{
    private String baseURL;
    private Map<String, String> headerParams;

    private boolean usePost = true;
    private String jsonBody;
    private Map<String, String> getParams;
    private Map<String, Schema.ColType> types;
    private List<String> requiredColumns;

    private OkHttpClient client;

    public RESTDataFrameLoader(
            String url,
            Map<String, String> headerParams,
            List<String> requiredColumns
    ) {
        this.requiredColumns= requiredColumns;
        this.baseURL = url;
        this.headerParams = headerParams;

        this.client = getUnsafeOkHttpClient();
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

    private static OkHttpClient getUnsafeOkHttpClient() {
        try {
            // Create a trust manager that does not validate certificate chains
            final TrustManager[] trustAllCerts = new TrustManager[] {
                    new X509TrustManager() {
                        @Override
                        public void checkClientTrusted(java.security.cert.X509Certificate[] chain, String authType) throws CertificateException {
                        }

                        @Override
                        public void checkServerTrusted(java.security.cert.X509Certificate[] chain, String authType) throws CertificateException {
                        }

                        @Override
                        public java.security.cert.X509Certificate[] getAcceptedIssuers() {
                            return new java.security.cert.X509Certificate[]{};
                        }
                    }
            };

            // Install the all-trusting trust manager
            final SSLContext sslContext = SSLContext.getInstance("SSL");
            sslContext.init(null, trustAllCerts, new java.security.SecureRandom());
            // Create an ssl socket factory with our all-trusting manager
            final SSLSocketFactory sslSocketFactory = sslContext.getSocketFactory();

            OkHttpClient.Builder builder = new OkHttpClient.Builder();
            builder.sslSocketFactory(sslSocketFactory, (X509TrustManager)trustAllCerts[0]);
            builder.hostnameVerifier(new HostnameVerifier() {
                @Override
                public boolean verify(String hostname, SSLSession session) {
                    return true;
                }
            });
            builder.connectTimeout(20, TimeUnit.SECONDS);

            OkHttpClient okHttpClient = builder.build();
            return okHttpClient;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
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

        CsvParserSettings settings = new CsvParserSettings();
        CsvParser csvParser = new CsvParser(settings);
        InputStream targetStream = new ByteArrayInputStream(response.getBytes(StandardCharsets.UTF_8.name()));
        InputStreamReader targetReader = new InputStreamReader(targetStream, "UTF-8");
        csvParser.beginParsing(targetReader);
        CSVDataFrameParser dfParser = new CSVDataFrameParser(csvParser, requiredColumns);
        dfParser.setColumnTypes(types);
        return dfParser.load();
    }
}
