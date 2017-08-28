package edu.stanford.futuredata.macrobase.contrib.aria;

import com.fasterxml.jackson.databind.ObjectMapper;
import edu.stanford.futuredata.macrobase.contrib.aria.json.APICubeRequest;
import edu.stanford.futuredata.macrobase.contrib.aria.json.CubeDimensionParser;
import edu.stanford.futuredata.macrobase.contrib.aria.json.CubeParser;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.util.MacrobaseException;
import okhttp3.*;

import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.*;
import java.util.concurrent.*;

public class CubeQueryService {
    // Params
    private URL baseURL;
    private Map<String, String> headerParams;
    private String startTime;
    private String endTime;

    private OkHttpClient client;
    private Map<String, Map<String, Double>> dimensionValuecounts;

    public static final List<String> quantileOperators = Arrays.asList(
            "Count",
            "Min",
            "Max",
            "Average",
            "StandardDeviation",
            "Percentile001",
            "Percentile01",
            "Percentile05",
            "Percentile50",
            "Percentile95",
            "Percentile99",
            "Percentile999"
    );

    public CubeQueryService(
            String baseURL,
            Map<String, String> headerParams,
            String startTime,
            String endTime
    ) throws MalformedURLException {
        if (!baseURL.endsWith("/")) {
            baseURL = baseURL+"/";
        }
        this.baseURL = new URL(baseURL);
        this.headerParams = headerParams;
        this.startTime = startTime;
        this.endTime = endTime;

        OkHttpClient.Builder b = new OkHttpClient.Builder();
        b.hostnameVerifier((hostname, session) -> true);
        b.connectTimeout(10, TimeUnit.SECONDS);
        this.client = b.build();

    }

    public DataFrame getFrequentCubeEntries(
            String measureName,
            List<String> dimNames,
            List<String> operations,
            double supportCutoff
    ) throws IOException, ExecutionException, InterruptedException, MacrobaseException {
        Map<String, List<String>> dimValues = getFrequentDimensionValues(
                measureName, dimNames, supportCutoff
        );
        return getCubeValues(
                measureName,
                dimValues,
                operations
        );
    }

    public DataFrame getAllCubeEntries(
            String measureName,
            List<String> dimNames,
            List<String> operations
    ) throws IOException {
        Map<String, List<String>> dimValues = getDimensionValues();
        HashMap<String, List<String>> selectedDimValues = new HashMap<>();
        for (String dim : dimNames) {
            selectedDimValues.put(dim, dimValues.get(dim));
        }
        return getCubeValues(
                measureName,
                selectedDimValues,
                operations
        );
    }

    public Map<String, List<String>> getFrequentDimensionValues(
            String measureName,
            List<String> dimNames,
            double supportCutoff
    ) throws IOException, InterruptedException, ExecutionException, MacrobaseException {
        Map<String, List<String>> dimValues = getDimensionValues();
        List<String> countOp = Arrays.asList("Count");

        ExecutorService executor = Executors.newFixedThreadPool(5);
        List<Callable<List<String>>> tasks = new ArrayList<>();

        for (String curDim: dimNames) {
            Callable<List<String>>  calcDimValues = () -> {
                List<String> curDimValues = dimValues.get(curDim);
                Map<String, List<String>> singletonDimMap = new HashMap<>();
                singletonDimMap.put(curDim, curDimValues);
                DataFrame curDimDF = getCubeValues(
                        measureName,
                        singletonDimMap,
                        countOp
                );

                double[] counts = curDimDF.getDoubleColumnByName("Count");
                String[] values = curDimDF.getStringColumnByName(curDim);
                double total = 0.0;
                for (double x : counts) {total += x;}

                ArrayList<String> curFrequent = new ArrayList<>();
                for (int i = 0; i < values.length; i++) {
                    if (counts[i] > total * supportCutoff) {curFrequent.add(values[i]);}
                }
                return curFrequent;
            };
            tasks.add(calcDimValues);
        }

        List<Future<List<String>>> futures = executor.invokeAll(tasks);
        executor.shutdown();
        Map<String, List<String>> frequentDimValues = new HashMap<>();
        for (int i = 0; i < dimNames.size(); i++) {
            Future<List<String>> curFuture = futures.get(i);
            if (curFuture.isCancelled()) {
                throw new MacrobaseException("Freq request failed");
            }
            frequentDimValues.put(dimNames.get(i), curFuture.get());
        }

        return frequentDimValues;
    }

    private String postRequest(URL url, String json_body) throws IOException {
        MediaType JSON = MediaType.parse("application/json; charset=utf-8");
        RequestBody body = RequestBody.create(JSON, json_body);
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

    private String getRequest(URL url, Map<String, String> params) throws IOException {
        HttpUrl.Builder httpBuilder = HttpUrl.get(url).newBuilder();
        for (String paramName : params.keySet()) {
            httpBuilder.addQueryParameter(paramName, params.get(paramName));
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

    public Map<String, List<String>> getDimensionValues() throws IOException {
        URL baseURL = new URL(
                this.baseURL,
                "dimensions"
        );
        Map<String, String> params = new HashMap<>();
        params.put("interval", String.format("%s/%s", startTime, endTime));
        String rawResponse = getRequest(baseURL, params);
        CubeDimensionParser p = CubeDimensionParser.loadFromString(rawResponse);
        return p.getDimensionValues();
    }

    public DataFrame getCubeValues(
            String measureName,
            Map<String, List<String>> dimValues,
            List<String> operations
    ) throws IOException {
        String pathString = String.format(
                "measures/%s/series/",
                measureName
        );
        URL mergedURL = new URL(baseURL, pathString);

        APICubeRequest req = APICubeRequest.fromDimensionValues(
                dimValues,
                startTime,
                endTime,
                operations
        );
        ObjectMapper mapper = new ObjectMapper();
        String rawResponse = postRequest(mergedURL, mapper.writeValueAsString(req));
        return CubeParser.loadFromString(rawResponse).getDataFrame();
    }
}
