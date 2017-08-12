package edu.stanford.futuredata.macrobase.example;

import com.google.common.collect.Lists;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.Location;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import edu.stanford.futuredata.macrobase.analysis.summary.apriori.APExplanation;
import edu.stanford.futuredata.macrobase.analysis.summary.apriori.APrioriSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.fpg.IncrementalSummarizer;
import edu.stanford.futuredata.macrobase.analysis.summary.ratios.PrevalenceRatio;
import edu.stanford.futuredata.macrobase.analysis.summary.ratios.RiskRatioMetric;
import edu.stanford.futuredata.macrobase.analysis.summary.util.AttributeEncoder;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Row;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.ingest.CSVDataFrameLoader;
import edu.stanford.futuredata.macrobase.ingest.DataFrameLoader;
import jdk.nashorn.internal.parser.JSONParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class TwitterQuery {
    private static final Logger log = LoggerFactory.getLogger("TwitterQuery");

    private static final BlockingQueue<String> msgQueue= new LinkedBlockingQueue<>(100000);
    ;

    private static void setup() throws Exception {
        boolean dolog = true;
        if(dolog) {
            BlockingQueue<String> inputQueue = new LinkedBlockingQueue<>(10000);

            BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

            /** Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
            Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
            StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
            List<Location> locations = Lists.newArrayList(
                    // SF
                    new Location(new Location.Coordinate(-122.55, 37.65),
                                 new Location.Coordinate(-122.375, 37.815)),
                    // Peninsula
                    new Location(new Location.Coordinate(-122.515, 37.26),
                                 new Location.Coordinate(-121.8, 37.649))
                                                         );

            hosebirdEndpoint.locations(locations);

            Authentication hosebirdAuth = new OAuth1(System.getenv("TWITTER_CONSUME_KEY"),
                                                     System.getenv("TWITTER_CONSUME_SECRET"),
                                                     System.getenv("TWITTER_TOKEN"),
                                                     System.getenv("TWITTER_TOKEN_SECRET"));

            log.info(System.getenv("TWITTER_CONSUME_KEY"),
                     System.getenv("TWITTER_CONSUME_SECRET"),
                     System.getenv("TWITTER_TOKEN"),
                     System.getenv("TWITTER_TOKEN_SECRET"));

            ClientBuilder builder = new ClientBuilder()
                    .name("Hosebird-Client-01")                              // optional: mainly for the logs
                    .hosts(hosebirdHosts)
                    .authentication(hosebirdAuth)
                    .endpoint(hosebirdEndpoint)
                    .processor(new StringDelimitedProcessor(inputQueue))
                    .eventMessageQueue(
                            eventQueue);                          // optional: use this if you want to process client events

            Client hosebirdClient = builder.build();
            // Attempts to establish a connection.
            hosebirdClient.connect();

            FileWriter fw = new FileWriter("tweets.txt");

            new Thread(() -> {
                while (!hosebirdClient.isDone()) {
                    try {
                        String msg = inputQueue.take();
                        msgQueue.put(msg);
                        log.debug(msg);
                        fw.write(msg);
                    } catch (Exception e) {
                        log.error("had an error:", e);
                    }
                }
            }).start();
        } else {
            try (BufferedReader br = new BufferedReader(new FileReader("tweets.txt"))) {
                String line;
                while ((line = br.readLine()) != null) {
                    msgQueue.add(line);
                }
            }

        }

    }

    private static final String TWITTER="EEE MMM dd HH:mm:ss ZZZZZ yyyy";
    private static SimpleDateFormat sf = new SimpleDateFormat(TWITTER);

    public static void main(String[] args) throws Exception {
        setup();
        sf.setLenient(true);

        List<Row> rows = new ArrayList<>();

        Schema s = new Schema();
        s.addColumn(Schema.ColType.STRING, "body");
        s.addColumn(Schema.ColType.DOUBLE, "time");
        s.addColumn(Schema.ColType.DOUBLE, "is_sf");


        boolean loop = true;
        while(loop) {
            String msg = msgQueue.take();
            JsonObject jsonObject = new JsonParser().parse(msg).getAsJsonObject();
            String text = jsonObject.get("text").getAsString().replaceAll("[^a-zA-Z 0-9.\\-;]+", "");
            Double time = (double) sf.parse(jsonObject.get("created_at").getAsString()).getTime();

            double lat = -1, lon = -1;

            if(jsonObject.has("geo") && !jsonObject.get("geo").isJsonNull()) {
                lon = jsonObject.get("geo").getAsJsonObject().get("coordinates").getAsJsonArray().get(0).getAsDouble();
                lat = jsonObject.get("geo").getAsJsonObject().get("coordinates").getAsJsonArray().get(1).getAsDouble();
            } else if(jsonObject.has("place")) {
                lon = jsonObject.get("place").getAsJsonObject().get("bounding_box").getAsJsonObject().get("coordinates").getAsJsonArray().get(0).getAsJsonArray().get(0).getAsJsonArray().get(0).getAsDouble();
                lat = jsonObject.get("place").getAsJsonObject().get("bounding_box").getAsJsonObject().get("coordinates").getAsJsonArray().get(0).getAsJsonArray().get(0).getAsJsonArray().get(1).getAsDouble();
            }

            double is_sf = lat <= 37.649 ? 1 : 0;

            System.out.printf("%f %f %f %b %s\n", lat, lon, time, is_sf, text);
            Row r = new Row(Lists.newArrayList(text, time, is_sf));
            rows.add(r);

            if(rows.size() % 3 == 0) {
                DataFrame df = new DataFrame(s, rows);

                APrioriSummarizer summarizer = new APrioriSummarizer();
                summarizer.setAttributes(Lists.newArrayList("body"));
                summarizer.setMinRatioMetric(2);
                summarizer.setMinSupport(.001);
                summarizer.setRatioMetric(new PrevalenceRatio());
                summarizer.setOutlierColumn("is_sf");

                summarizer.setEncoder(new AttributeEncoder()
                                              .setColumnNames(Lists.newArrayList("body"))
                                              .columnTokenizer("body",
                                                               (tw -> tw.toLowerCase().split(" "))));

                // get a frame of data
                summarizer.process(df);
                APExplanation ape = summarizer.getResults();
                System.out.println(ape.prettyPrint());            }
        }


    }
}
