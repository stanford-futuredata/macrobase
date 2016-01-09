package macrobase.ingest;

import com.google.common.collect.Sets;
import macrobase.datamodel.Datum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/*
 This is an ugly class, but it really speeds up interactive
 data processing.
 */

abstract public class MemoryCachingSQLLoader extends SQLLoader {
    private static final Logger log = LoggerFactory.getLogger(MemoryCachingSQLLoader.class);

    private class DataCache {
        private final DatumEncoder encoder;
        private final List<String> attributes;
        private final List<String> lowMetrics;

        public DatumEncoder getEncoder() {
            return encoder;
        }

        public List<String> getAttributes() {
            return attributes;
        }

        public List<String> getLowMetrics() {
            return lowMetrics;
        }

        public List<String> getHighMetrics() {
            return highMetrics;
        }

        public List<Datum> getResults() {
            return results;
        }

        public String getBaseQuery() {
            return baseQuery;
        }

        private final List<String> highMetrics;
        private final List<Datum> results;
        private final String baseQuery;

        public DataCache(DatumEncoder encoder,
                         List<String> attributes,
                         List<String> lowMetrics,
                         List<String> highMetrics,
                         String baseQuery,
                         List<Datum> results) {
            this.encoder = encoder;
            this.attributes = attributes;
            this.lowMetrics = lowMetrics;
            this.highMetrics = highMetrics;
            this.results = results;
            this.baseQuery = baseQuery;
        }
    }

    private int getLimitSize(String baseQuery) {
        if(baseQuery.contains("LIMIT")) {
            return Integer.parseInt(baseQuery.split("LIMIT")[1].split(";")[0]);
        } else {
            return Integer.MAX_VALUE;
        }
    }

    private boolean checkMatchingBaseQuery(String newBase,
                                          String oldBase) {
        if(newBase.equals(oldBase)) {
            return true;
        }

        String newWithoutLimit = newBase.split("LIMIT")[0];
        String oldWithoutLimit = oldBase.split("LIMIT")[0];

        if(!newWithoutLimit.equals(oldWithoutLimit)) {
            return false;
        } else if(newBase.contains("LIMIT") && ! oldBase.contains("LIMIT")) {
            return true;
        } else if(newBase.contains("LIMIT") && oldBase.contains("LIMIT")) {
            int newLimitSize = Integer.parseInt(newBase.split("LIMIT")[1].split(";")[0]);
            int oldLimitSize = Integer.parseInt(oldBase.split("LIMIT")[1].split(";")[0]);

            if(newLimitSize > oldLimitSize) {
                return false;
            } else {
                return true;
            }
        }

        return false;
    }

    private List<DataCache> cachedResults = new ArrayList<>();

    private List<Datum> _getData(DatumEncoder encoder,
                                   List<String> attributes,
                                   List<String> lowMetrics,
                                   List<String> highMetrics,
                                   String baseQuery) throws SQLException, IOException {
        for(DataCache cached : cachedResults) {
            if(cached.getAttributes().equals(attributes) &&
               cached.getLowMetrics().equals(lowMetrics) &&
               cached.getHighMetrics().equals(highMetrics) &&
               checkMatchingBaseQuery(cached.getBaseQuery(), baseQuery)) {
                log.debug("Returning (exact) cached data!");

                List<Datum> ret = cached.getResults();

                int limitSize = getLimitSize(baseQuery);
                if(limitSize != Integer.MAX_VALUE && limitSize < ret.size()) {
                    ret.subList(0, limitSize);
                }

                encoder.copy(cached.getEncoder());

                // LRU
                cachedResults.remove(cached);
                cachedResults.add(cached);

                return ret;
            }
            // we have a subset of the metrics we're looking for...
            else if (Sets.difference(new HashSet<>(attributes),
                                     new HashSet<>(cached.getAttributes())).isEmpty() &&
                     Sets.difference(new HashSet<>(lowMetrics),
                                     new HashSet<>(cached.getLowMetrics())).isEmpty() &&
                     Sets.difference(new HashSet<>(highMetrics),
                                     new HashSet<>(cached.getHighMetrics())).isEmpty() &&
                     checkMatchingBaseQuery(cached.getBaseQuery(), baseQuery)) {
                log.debug("Returning (subset of metrics) cached data!");


                // mapping from old metric to new metric
                HashMap<Integer, Integer> oldToNewAttributeDimension = new HashMap<>();
                int newAttributeIndex = 0;
                for(String attribute : attributes) {
                    int oldIndex = cached.getAttributes().indexOf(attribute);
                    oldToNewAttributeDimension.put(oldIndex, newAttributeIndex);
                    newAttributeIndex++;
                }

                encoder.updateAttributeDimensions(oldToNewAttributeDimension);

                // mapping from old metric to new metric
                HashMap<Integer, Integer> updatedMetricDimensionMapping = new HashMap<>();
                int newMetricIndex = 0;
                for(String lowMetric : lowMetrics) {
                    int oldIndex = cached.getLowMetrics().indexOf(lowMetric);
                    updatedMetricDimensionMapping.put(oldIndex, newMetricIndex);
                    newMetricIndex++;
                }

                for(String highMetric : highMetrics) {
                    int oldIndex = cached.getLowMetrics().size() + cached.getHighMetrics().indexOf(highMetric);
                    updatedMetricDimensionMapping.put(oldIndex, newMetricIndex);
                    newMetricIndex++;
                }

                List<Datum> ret = new ArrayList<>(cached.getResults().size());

                int limitSize = getLimitSize(baseQuery);

                Set<Integer> targetAttributes = new HashSet<>(oldToNewAttributeDimension.keySet());
                Set<Integer> targetMetrics = new HashSet<>(updatedMetricDimensionMapping.keySet());
                for(Datum d : cached.getResults()) {
                    ret.add(new Datum(d, targetAttributes, targetMetrics));

                    if(ret.size() == limitSize) {
                        break;
                    }
                }

                // LRU
                cachedResults.remove(cached);
                cachedResults.add(cached);

                return ret;
            }
        }

        List<Datum> data = super.getData(encoder, attributes, lowMetrics, highMetrics, baseQuery);
        cachedResults.add(new DataCache(encoder,
                                        attributes,
                                        lowMetrics,
                                        highMetrics,
                                        baseQuery,
                                        data));

        return data;
    }

    @Override
    public List<Datum> getData(DatumEncoder encoder,
                               List<String> attributes,
                               List<String> lowMetrics,
                               List<String> highMetrics,
                               String baseQuery) throws SQLException, IOException {

        while(true) {
            try {
                return _getData(encoder, attributes, lowMetrics, highMetrics, baseQuery);
            } catch (OutOfMemoryError e) {
                if(!cachedResults.isEmpty()) {
                    DataCache removed = cachedResults.remove(0);
                    log.info("Ran out of memory; freeing an entry ({}) in the cache...",
                             removed.getBaseQuery());
                } else {
                    throw e;
                }
            }
        }

    }
}
