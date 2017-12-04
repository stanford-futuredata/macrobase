package edu.stanford.futuredata.macrobase.sql;

import com.google.common.collect.ImmutableMap;
import edu.stanford.futuredata.macrobase.analysis.summary.ratios.ExplanationMetric;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.util.MacrobaseException;
import edu.stanford.futuredata.macrobase.util.MacrobaseSQLException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.roaringbitmap.RoaringBitmap;

public class Diff {


  public static DataFrame diff(final DataFrame outliers, final DataFrame inliers,
      final List<String> cols,
      final String ratioMetricStr, final int order) throws MacrobaseException {

    // return at the end
    final DataFrame resultDf = new DataFrame();

    final ExplanationMetric metricFn = ExplanationMetric.getMetricFn(ratioMetricStr);
    // global stats to pass to metricFn.calc whenever we compute ratio
    final int outlierCount = outliers.getNumRows();
    final int totalCount = outlierCount + inliers.getNumRows();

    // double column values that will be added to resultDf
    final List<Double> ratios = new LinkedList<>();
    final List<Double> supports = new LinkedList<>();
    final List<Double> matchedOutlierCounts = new LinkedList<>();

    // String column values that will be added to resultDf
    final Map<String, List<String>> colResultsMap = new HashMap<>();
    for (String col : cols) {
      colResultsMap.put(col, new LinkedList<>());
    }

    final Map<String, Map<String, RoaringBitmap>> bitmapsForColsOutliers = new HashMap<>();
    final Map<String, Map<String, RoaringBitmap>> bitmapsForColsInliers = new HashMap<>();

    // Singleton combinations
    for (String col : cols) {

      final String[] colValsOutliers = outliers.getStringColumnByName(col);
      final String[] colValsInliers = inliers.getStringColumnByName(col);

      if (colValsOutliers == null || colValsInliers == null) {
        throw new MacrobaseSQLException(
            "Column " + col + " is not an attribute column, but a metric column");
      }

      final Map<String, RoaringBitmap> bitmapsForColOutliers = generateBitmapsForColumn(
          colValsOutliers);
      bitmapsForColsOutliers.put(col, bitmapsForColOutliers);
      final Map<String, RoaringBitmap> bitmapsForColInliers = generateBitmapsForColumn(
          colValsInliers);
      bitmapsForColsInliers.put(col, bitmapsForColInliers);

      for (String colVal : bitmapsForColOutliers.keySet()) {
        if (!bitmapsForColInliers.containsKey(colVal)) {
          continue;
        }

        final Map<String, String> resultColVals = ImmutableMap.of(col, colVal);

        addColumnValuesToResultColumns(colResultsMap, resultColVals);

        addRatioValuesToResultColumns(metricFn, outlierCount, totalCount, ratios, supports,
            matchedOutlierCounts, bitmapsForColOutliers.get(colVal),
            bitmapsForColInliers.get(colVal));
      }
    }

    if (order >= 2) {
      for (int i = 0; i < cols.size(); ++i) {
        final String firstCol = cols.get(i);
        final Map<String, RoaringBitmap> bitmapsForFirstColOutliers = bitmapsForColsOutliers
            .get(firstCol);
        final Map<String, RoaringBitmap> bitmapsForFirstColInliers = bitmapsForColsInliers
            .get(firstCol);

        for (int j = i + 1; j < cols.size(); ++j) {
          final String secondCol = cols.get(j);
          final Map<String, RoaringBitmap> bitmapsForSecondColOutliers = bitmapsForColsOutliers
              .get(secondCol);
          final Map<String, RoaringBitmap> bitmapsForSecondColInliers = bitmapsForColsInliers
              .get(secondCol);

          for (String firstColVal : bitmapsForFirstColOutliers.keySet()) {
            if (!bitmapsForFirstColInliers.containsKey(firstColVal)) {
              continue;
            }

            final RoaringBitmap bitmapForFirstColValOutliers = bitmapsForFirstColOutliers
                .get(firstColVal);
            final RoaringBitmap bitmapForFirstColValInliers = bitmapsForFirstColInliers
                .get(firstColVal);

            for (String secondColVal : bitmapsForSecondColOutliers.keySet()) {
              if (!bitmapsForSecondColInliers.containsKey(secondColVal)) {
                continue;
              }

              final Map<String, String> resultColVals = ImmutableMap
                  .of(firstCol, firstColVal, secondCol, secondColVal);
              addColumnValuesToResultColumns(colResultsMap, resultColVals);

              final RoaringBitmap firstAndSecondColValsOutliers = RoaringBitmap
                  .and(bitmapForFirstColValOutliers, bitmapsForSecondColOutliers
                      .get(secondColVal));
              final RoaringBitmap firstAndSecondColValsInliers = RoaringBitmap
                  .and(bitmapForFirstColValInliers, bitmapsForSecondColInliers
                      .get(secondColVal));
              addRatioValuesToResultColumns(metricFn, outlierCount,
                  totalCount, ratios, supports, matchedOutlierCounts,
                  firstAndSecondColValsOutliers, firstAndSecondColValsInliers);

              if (order >= 3) {
                for (int k = j + 1; k < cols.size(); ++k) {
                  final String thirdCol = cols.get(k);
                  final Map<String, RoaringBitmap> bitmapsForThirdColOutliers = bitmapsForColsOutliers
                      .get(thirdCol);
                  final Map<String, RoaringBitmap> bitmapsForThirdColInliers = bitmapsForColsInliers
                      .get(thirdCol);

                  for (String thirdColVal : bitmapsForThirdColOutliers.keySet()) {
                    if (!bitmapsForThirdColInliers.containsKey(thirdColVal)) {
                      continue;
                    }

                    final Map<String, String> resultColValsAllThree = ImmutableMap
                        .of(firstCol, firstColVal, secondCol, secondColVal, thirdCol, thirdColVal);
                    addColumnValuesToResultColumns(colResultsMap, resultColValsAllThree);

                    final RoaringBitmap allThreeColValsOutliers = RoaringBitmap
                        .and(bitmapForFirstColValOutliers, bitmapsForSecondColOutliers
                            .get(secondColVal));
                    final RoaringBitmap allThreeColValsInliers = RoaringBitmap
                        .and(bitmapForFirstColValInliers, bitmapsForSecondColInliers
                            .get(secondColVal));
                    addRatioValuesToResultColumns(metricFn, outlierCount,
                        totalCount, ratios, supports, matchedOutlierCounts,
                        allThreeColValsOutliers, allThreeColValsInliers);
                  }
                }
              }
            }
          }
        }
      }
    }

    // Generate DataFrame with results
    for (String attr : colResultsMap.keySet()) {
      List<String> attrResultVals = colResultsMap.get(attr);
      resultDf.addColumn(attr, attrResultVals.toArray(new String[0]));
    }

    resultDf.addColumn(ratioMetricStr, ratios.stream().mapToDouble(x -> x).toArray());
    resultDf.addColumn("support", supports.stream().mapToDouble(x -> x).toArray());
    resultDf
        .addColumn("outlier_count", matchedOutlierCounts.stream().mapToDouble(x -> x).toArray());

    return resultDf;
  }

  /**
   *
   * @param colResultsMap
   * @param resultColVals
   */
  // Add attribute value to current column; add null to other columns
  private static void addColumnValuesToResultColumns(Map<String, List<String>> colResultsMap,
      final Map<String, String> resultColVals) {
    for (String col : colResultsMap.keySet()) {
      colResultsMap.get(col).add(resultColVals.get(col));
    }
  }

  /**
   *
   * @param metricFn
   * @param outlierCount
   * @param totalCount
   * @param ratios
   * @param supports
   * @param matchedOutlierCounts
   * @param bitmapOutlier
   * @param bitmapInlier
   */
  private static void addRatioValuesToResultColumns(ExplanationMetric metricFn, int outlierCount,
      int totalCount, List<Double> ratios, List<Double> supports, List<Double> matchedOutlierCounts,
      RoaringBitmap bitmapOutlier, RoaringBitmap bitmapInlier) {

    final int matchedOutlier = bitmapOutlier.getCardinality();
    matchedOutlierCounts.add((double) matchedOutlier);
    final int matchedTotal = matchedOutlier + bitmapInlier.getCardinality();

    final double ratio = metricFn.calc(matchedOutlier, matchedTotal, outlierCount, totalCount);
    ratios.add(ratio);
    final double support = (matchedOutlier + 0.0) / outlierCount;
    supports.add(support);
  }

  /**
   * For each distinct value in an array of Strings, generate a bitmap that tracks the indices of
   * when the value appears in the array.
   *
   * @param arr The array
   * @return A HashMap<String, RoaringBitmap>, where each key is a value that appears in @arr and
   * maps to a RoaringBitmap
   */
  private static Map<String, RoaringBitmap> generateBitmapsForColumn(final String[] arr) {
    Map<String, RoaringBitmap> map = new HashMap<>();
    for (int i = 0; i < arr.length; ++i) {
      final String val = arr[i];
      if (!map.containsKey(val)) {
        map.put(val, new RoaringBitmap());
      }
      final RoaringBitmap bitmap = map.get(val);
      bitmap.add(i);
    }
    return map;
  }
}
