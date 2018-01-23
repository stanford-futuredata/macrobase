IMPORT FROM CSV FILE 'core/demo/wikiticker.csv' INTO wiki(time string, channel
  string, cityName string, comment string, countryIsoCode string, countryName
  string, isAnonymous string, isMinor string, isNew string, isRobot string,
  isUnpatrolled string, metroCode string, namespace string, page string,
  regionIsoCode string, regionName string, user string, delta double, added
  double, deleted double);

SELECT normalize(delta) as norm FROM wiki;

SELECT percentile(delta) as percentile FROM wiki;

SELECT * FROM DIFF (SPLIT (SELECT *, percentile(delta) as percentile FROM wiki) WHERE percentile > 0.95) ON *;

SELECT * FROM DIFF (SPLIT (SELECT *, percentile(delta) as percentile FROM wiki) WHERE percentile > 0.95) ON * WITH MIN SUPPORT 0.1;

SELECT * FROM DIFF (SPLIT (SELECT *, percentile(delta) as percentile FROM wiki) WHERE percentile > 0.95)
  ON channel, isAnonymous, isMinor, isNew, isRobot, isUnpatrolled, namespace
  WITH MIN RATIO 2.0 MIN SUPPORT 0.01
  ORDER BY global_ratio DESC;

SELECT * FROM DIFF (SPLIT (SELECT *, normalize(added) as norm FROM wiki) WHERE  norm > 0.005)
  ON isRobot, channel, isUnpatrolled, isNew, isMinor, isAnonymous, namespace
  WITH MIN SUPPORT 0.1 ORDER BY support DESC;

SELECT * FROM DIFF (SPLIT wiki WHERE deleted > 0.0)
  ON isRobot, channel, isUnpatrolled, isNew, isMinor, isAnonymous, namespace
  WITH MIN SUPPORT 0.1 ORDER BY global_ratio;
