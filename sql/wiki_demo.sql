IMPORT FROM CSV FILE 'core/demo/wikiticker.csv' INTO wiki(time string, channel
  string, cityName string, comment string, countryIsoCode string, countryName
  string, isAnonymous string, isMinor string, isNew string, isRobot string,
  isUnpatrolled string, metroCode string, namespace string, page string,
  regionIsoCode string, regionName string, user string, delta double, added
  double, deleted double);

SELECT NORMALIZE(delta) as norm FROM wiki;

SELECT * FROM DIFF (SPLIT (SELECT *, NORMALIZE(delta) as norm FROM wiki) WHERE norm > 0.01) ON *;

SELECT * FROM DIFF (SPLIT (SELECT *, NORMALIZE(delta) as norm FROM wiki) WHERE norm > 0.01) ON isRobot, isNew, isMinor, isAnonymous;

SELECT * FROM DIFF (SPLIT (SELECT *, NORMALIZE(delta) as norm FROM wiki) WHERE norm > 0.01) ON *;

SELECT * FROM DIFF (SPLIT (SELECT *, NORMALIZE(delta) as norm FROM wiki) WHERE norm > 0.01) ON isRobot, isNew, isMinor, isAnonymous, countryIsoCode;

SELECT * FROM DIFF (SPLIT (SELECT *, NORMALIZE(delta) as norm FROM wiki) WHERE norm > 0.01) ON isRobot, isNew, isMinor, isAnonymous WITH MIN SUPPORT 0.1;

SELECT * FROM DIFF (SPLIT (SELECT *, NORMALIZE(delta) as norm FROM wiki) WHERE norm > 0.01) ON * WITH MIN SUPPORT 0.1;

SELECT * FROM DIFF (SPLIT (SELECT *, NORMALIZE(delta) as norm FROM wiki) WHERE norm > 0.01) ON * WITH MIN SUPPORT 0.1 INTO OUTFILE 'temp.csv';

SELECT countryIsoCode FROM DIFF (SPLIT (SELECT *, NORMALIZE(delta) as norm FROM wiki) WHERE norm > 0.01) ON * WITH MIN SUPPORT 0.1 INTO OUTFILE 'temp.csv';

SELECT countryIsoCode FROM DIFF (SPLIT (SELECT *, NORMALIZE(delta) as norm FROM wiki) WHERE norm > 0.01) ON * WITH MIN SUPPORT 0.1 ORDER BY countryIsoCode;

SELECT countryIsoCode FROM DIFF (SPLIT (SELECT *, NORMALIZE(delta) as norm FROM wiki) WHERE norm > 0.01) ON * WITH MIN SUPPORT 0.1 ORDER BY countryIsoCode DESC;

SELECT countryIsoCode FROM DIFF (SPLIT (SELECT *, NORMALIZE(delta) as norm FROM wiki) WHERE norm > 0.01) ON * WITH MIN SUPPORT 0.1 ORDER BY countryIsoCode;

SELECT * FROM DIFF (SPLIT (SELECT *, NORMALIZE(delta) as norm FROM wiki) WHERE norm > 0.01) ON * WITH MIN SUPPORT 0.1;

SELECT regionName FROM DIFF (SPLIT (SELECT *, NORMALIZE(delta) as norm FROM wiki) WHERE norm > 0.01) ON * WITH MIN SUPPORT 0.1;

SELECT channel FROM DIFF (SPLIT (SELECT *, NORMALIZE(delta) as norm FROM wiki) WHERE norm > 0.01) ON * WITH MIN SUPPORT 0.1;

SELECT * FROM DIFF (SPLIT (SELECT *, NORMALIZE(delta) as norm FROM wiki) WHERE norm > 0.01) ON * WITH MIN SUPPORT 0.1;

SELECT * FROM DIFF (SPLIT (SELECT *, NORMALIZE(delta) as norm FROM wiki) WHERE norm > 0.01) ON isRobot, channel, isUnpatrolled, isNew, isMinor, isAnonymous, cityName, metroCode, namespace WITH MIN SUPPORT 0.1;

SELECT channel FROM DIFF (SPLIT (SELECT *, NORMALIZE(delta) as norm FROM wiki) WHERE norm > 0.01) ON isRobot, channel, isUnpatrolled, isNew, isMinor, isAnonymous, cityName, metroCode, namespace WITH MIN SUPPORT 0.1 ORDER BY channel;

SELECT * FROM DIFF (SPLIT (SELECT *, NORMALIZE(delta) as norm FROM wiki) WHERE norm > 0.01) ON isRobot, channel, isUnpatrolled, isNew, isMinor, isAnonymous, cityName, metroCode, namespace WITH MIN SUPPORT 0.1 ORDER BY channel;

SELECT * FROM DIFF (SPLIT (SELECT *, NORMALIZE(delta) as norm FROM wiki) WHERE norm > 0.01) ON isRobot, channel, isUnpatrolled, isNew, isMinor, isAnonymous, cityName, metroCode, namespace WITH MIN SUPPORT 0.1 ORDER BY metroCode;

SELECT * FROM DIFF (SPLIT (SELECT *, NORMALIZE(delta) as norm FROM wiki) WHERE norm > 0.01) ON isRobot, channel, isUnpatrolled, isNew, isMinor, isAnonymous, cityName, namespace WITH MIN SUPPORT 0.1;

SELECT * FROM DIFF (SPLIT (SELECT *, NORMALIZE(delta) as norm FROM wiki) WHERE norm > 0.01) ON isRobot, channel, isUnpatrolled, isNew, isMinor, isAnonymous, namespace WITH MIN SUPPORT 0.1 ORDER BY isRobot;

SELECT * FROM DIFF (SPLIT (SELECT *, NORMALIZE(delta) as norm FROM wiki) WHERE norm > 0.01) ON isRobot, channel, isUnpatrolled, isNew, isMinor, isAnonymous, namespace WITH MIN SUPPORT 0.1 ORDER BY global_ratio;

SELECT * FROM DIFF (SPLIT (SELECT *, normalize(added) as norm from wiki) where norm > 0.005) ON isRobot, channel, isUnpatrolled, isNew, isMinor, isAnonymous, namespace WITH MIN SUPPORT 0.1 ORDER BY support DESC;

SELECT * FROM DIFF (SPLIT wiki where deleted > 0.0) ON isRobot, channel, isUnpatrolled, isNew, isMinor, isAnonymous, namespace WITH MIN SUPPORT 0.1 ORDER BY global_ratio;
