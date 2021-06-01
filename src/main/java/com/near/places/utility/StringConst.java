package com.near.places.utility;

import org.apache.spark.sql.Column;

public interface StringConst {
  String LAT="lat";
  String LON="lng";
  String HOST="HOST";
  String PROXIMAV1API="PROXIMAV1API";
  String PLACES="places";
  String PROXIMAV2API="PROXIMAV2API";
  String PAYLOAD="payload";
  String CAT1="cat1";
  String CAT2="cat2";
  String NAME="name";
  String OVERWRITE="overwrite";
  String PARQUET="parquet";
  String POIINFOPATH="POIINFOPATH";
  String NEAR_PLACES="Near places";
  String FILEPATH = "FILEPATH";
  String PROPERTIES_FILE_PATH = "src/main/resources/application.properties";
  String QUERY = "select devCarrier,count(distinct ifa) AS numberOfMobileDevices from placesData group by devCarrier order by numberOfMobileDevices desc";
  String DEV_CARRIER = "devCarrier";
  String PLACES_DATA = "placesData";
  String OPERATORFILEPATH = "OPERATORFILEPATH";
  String HEADER = "header";
  String TRUE = "true";
  String SEP="sep";
  String POI_iD = "value";
}
