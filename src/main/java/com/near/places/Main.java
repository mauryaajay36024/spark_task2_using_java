package com.near.places;

import com.near.places.service.OperatorInfo;
import com.near.places.service.PoiInfo;
import com.near.places.utility.Resource;

public class Main {
  public static void main(String[] args) {
    Resource resource=new Resource();
    OperatorInfo operatorInfo=new OperatorInfo();
    PoiInfo poiInfo=new PoiInfo();
    // To read the data from source file
    resource.readParquetFile();

    // Mobile devices used under different operators
    operatorInfo.mobileInfoForOperators();

    // Nearest POI’s for all the latitude longitude in
    poiInfo.nearestPoiDetail();

  }
}
