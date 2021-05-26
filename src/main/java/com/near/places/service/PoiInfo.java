package com.near.places.service;

import com.near.places.utility.Resource;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.io.IOException;
import java.util.Iterator;

public class PoiInfo {
  Resource resource=new Resource();
  public void nearestPoiDetail() throws IOException {
    Dataset dataFrame=resource.getDataFrame();

    Dataset<Row> locationData = dataFrame.select("lat","lng");
    for (Iterator<Row> rowData = locationData.toLocalIterator(); rowData.hasNext(); ) {
      String coordinate = (rowData.next()).toString();

      //Taking latitude and longitude
      String[] lat_lng_coordinate = get_lat_lng(coordinate);

      //Taking poiId
      String poiId=getPoiId(lat_lng_coordinate);

      //Get data for particular poiId
      getDataFromPoiId(poiId);
    }
    resource.getSpark().stop();
  }

  private static void getDataFromPoiId(String poiId) throws IOException {
    HttpClient client = new DefaultHttpClient();
    HttpGet request = new HttpGet("http://places.zprk.io/v2/places/IND?key=nearplacestestkey&near_poi_id="+poiId);
    HttpResponse response = client.execute(request);
    //Taking response
    String responseData = EntityUtils.toString(response.getEntity());

    System.out.println(responseData);

  }

  public static String getPoiId(String[] locationCoordinate) throws IOException {
    String poiId;
    //Sending GET request to fetch POIID
    HttpClient client = new DefaultHttpClient();
    HttpGet request = new HttpGet("http://places.zprk.io/v1/proxim/"+locationCoordinate[0]+"/"+locationCoordinate[1]+"/1000?withdist=true&sort=ASC");
    HttpResponse response = client.execute(request);
    //Taking response
    String responseData = EntityUtils.toString(response.getEntity());

    if(responseData.length()>30){
      poiId=responseData.substring(14,26);
      return poiId;
    }
    return null;
  }

  public static String[] get_lat_lng(String coordinate) {
    String[] coordinateValue = new String[2];
    String[] rawCoordinate = coordinate.split(",");
    coordinateValue[0]=rawCoordinate[0].replace("[", "");
    coordinateValue[1]=rawCoordinate[1].replace("]", "");

    return coordinateValue;
  }
}
