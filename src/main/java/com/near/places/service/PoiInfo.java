package com.near.places.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.near.places.utility.Resource;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import java.io.*;

public class PoiInfo implements Serializable {
  // Resource creates SparkSession
  Resource resource=new Resource();
  public void nearestPoiDetail() {
    Dataset<Row> dataFrame=resource.getDataFrame();
    Dataset<Row> locationDataRdd = dataFrame.select("lat","lng");

    locationDataRdd.foreachPartition(iterator -> {
      while (iterator.hasNext()) {
        Row row = iterator.next();

        // Fetch nearest poiId for given latitude and longitude
        String poiId=getPoiId(row.getDouble(0), row.getDouble(1));

        // fetch the details for nearest poiId
        getDataFromPoiId(poiId);
      }
    });
  }

  public String getPoiId(double latitude,double longitude) throws IOException {
    //Sending GET request to fetch PoiId
    HttpClient client = new DefaultHttpClient();
    HttpGet request = new HttpGet(String.format("http://places.zprk.io/v1/proxim/%f/%f/1000?withdist=true&sort=ASC",latitude,longitude));
    HttpResponse response = client.execute(request);

    //get jsonNode out of http response
    JsonNode jsonNode=stringToJson(response);
    String[] poiId = jsonNode.get("places").get(0).get(0).asText().split("\\|");
    return poiId[0];
  }

  public void getDataFromPoiId(String poiId) throws IOException {
    HttpClient client = new DefaultHttpClient();
    HttpGet request = new HttpGet(String.format("http://places.zprk.io/v2/places/IND?key=nearplacestestkey&near_poi_id=%s", poiId));
    HttpResponse response = client.execute(request);

    //get jsonNode out of http response
    JsonNode jsonNode = stringToJson(response);
    String brand = jsonNode.get("payload").get(0).get("name").asText();
    String category1 = jsonNode.get("payload").get(0).get("cat1").asText();
    String category2 = jsonNode.get("payload").get(0).get("cat2").asText();
    System.out.printf("brand=%s category1=%-25s category2=%-25s%n", brand, category1, category2);
  }

  //To convert the http response to json object
  public JsonNode stringToJson(HttpResponse response) throws IOException {
    String responseData = EntityUtils.toString(response.getEntity());
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.readTree(responseData);
  }
}
