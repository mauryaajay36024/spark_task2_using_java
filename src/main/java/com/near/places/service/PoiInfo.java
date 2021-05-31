package com.near.places.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.near.places.utility.Resource;
import org.apache.http.HttpResponse;
import org.apache.http.client.HttpClient;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.DefaultHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.*;
import java.io.*;
import java.util.ArrayList;
import java.util.List;


public class PoiInfo implements Serializable {
  Resource resource=new Resource();
  static List<String> poiDetailList=new ArrayList<>();
  public void nearestPoiDetail() {
    Dataset<Row> dataFrame=resource.getDataFrame();
    Dataset<Row> locationDataRdd = dataFrame.select("lat","lng");

    JavaRDD<Row> locationData = locationDataRdd.toJavaRDD();
    locationData.foreachPartition(iterator -> {
      while (iterator.hasNext()) {
        Row row = iterator.next();
        String poiId=null;
        // Fetch nearest poiId for given latitude and longitude
        if(!row.anyNull()) {
          poiId = getPoiId(row.getDouble(0), row.getDouble(1));
        }

         //fetch the details for nearest poiId
        if(poiId !=null) {
          getDataFromPoiId(poiId);
        }
      }
    });

    savePoiDetailToFile(poiDetailList);
  }

  public String getPoiId(double latitude,double longitude)  {
    //Sending GET request to fetch PoiId
    HttpClient client = new DefaultHttpClient();
    HttpGet request = new HttpGet(String.format(resource.properties.getProperty("PROXIMAV1API"),latitude,longitude));
    HttpResponse response = null;
    try {
      response = client.execute(request);
    } catch (IOException e) {
      e.printStackTrace();
    }

    //get jsonNode out of http response
    JsonNode jsonNode = null;
    if(response !=null) {
      jsonNode = convertStringToJson(response);
    }

    if(jsonNode !=null && jsonNode.get("places").has(0)) {
      String[] poiId = jsonNode.get("places").get(0).get(0).asText().split("\\|");
      return poiId[0];
    }
    return null;
  }

  public void getDataFromPoiId(String poiId)  {
    HttpClient client = new DefaultHttpClient();
    HttpGet request = new HttpGet(String.format(resource.properties.getProperty("PROXIMAV2API"), poiId));
    HttpResponse response = null;
    try {
      response = client.execute(request);
    } catch (IOException e) {
      e.printStackTrace();
    }
    //get jsonNode out of http response
    JsonNode jsonNode = null;
    if(response !=null) {
      jsonNode = convertStringToJson(response);
    }

    if(jsonNode !=null && jsonNode.has("payload") && jsonNode.get("payload").get(0).has("name")
    && jsonNode.get("payload").get(0).has("cat1") && jsonNode.get("payload").get(0).has("cat2")) {
      String brand = jsonNode.get("payload").get(0).get("name").asText();
      String category1 = jsonNode.get("payload").get(0).get("cat1").asText();
      String category2 = jsonNode.get("payload").get(0).get("cat2").asText();
      String data=String.format("%s,%s,%s",brand,category1,category2);
      poiDetailList.add(data);
    }
  }

  //To convert the http response to json object
  public JsonNode convertStringToJson(HttpResponse response)  {
    String responseData = null;
    try {
      responseData = EntityUtils.toString(response.getEntity());
    } catch (IOException e) {
      e.printStackTrace();
    }
    ObjectMapper objectMapper = new ObjectMapper();
    try {
      return objectMapper.readTree(responseData);
    } catch (IOException e) {
      e.printStackTrace();
    }
    return null;
  }

  public void savePoiDetailToFile(List<String> poiDetailList){
    //Converting into dataframe for HTTP response
    Dataset<String> poiDataset = resource.getSpark().createDataset(poiDetailList,Encoders.STRING());
    Dataset<Row> poiDetailDataframe = resource.getSpark().read().json(poiDataset);

    poiDetailDataframe.write()
        .mode("overwrite")
        .format("parquet")
        .save(resource.properties.getProperty("POIINFOPATH"));

  }
}
