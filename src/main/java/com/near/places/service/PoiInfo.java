package com.near.places.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.near.places.utility.Resource;
import com.near.places.utility.StringConst;
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
  static List<String> poiIdList = new ArrayList<>();
  public void nearestPoiDetail() {
    Dataset<Row> dataFrame=resource.getDataFrame();
    Dataset<Row> locationDataRdd = dataFrame.select(StringConst.LAT,StringConst.LON);

    // Fetch nearest poiId for given latitude and longitude
    JavaRDD<Row> locationData = locationDataRdd.toJavaRDD();
    locationData.foreachPartition(iterator -> {
      while (iterator.hasNext()) {
        Row row = iterator.next();
        String poiId;
        if(!row.anyNull()) {
          poiId = getPoiId(row.getDouble(0), row.getDouble(1));
          if(poiId !=null) {
            poiIdList.add(poiId);
          }
        }
      }
    });
    //Converting poiIdList into dataframe
    Dataset<Row> poiIdDataset = resource.getSpark().createDataset(poiIdList, Encoders.STRING()).toDF();
    Dataset<Row> poiData = poiIdDataset.select(StringConst.POI_iD);

    // Fetch nearest poiId details
    poiData.foreachPartition(iterator -> {
      while (iterator.hasNext()) {
        Row row = iterator.next();
        if(!row.anyNull()) {
          getDataFromPoiId(row.getString(0));
        }
      }
    });
    savePoiDetailToFile(poiDetailList);
  }

  public String getPoiId(double latitude,double longitude)  {
    //Sending GET request to fetch PoiId
    HttpClient client = new DefaultHttpClient();
    HttpGet request = new HttpGet(String.format(resource.properties.getProperty(StringConst.PROXIMAV1API),latitude,longitude));
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
    if(jsonNode !=null && jsonNode.get(StringConst.PLACES).has(0)) {
      String[] poiId = jsonNode.get(StringConst.PLACES).get(0).get(0).asText().split("\\|");
      return poiId[0];
    }
    return null;
  }

  public void getDataFromPoiId(String poiId)  {
    HttpClient client = new DefaultHttpClient();
    HttpGet request = new HttpGet(String.format(resource.properties.getProperty(StringConst.PROXIMAV2API), poiId));
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
    if(jsonNode !=null) {
      if(jsonNode.get(StringConst.PAYLOAD).get(0) !=null ) {
        if(jsonNode.get(StringConst.PAYLOAD).get(0).has(StringConst.NAME)
            && jsonNode.get(StringConst.PAYLOAD).get(0).has(StringConst.CAT1) && jsonNode.get(StringConst.PAYLOAD).get(0).has(StringConst.CAT2)) {
          String brand = jsonNode.get(StringConst.PAYLOAD).get(0).get(StringConst.NAME).asText();
          String category1 = jsonNode.get(StringConst.PAYLOAD).get(0).get(StringConst.CAT1).asText();
          String category2 = jsonNode.get(StringConst.PAYLOAD).get(0).get(StringConst.CAT2).asText();
          String nearestPoiData = String.format("%s,%s,%s", brand, category1, category2);
          poiDetailList.add(nearestPoiData);
        }
      }
    }
  }

  //To convert the http response to json object
  public JsonNode convertStringToJson(HttpResponse response)  {
    if(response.getStatusLine().getStatusCode()==200) {
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
    }
    return null;
  }

  public void savePoiDetailToFile(List<String> poiDetailList){
    //Converting poiDetailList into dataframe
    Dataset<Row> poiDataset = resource.getSpark().createDataset(poiDetailList,Encoders.STRING()).toDF();
    poiDataset.write()
        .mode(StringConst.OVERWRITE)
        .format(StringConst.PARQUET)
        .save(resource.properties.getProperty(StringConst.POIINFOPATH));
  }
}
