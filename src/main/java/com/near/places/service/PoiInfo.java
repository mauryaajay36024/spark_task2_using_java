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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import java.io.*;
import java.util.ArrayList;
import java.util.List;


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
    HttpGet request = new HttpGet(String.format(resource.properties.getProperty("PROXIMAV1API"),latitude,longitude));
    HttpResponse response = client.execute(request);

    //get jsonNode out of http response
    JsonNode jsonNode=stringToJson(response);
    String[] poiId = jsonNode.get("places").get(0).get(0).asText().split("\\|");
    return poiId[0];
  }

  public void getDataFromPoiId(String poiId) throws IOException {
    HttpClient client = new DefaultHttpClient();
    HttpGet request = new HttpGet(String.format(resource.properties.getProperty("PROXIMAV2API"), poiId));
    HttpResponse response = client.execute(request);

    //get jsonNode out of http response
    JsonNode jsonNode = stringToJson(response);

    String brand = jsonNode.get("payload").get(0).get("name").asText();
    String category1 = jsonNode.get("payload").get(0).get("cat1").asText();
    String category2 = jsonNode.get("payload").get(0).get("cat2").asText();

    List<String> list=new ArrayList<>();
    list.add(brand);
    list.add(category1);
    list.add(category2);

    //Converting dataframe for HTTP response
    JavaSparkContext sparkContext = new JavaSparkContext();
    JavaRDD<Row> rowRDD = sparkContext.parallelize(list).map((String row) -> RowFactory.create(row));
    StructType schema = DataTypes.createStructType(
        new StructField[] {
            DataTypes.createStructField("brand", DataTypes.StringType, false),
            DataTypes.createStructField("category1", DataTypes.StringType, false),
            DataTypes.createStructField("category2", DataTypes.StringType, false)
        });

    Dataset<Row> df=resource.getSpark().sqlContext().createDataFrame(rowRDD,schema).toDF();

    df.write()
        .mode("overwrite")
        .format("csv")
        .save(resource.properties.getProperty("POIINFOPATH"));
  }

  //To convert the http response to json object
  public JsonNode stringToJson(HttpResponse response) throws IOException {
    String responseData = EntityUtils.toString(response.getEntity());
    ObjectMapper objectMapper = new ObjectMapper();
    return objectMapper.readTree(responseData);
  }
}
