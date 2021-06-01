package com.near.places.utility;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import java.io.FileReader;
import java.io.Serializable;
import java.util.Properties;

public class Resource implements Serializable {
  private static Dataset<Row> dataFrame;
  private static SparkSession spark;
  public Properties properties=null;

  public Resource() {
 try{
     properties=new Properties();
     properties.load(new FileReader(StringConst.PROPERTIES_FILE_PATH));
    }catch(Exception e){
      System.out.println(e.getMessage());
    }
  }

  public  SparkSession getSpark() {
    return spark;
  }

  public  void setSpark(SparkSession spark) {
    Resource.spark = spark;
  }

  public Dataset<Row> getDataFrame() {
    return dataFrame;
  }

  public void setDataFrame(Dataset<Row> dataFrame) {
    Resource.dataFrame = dataFrame;
  }

  public void readParquetFile(){
    SparkSession spark = SparkSession
        .builder()
        .master(properties.getProperty(StringConst.HOST))
        .appName(StringConst.NEAR_PLACES)
        .getOrCreate();

    Dataset<Row> df = spark.read().parquet(properties.getProperty(StringConst.FILEPATH));
    this.setDataFrame(df);
    this.setSpark(spark);
  }
}
