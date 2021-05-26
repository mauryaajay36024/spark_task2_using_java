package com.near.places.utility;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

public class Resource {
  private static Dataset dataFrame;
  private static SparkSession spark;

  public Resource() {
  }

  public  SparkSession getSpark() {
    return spark;
  }

  public  void setSpark(SparkSession spark) {
    Resource.spark = spark;
  }

  public Dataset getDataFrame() {
    return dataFrame;
  }

  public void setDataFrame(Dataset dataFrame) {
    Resource.dataFrame = dataFrame;
  }

  public void readParquetFile(){
    SparkSession spark = SparkSession
        .builder()
        .master("local[*]")
        .appName("Near places")
        .getOrCreate();

    Dataset<Row> df = spark.read().parquet("/Users/ajaymaurya/Downloads/spark/places.parquet");
    this.setDataFrame(df);
    this.setSpark(spark);
  }
}
