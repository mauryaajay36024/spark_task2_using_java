package com.near.places.service;

import com.near.places.utility.Resource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class OperatorInfo {
  Resource resource=new Resource();
  public void mobileInfoForOperators(){

    Dataset dataFrame = resource.getDataFrame();

    long noOfOperators=dataFrame.select("devCarrier").distinct().count();
    System.out.println("Number of Operators :"+noOfOperators);

    // Register the DataFrame as a SQL temporary view
    dataFrame.createOrReplaceTempView("placesData");

    // number of mobile devices used under different operators
    Dataset<Row> data=resource.getSpark().sql("select devCarrier,count(distinct ifa) AS numberOfMobileDevices from placesData group by devCarrier order by numberOfMobileDevices desc");

    data.show();

    data.write()
        .option("header","true")
        .option("sep",",")
        .mode("overwrite")
        .format("csv")
        .save("/Users/ajaymaurya/Downloads/spark/sparkTaskJava1/");

    System.out.println("Data Saved successfully");
  }
}
