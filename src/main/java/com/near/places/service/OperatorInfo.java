package com.near.places.service;

import com.near.places.utility.Resource;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class OperatorInfo {
  static final String QUERY="select devCarrier,count(distinct ifa) AS numberOfMobileDevices from placesData group by devCarrier order by numberOfMobileDevices desc";
  Resource resource=new Resource();

  public void mobileInfoForOperators(){
    Dataset<Row> dataFrame = resource.getDataFrame();

    long noOfOperators=dataFrame.select("devCarrier").distinct().count();
    System.out.printf("No of Operators:: %o%n",noOfOperators);

    // Register the DataFrame as a SQL temporary view
    dataFrame.createOrReplaceTempView("placesData");

    // number of mobile devices used under different operators
    Dataset<Row> data=resource.getSpark().sql(QUERY);

    data.write()
        .option("header","true")
        .option("sep",",")
        .mode("overwrite")
        .format("csv")
        .save(resource.properties.getProperty("OPERATORFILEPATH"));

    System.out.println("Data Saved successfully");
  }
}
