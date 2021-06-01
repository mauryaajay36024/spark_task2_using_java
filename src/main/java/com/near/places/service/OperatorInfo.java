package com.near.places.service;

import com.near.places.utility.Resource;
import com.near.places.utility.StringConst;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;

public class OperatorInfo {
  Resource resource=new Resource();

  public void mobileInfoForOperators(){
    Dataset<Row> dataFrame = resource.getDataFrame();

    long noOfOperators=dataFrame.select(StringConst.DEV_CARRIER).distinct().count();
    System.out.printf("No of Operators:: %o",noOfOperators);

    // Register the DataFrame as a SQL temporary view
    dataFrame.createOrReplaceTempView(StringConst.PLACES_DATA);

    // number of mobile devices used under different operators
    Dataset<Row> data=resource.getSpark().sql(StringConst.QUERY);
    data.write()
        .option(StringConst.HEADER,StringConst.TRUE)
        .option(StringConst.SEP,",")
        .mode(StringConst.OVERWRITE)
        .format(StringConst.PARQUET)
        .save(resource.properties.getProperty(StringConst.OPERATORFILEPATH));
  }
}
