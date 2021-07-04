package wuzzuf;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;

public class Main {
    public static void main(String[] args) {
        final  String wuzzufPath="src/main/resources/data/Wuzzuf_Jobs.csv";
        //create spark session
        final SparkSession sparkSession= SparkSession.builder ().appName ("Wuzzuf Session").master ("local[2]").getOrCreate ();
        //read csv file as dataset
        Dataset<Row> wuzzufDataFrame=sparkSession.read ().option ("header", "true").csv (wuzzufPath);
        //display first 20 rows
        wuzzufDataFrame.show(20);
        //display structure
        wuzzufDataFrame.printSchema();
        //display summary
        wuzzufDataFrame.describe().show();
        //clean data by drop duplicates and null rows
        wuzzufDataFrame=wuzzufDataFrame.dropDuplicates().na().drop();
        //replace 'null Yrs of Exp' in YearsExp column by '0 Yrs of Exp' for bounce task
        wuzzufDataFrame=wuzzufDataFrame.withColumn("YearsExp", functions.when(functions.col("YearsExp").equalTo("null Yrs of Exp"),"0 Yrs of Exp").otherwise(functions.col("YearsExp")));
        //display summary
        wuzzufDataFrame.describe().show();


    }

}
