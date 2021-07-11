/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package wuzzuf;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

/**
 *
 * @author randahesham60
 */
public class Skills {
        public static void main(String[] args) {
            
        
        // Create Spark Session to create connection to Spark
        final SparkSession sparkSession = SparkSession.builder ().appName ("Wuzzuf Session").master ("local[2]").getOrCreate ();

        // Get DataFrameReader using SparkSession
        final DataFrameReader dataFrameReader = sparkSession.read ();
        // Set header option to true to specify that first row in file contains
        // name of columns
        dataFrameReader.option ("header", "true");
        final Dataset<Row> wuzzufDataFrame = dataFrameReader.csv ("src/main/resources/data/Wuzzuf_Jobs.csv");

        // Print Schema to see column names, types and other metadata
        wuzzufDataFrame.printSchema ();   
    
        // register dataframe as temporary view
        wuzzufDataFrame.createOrReplaceTempView("Wuzzuf_Jobs");
        // get most popular skills with a sql query
        final Dataset<Row> most_pop_skills = sparkSession.sql("SELECT cast(Skills as string) Skills FROM Wuzzuf_Jobs");
        most_pop_skills.show();
        
        
        // convert dataset to javaRDD
        JavaRDD<String> most_pop_skills_column = most_pop_skills.toJavaRDD().map(f -> f.toString());

        // map RDD, split strings, handle all
        JavaRDD<String> all_rows_skills = most_pop_skills_column.flatMap (skill -> Arrays.asList (skill
                .toLowerCase()
                .trim ()
                .replaceAll ("\\[", "").replaceAll("\\]", "").replaceAll ("\\<", "")
                .split (", ")).iterator ());
        
        // add skill and count in a Map
        Map<String, Long> skill_count = all_rows_skills.countByValue();
        
        // sort in descending order
        List<Map.Entry> popular_sorted_skills = skill_count.entrySet ().stream ()
                .sorted(Collections.reverseOrder(Map.Entry.comparingByValue())).collect (Collectors.toList ());
        
        System.out.println("#######################################################################");
        
        // print skill one by one
        for (Map.Entry<String, Long> entry : popular_sorted_skills) {
            System.out.println (entry.getKey () + " : " + entry.getValue ());
        }
    }
}
