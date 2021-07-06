/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package wuzzuf;

import java.awt.Color;
import java.util.Arrays;
import java.util.List;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.knowm.xchart.PieChart;
import org.knowm.xchart.PieChartBuilder;
import org.knowm.xchart.SwingWrapper;

/**
 *
 * @author randahesham60
 */
public class charts {

    public static void main(String[] args) {
        // Create Spark Session to create connection to Spark
        final SparkSession sparkSession = SparkSession.builder ().appName ("Wuzzuf Session").master ("local[2]")
                .getOrCreate ();

        // Get DataFrameReader using SparkSession
        final DataFrameReader dataFrameReader = sparkSession.read ();
        // Set header option to true to specify that first row in file contains
        // name of columns
        dataFrameReader.option ("header", "true");
        final Dataset<Row> wuzzufDataFrame = dataFrameReader.csv ("src/main/resources/data/Wuzzuf_Jobs.csv");

        // Print Schema to see column names, types and other metadata
        wuzzufDataFrame.printSchema ();

        // Create view and execute query to convert types as, by default, all columns have string types
        wuzzufDataFrame.createOrReplaceTempView ("COMPANY_JOBSs");
        final Dataset<Row> companyJobsData = sparkSession
                .sql ("SELECT Company, COUNT(*) DemandedJobsNo FROM COMPANY_JOBSs GROUP BY Company ORDER BY DemandedJobsNo DESC");
        companyJobsData.show (20);

        // Create Chart
        PieChart chart = new PieChartBuilder ().width (1024).height (728).title ("Top Ten Demanding Companies For Jobs").build ();
        // Customize Chart
        Color[] sliceColors = new Color[]{
            new Color (111, 46, 67), 
            new Color (204, 77, 88), 
            new Color (243, 101, 35),
            new Color (245, 149, 29),
            new Color (249, 194, 50),
            new Color (111, 156, 51),
            new Color (43, 138, 134),
            new Color (73, 114, 136),
            new Color (22, 87, 141),
            new Color (95, 77, 153)};
        chart.getStyler ().setSeriesColors (sliceColors);
        
        //Demanded Jobs Number Column 
        List<Row> JobArray = companyJobsData.select("DemandedJobsNo").collectAsList();
        Long[] job_Num = new Long[JobArray.size()];
        for (int i = 0; i < JobArray.size(); i++)
            job_Num[i] = (Long) JobArray.get(i).get(0);
        int[] Num_values = Arrays.stream(job_Num).mapToInt(x -> x.intValue()).toArray();
        
        //Company Column
        List<Row> CompanyArray= companyJobsData.select("Company").collectAsList();
        String[] companies = new String[10];
        for (int i = 0; i < 10; i++)
            companies[i] = (String) CompanyArray.get(i).get(0);

        // Series
        for (int i = 0; i <10; i++)
        {
            chart.addSeries(companies[i], Num_values[i]);
        }
        // Show it
        new SwingWrapper(chart).displayChart ();
        
        
        
    }
}
        
        
        
        