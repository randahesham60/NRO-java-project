/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package wuzzuf;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.knowm.xchart.CategoryChart;
import org.knowm.xchart.CategoryChartBuilder;
import org.knowm.xchart.SwingWrapper;
import org.knowm.xchart.style.Styler.LegendPosition;

/**
 *
 * @author randahesham60
 */
public class TwoOperationsAndBarChart {
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
        
        // get most popular job titles with a sql query
        final Dataset<Row> most_pop_jobs = sparkSession.sql("SELECT cast(Title as string) Title, cast(COUNT(*) as int) MostPopularJobTitles FROM Wuzzuf_Jobs GROUP BY Title ORDER BY MostPopularJobTitles DESC");
        most_pop_jobs.show();
        
        // get most popular areas with a sql query
        final Dataset<Row> most_pop_locs = sparkSession.sql("SELECT cast(Location as string) Location, cast(COUNT(*) as int) MostPopularAreas FROM Wuzzuf_Jobs GROUP BY Location ORDER BY MostPopularAreas DESC");
        most_pop_locs.show();
        
        // register the output of the query as list        
        List<Row> listOfPopJobs = most_pop_jobs.takeAsList(5); //(int) most_pop_jobs.count()
        
        // store indices of the list in another two lists
        String[] titles = listOfPopJobs.stream().map(s -> s.get(0)).collect(Collectors.toList()).toArray(String[]::new);
        Integer[] titles_count = listOfPopJobs.stream().map(s -> s.get(1)).collect(Collectors.toList()).toArray(Integer[]::new);
        
        System.out.println(Arrays.asList(titles));
        System.out.println(Arrays.asList(titles_count));
        
        // register the output of the query as list   
        List<Row> listOfPopAreas = most_pop_locs.takeAsList(5); //(int) most_pop_locs.count()
        
        // store indices of the list in another two lists
        String[] areas = listOfPopAreas.stream().map(s -> s.get(0)).collect(Collectors.toList()).toArray(String[]::new);
        Integer[] areas_count = listOfPopAreas.stream().map(s -> s.get(1)).collect(Collectors.toList()).toArray(Integer[]::new);
    
  
        getBarChart("Most Popular Job Titles", "Job Title", "Number of Demand", titles, titles_count);
        
        getBarChart("Most Popular Areas", "Location", "Number of Demand", areas, areas_count);
      

    }
    
    
    public static void getBarChart(String title, String x_axis, String y_axis, String[] x_col, Integer[] y_col) {

        // Create Chart
        CategoryChart chart = new CategoryChartBuilder().width(800).height(600).title(title).xAxisTitle(x_axis).yAxisTitle(y_axis).build();

        // Customize Chart
        chart.getStyler().setLegendPosition(LegendPosition.InsideNW);
        chart.getStyler().setHasAnnotations(true);
        chart.getStyler().setStacked (true);

        // Series
        chart.addSeries(x_axis, Arrays.asList(x_col), Arrays.asList(y_col));

        // Show it
        new SwingWrapper (chart).displayChart ();

    }


}