package testSpark;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;

public class test3 {
    public static void main(String[] args){
        SparkConf conf = new SparkConf().setAppName("test spark").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);

//        List<Integer> data = Arrays.asList(1, 2, 3, 4, 5);
//        JavaRDD<Integer> distData = sc.parallelize(data);
//       System.out.println(distData.reduce(Integer::sum));
///////////////////////////////////////////////////////
//        JavaRDD<String> lines = sc.textFile("src/main/resources/data/data.txt");
//        JavaRDD<Integer> lineLengths = lines.map(String::length);
//        int totalLength = lineLengths.reduce(Integer::sum);
//        System.out.println(totalLength);
////////////////////////////////////////////////////////
//        JavaRDD<String> lines = sc.textFile("src/main/resources/data/data.txt");
//        JavaPairRDD<String, Integer> pairs = lines.mapToPair(s -> new Tuple2(s, 1));
//        JavaPairRDD<String, Integer> counts = pairs.reduceByKey(Integer::sum);
//
//        System.out.println(counts.sortByKey().collect());


        JavaRDD<String> lines = sc.textFile("src/main/resources/data/Wuzzuf_Jobs.csv");

        JavaRDD<Integer> lineLengths = lines.map(String::length);


        System.out.println(lines.count());


    }
}
