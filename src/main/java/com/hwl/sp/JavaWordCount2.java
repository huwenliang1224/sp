package com.hwl.sp;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.Arrays;
import java.util.List;
import java.util.regex.Pattern;

public class JavaWordCount2 {
    private static final Pattern SPACE = Pattern.compile(" ");

    public static void main(String[] args) throws Exception {

        SparkSession spark = SparkSession.builder().appName("test").getOrCreate();

        JavaRDD<String> rdd = spark.read().textFile("wc.txt").javaRDD();

        JavaRDD<String> rdd2 = rdd.flatMap(line -> Arrays.asList(SPACE.split(line)).iterator());

        JavaPairRDD<String, Integer> rdd3 = rdd2.mapToPair(line -> new Tuple2<>(line, 1));

        JavaPairRDD<String, Integer> rdd4 = rdd3.reduceByKey((i, j) -> i + j);

        List<Tuple2<String, Integer>> list = rdd4.collect();

        for (Tuple2 tuple2 : list) {
            System.out.println(tuple2._1 + " " + tuple2._2);
        }

        spark.stop();
    }
}
