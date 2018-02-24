package com.hwl.sp;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

public class Test {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("Test")
                .getOrCreate();

        List<Tuple2<Integer, Integer>> data = new ArrayList<>(4);
        data.add(new Tuple2<>(1, 2));
        data.add(new Tuple2<>(1, 3));
        data.add(new Tuple2<>(3, 2));
        data.add(new Tuple2<>(2, 4));

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        JavaPairRDD<Integer, Integer> rdd1 = jsc.parallelizePairs(data);
        JavaPairRDD<Integer, Integer> rdd2 = rdd1.mapToPair(e -> new Tuple2<>(e._2(), e._1()));

//        rdd1.join(rdd2).foreachPartition((VoidFunction<Iterator<Tuple2<Integer, Tuple2<Integer, Integer>>>>) tuple2Iterator -> {
//            while (tuple2Iterator.hasNext()) {
//                Tuple2<Integer, Tuple2<Integer, Integer>> line = tuple2Iterator.next();
//                System.out.println(line._1() + "(" + line._2()._1() + "," + line._2()._2() + ")");
//            }
//        });

        List<Tuple2<Integer, Tuple2<Integer, Integer>>> result = rdd1.join(rdd2).collect();
        for (Tuple2<Integer, Tuple2<Integer, Integer>> line : result) {
            System.out.println(line._1() + "(" + line._2()._1() + "," + line._2()._2() + ")");
        }

        spark.stop();
    }
}
