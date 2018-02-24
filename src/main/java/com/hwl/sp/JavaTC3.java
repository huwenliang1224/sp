package com.hwl.sp;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.Optional;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.SparkSession;
import scala.Tuple2;

import java.util.*;

public class JavaTC3 {

    public static void main(String[] args) {
        SparkSession spark = SparkSession
                .builder()
                .appName("JavaTC3")
                .getOrCreate();

        List<Tuple2<Integer, Integer>> data = new ArrayList<>(4);
        data.add(new Tuple2<>(1, 2));
        data.add(new Tuple2<>(1, 3));
        data.add(new Tuple2<>(3, 2));
        data.add(new Tuple2<>(2, 4));

        JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());

        JavaPairRDD<Integer, Integer> rdd1 = jsc.parallelizePairs(data);
        JavaPairRDD<Integer, Integer> rdd2 = rdd1.mapToPair(e -> new Tuple2<>(e._2(), e._1()));

//        JavaPairRDD<Integer, Tuple2<Integer, Integer>> after_join = rdd1.join(rdd2);
        JavaPairRDD<Integer, Tuple2<Integer, Integer>> after_join = rdd1.join(rdd2);

        //1
        after_join.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Integer, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Integer, Integer>> line) throws Exception {
                System.out.println(line._1() + "(" + line._2()._1() + "," + line._2()._2() + ")");
            }
        });

        //2
//        List<Tuple2<Integer, Tuple2<Integer,  Optional<Integer>>>> result = after_join.collect();
//        for (Tuple2<Integer, Tuple2<Integer,  Optional<Integer>>> line : result) {
//            System.out.println(line._1() + "(" + line._2()._1() + "," + line._2()._2().isPresent() + ")");
//        }

        spark.stop();
    }
}
