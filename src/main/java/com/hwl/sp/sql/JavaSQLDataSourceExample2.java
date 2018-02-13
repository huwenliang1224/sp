/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hwl.sp.sql;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;

import java.io.*;
import java.util.stream.Collectors;

import static org.apache.spark.sql.functions.collect_list;

public class JavaSQLDataSourceExample2 {

    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL data sources example2")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        Dataset<Row> df = spark.read().json("test2.json");

        Dataset<Row> result = df.groupBy("class").agg(collect_list("name"));

        BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("/root/project/sp/result.txt"), "utf-8"));

        result.javaRDD().collect().forEach(row -> {
            try {
                bw.write(row.getString(0) + " " + row.<String>getList(1).stream().collect(Collectors.joining(",")));
            } catch (IOException e) {
            }
        });

        bw.close();
        spark.stop();
    }
}
