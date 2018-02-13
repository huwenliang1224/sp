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

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class JavaSQLDataSourceExample2 {

    public static class Square implements Serializable {
        private int value;
        private int square;

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public int getSquare() {
            return square;
        }

        public void setSquare(int square) {
            this.square = square;
        }
    }

    public static class Cube implements Serializable {
        private int value;
        private int cube;

        public int getValue() {
            return value;
        }

        public void setValue(int value) {
            this.value = value;
        }

        public int getCube() {
            return cube;
        }

        public void setCube(int cube) {
            this.cube = cube;
        }
    }

    public static void main(String[] args) throws IOException {
        SparkSession spark = SparkSession
                .builder()
                .appName("Java Spark SQL data sources example2")
                .config("spark.some.config.option", "some-value")
                .getOrCreate();

        List<Square> squares = new ArrayList<>();
        for (int value = 1; value <= 5; value++) {
            Square square = new Square();
            square.setValue(value);
            square.setSquare(value * value);
            squares.add(square);
        }

// Create a simple DataFrame, store into a partition directory
        Dataset<Row> squaresDF = spark.createDataFrame(squares, Square.class);
        squaresDF.write().parquet("data/test_table/key=1");

        List<Cube> cubes = new ArrayList<>();
        for (int value = 6; value <= 10; value++) {
            Cube cube = new Cube();
            cube.setValue(value);
            cube.setCube(value * value * value);
            cubes.add(cube);
        }

// Create another DataFrame in a new partition directory,
// adding a new column and dropping an existing column
        Dataset<Row> cubesDF = spark.createDataFrame(cubes, Cube.class);
        cubesDF.write().parquet("data/test_table/key=2");

// Read the partitioned table
        Dataset<Row> mergedDF = spark.read().option("mergeSchema", true).parquet("data/test_table");
        mergedDF.printSchema();
        mergedDF.show();

        spark.stop();
    }
}
