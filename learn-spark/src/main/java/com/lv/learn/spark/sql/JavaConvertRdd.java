package com.lv.learn.spark.sql;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.*;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * RDD转换为Dataset的两种方式
 * <pre>
 *     - Inferring the Schema Using Reflection（通过反射自动转换）
 *     - Programmatically Specifying the Schema（以编程的方式指定 rdd格式）
 * </pre>
 *
 * @author lvsicheng
 * @date 2019-05-01 20:57
 */
public class JavaConvertRdd implements Serializable {

    /**
     * Inferring the Schema Using Reflection（通过反射自动转换）
     *
     * @param spark spark session
     */
    public static void runInferSchemaExample(SparkSession spark) {
        // 1.Create an RDD of Person objects from a text file
        Dataset<String> fileDS = spark.read().textFile(JavaConvertRdd.class.getResource("/") + "people.txt");
        JavaRDD<String> stringJavaRDD = fileDS.javaRDD();
        JavaRDD<JavaSparkSQLExample.Person> personRDD = stringJavaRDD.map(line -> {
            String[] split = line.split(",");
            JavaSparkSQLExample.Person person = new JavaSparkSQLExample.Person();
            person.setName(split[0]);
            person.setAge(Integer.valueOf(split[1].trim()));
            return person;
        });

        // 2.Apply a schema to an RDD of JavaBeans to get a DataFrame
        Dataset<Row> personDF = spark.createDataFrame(personRDD, JavaSparkSQLExample.Person.class);

        // Register the DataFrame as a temporary view
        personDF.createOrReplaceTempView("person");

        // SQL statements can be run by using the sql methods provided by spark
        Dataset<Row> teensDF = spark.sql("select * from person where age between 13 and 19");
        teensDF.printSchema();
        teensDF.show();

        // The columns of a row in the result can be accessed by field index
        Dataset<String> teenagerNamesByIndexDF = teensDF.map((MapFunction<Row, String>) row -> "Name: " + row.getString(1), Encoders.STRING());
        teenagerNamesByIndexDF.show();

        // or by field name
        Dataset<String> teenagerNamesByFieldDF = teensDF.map((MapFunction<Row, String>) row -> "Name: " + row.getAs("name"), Encoders.STRING());
        teenagerNamesByFieldDF.show();

    }

    /**
     * Programmatically Specifying the Schema（以编程的方式指定 rdd格式）
     * <p>
     * a Dataset<Row> can be created programmatically with three steps.
     * <p>
     * <pre>
     *      1.Create an RDD of Rows from the original RDD;
     *      2.Create the schema represented by a StructType matching the structure of Rows in the RDD created in Step 1.
     *      3.Apply the schema to the RDD of Rows via createDataFrame method provided by SparkSession.
     * </pre>
     *
     * @param spark spark session
     */
    public static void runProgramSchemaExample(SparkSession spark) {
        // Create an RDD
        RDD<String> stringRDD = spark.sparkContext().textFile(JavaConvertRdd.class.getResource("/") + "people.txt", 1);
        JavaRDD<String> peopleRDD = stringRDD.toJavaRDD();

        // The schema is encoded in a string
        String schemaStr = "name age";

        // Generate the schema based on the string of schema
        List<StructField> fields = new ArrayList<>();
        for (String fieldName : schemaStr.split(" ")) {
            StructField field = DataTypes.createStructField(fieldName, DataTypes.StringType, true);
            fields.add(field);
        }
        StructType schema = DataTypes.createStructType(fields);

        // Convert records of the RDD (people) to Rows
        JavaRDD<Row> rowRDD = peopleRDD.map((Function<String, Row>) record -> {
            String[] attrs = record.split(",");
            return RowFactory.create(attrs[0], attrs[1].trim());
        });

        // Apply the schema to the RDD
        Dataset<Row> peopleDF = spark.createDataFrame(rowRDD, schema);
        peopleDF.createOrReplaceTempView("people");

        Dataset<Row> result = spark.sql("select * from people");
        Dataset<String> recordDS = result.map((MapFunction<Row, String>) row -> "Name:" + row.getAs("name") + ",Age:" + row.getAs("age"), Encoders.STRING());
        recordDS.show();
    }

    public static void main(String[] args) {
        // 创建 SparkSession
        SparkSession spark = SparkSession
                .builder()
                .master("local[3]")
                .appName("Convert RDD TO Dataset Example")
                .config("spark.sql.warehouse.dir", "G:/BaiduNetdiskDownload/learn/learn-bigdata/spark-warehouse")
                .getOrCreate();

//        runInferSchemaExample(spark);

        runProgramSchemaExample(spark);

    }

}
