package com.jhp.study.spark.sql.base;

import com.jhp.study.spark.constant.SparkJobConstant;
import com.jhp.study.spark.util.SparkContextUtil;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;

/**
 * Created by jack on 2017/10/2.
 */
public class DataFrameFunction {

    private static void testDataFrame(){
        JavaSparkContext javaSparkContext = SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_FUNCTION_APP);
        SQLContext sqlContext = new SQLContext(javaSparkContext);
        String jsonFilePath ="/soft/spark-study-java-1.6x/src/main/resources/person.json";
        DataFrame dataFrame = sqlContext.read().json(jsonFilePath);
        JavaRDD<String> rowJavaRDD = dataFrame.javaRDD().map(new Function<Row, String>() {
            @Override
            public String call(Row row) throws Exception {
                return row.get(2).toString();
            }
        });

        for(String s:rowJavaRDD.collect()){
            System.out.println(s);
        }

        //显示原始数据
        dataFrame.show();
        //显示数据结构
        dataFrame.printSchema();
        //根据名称查询
        dataFrame.select("name").show();
        //根据多个列查询
        dataFrame.select(dataFrame.col("id"),dataFrame.col("age")).show();
        //根据指定的条件进行过滤
        dataFrame.filter(dataFrame.col("age").gt(20)).show();
        //根据指定的列进行聚合
        dataFrame.groupBy(dataFrame.col("age")).count().show();
    }

    public static void main(String[] args) {
        testDataFrame();
    }
}
