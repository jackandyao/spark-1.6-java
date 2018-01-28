package com.jhp.study.spark.app.word;

import com.jhp.study.spark.constant.SparkJobConstant;
import com.jhp.study.spark.util.SparkContextUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.Arrays;

/**
 * Created by jack on 2017/10/2.
 */
public class WordCountSpark {

    private static void wordCount(){
        String textPath ="/soft/spark-study-java-1.6x/src/main/resources/word.txt";
        JavaSparkContext javaSparkContext = SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_APP);
        JavaRDD<String> textRDD = javaSparkContext.textFile(textPath);
        textRDD.persist(StorageLevel.MEMORY_ONLY());

        JavaRDD<String> flatMapRDD = textRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                return Arrays.asList(s.split(" "));
            }
        });

        JavaPairRDD<String,Integer> pairRDD = flatMapRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String word) throws Exception {
                return new Tuple2<String, Integer>(word,1);
            }
        });
        // (word 1) (hello 3) (in 2)
        JavaPairRDD<String,Integer>reduceByKeyRDD = pairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });

        // (1 word) (3 hello) (2 in)
        JavaPairRDD<Integer,String> secondPairRDD = reduceByKeyRDD.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {
            @Override
            public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
                return new Tuple2<Integer, String>(tuple._2,tuple._1);
            }
        });


        //(3 hello) (2 in) (1 word)
        JavaPairRDD<Integer,String>sortByKeyRDD =secondPairRDD.sortByKey(false);
        JavaPairRDD<String,Integer>resultRDD = sortByKeyRDD.mapToPair(new PairFunction<Tuple2<Integer, String>, String, Integer>() {
            @Override
            public Tuple2<String,Integer> call(Tuple2<Integer, String> tuple2) throws Exception {
                return new Tuple2<String, Integer>(tuple2._2,tuple2._1);
            }
        });

        //(hello 3) (in 2) (word 1)
        System.out.println("===排完顺序的结果========");
        resultRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                System.out.println(tuple._1 +":" +tuple._2);
            }
        });
        javaSparkContext.close();
    }

    public static void main(String[] args) {
        wordCount();
    }
}
