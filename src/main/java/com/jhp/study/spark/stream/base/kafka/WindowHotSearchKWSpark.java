package com.jhp.study.spark.stream.base.kafka;

import com.jhp.study.spark.constant.SparkJobConstant;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.*;

/**
 * Created by jack on 2017/10/5.
 * 给予滑动窗口统计热门搜索关键词
 *
 */
public class WindowHotSearchKWSpark {
    /**
     * 数据格式 用户 关键字 ：贾红平 双截棍
     */
    private static void windowHotSearchWord(){
        SparkConf sparkConf = new SparkConf().setMaster(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL_2).setAppName(SparkJobConstant.SPARK_JOB_STREAM_APP);
        JavaStreamingContext jscc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        //创建kafka参数
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list","localhost:9092,localhost:9093,localhost:9094");

        // 然后，要创建一个set，里面放入，你要读取的topic
        // 这个，就是我们所说的，它自己给你做的很好，可以并行读取多个topic
        Set<String> topics = new HashSet<String>();
        topics.add("hot_kw_log");

        JavaPairInputDStream<String,String> javaPairInputDStream = KafkaUtils.createDirectStream(jscc,String.class,String.class, StringDecoder.class,StringDecoder.class,kafkaParams,topics);

        JavaPairDStream<String,Integer> javaPairDStream= javaPairInputDStream.mapToPair(new PairFunction<Tuple2<String,String>, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Tuple2<String, String> tuple) throws Exception {
                System.out.println("tuple:"+tuple);
                String log = tuple._2;
                String []logs = log.split(" ");
                return new Tuple2<String, Integer>(logs[1],1);
            }
        });

        //使用滑动窗口统计热搜词 等待我们的滑动间隔到了以后，10秒钟到了，会将之前60秒的RDD，因为一个batch间隔是，5秒
        // 60秒，就有12个RDD，给聚合起来，然后，统一执行redcueByKey操作
        JavaPairDStream<String,Integer> reducePairDStream=javaPairDStream.reduceByKeyAndWindow(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        },Durations.seconds(60),Durations.seconds(10));

        //转换一个格式 获取搜索top关键字
        JavaPairDStream<String,Integer> mapDstreamRDD = reducePairDStream.transformToPair(new Function<JavaPairRDD<String, Integer>, JavaPairRDD<String, Integer>>() {
            @Override
            public JavaPairRDD<String, Integer> call(JavaPairRDD<String, Integer> reducerdd) throws Exception {

                JavaPairRDD<Integer, String> mapRDD = reducerdd.mapToPair(new PairFunction<Tuple2<String,Integer>, Integer, String>() {
                    @Override
                    public Tuple2<Integer, String> call(Tuple2<String, Integer> tuple) throws Exception {
                        return new Tuple2<Integer, String>(tuple._2,tuple._1);
                    }
                });
                //排序
                JavaPairRDD<Integer,String> sortRDD = mapRDD.sortByKey(false);
               //重新转换 关键字 频率 这种格式
               JavaPairRDD<String,Integer> resultrdd= sortRDD.mapToPair(new PairFunction<Tuple2<Integer,String>, String, Integer>() {
                    @Override
                    public Tuple2<String, Integer> call(Tuple2<Integer, String> tuple) throws Exception {
                        return new Tuple2<String, Integer>(tuple._2,tuple._1);
                    }
                });
                // 然后用take()，获取排名前3的热点搜索词
                List<Tuple2<String, Integer>> hogSearchWordCounts = resultrdd.take(3);
                for(Tuple2<String, Integer> wordCount : hogSearchWordCounts) {
                    System.out.println(wordCount._1 + ": " + wordCount._2);
                }
                //最终要返回出去
                return reducerdd;
            }
        });
        mapDstreamRDD.print();
        jscc.start();
        jscc.awaitTermination();
        jscc.close();
    }

    public static void main(String[] args) {
        windowHotSearchWord();
    }
}
