package com.jhp.study.spark.stream.app.news;

import com.jhp.study.spark.constant.SparkJobConstant;
import kafka.serializer.StringDecoder;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.execution.columnar.LONG;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaPairInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka.KafkaUtils;
import scala.Tuple2;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by jack on 2017/10/8.
 */
public class NewsRealTimeSpark {

    static SparkConf sparkConf = new SparkConf().setMaster(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL_2).setAppName(SparkJobConstant.SPARK_JOB_STREAM_APP);
    static JavaStreamingContext jscc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

    /**
     * 根据不满足条件的日志
     * @param type
     * @return
     */
    private static JavaPairDStream<String,String> filterJavaPairDstream(final String type){

        //创建kafka参数
        Map<String, String> kafkaParams = new HashMap<String, String>();
        kafkaParams.put("metadata.broker.list","localhost:9092,localhost:9093,localhost:9094");

        // 然后，要创建一个set，里面放入，你要读取的topic
        // 这个，就是我们所说的，它自己给你做的很好，可以并行读取多个topic
        Set<String> topics = new HashSet<String>();
        topics.add("news_log");

        JavaPairInputDStream<String,String> javaPairInputDStream = KafkaUtils.createDirectStream(jscc,String.class,String.class, StringDecoder.class,StringDecoder.class,kafkaParams,topics);

        //过滤对应的日志类型
        JavaPairDStream<String,String>javaPairDStream= javaPairInputDStream.filter(new Function<Tuple2<String, String>, Boolean>() {
            @Override
            public Boolean call(Tuple2<String, String> tuple) throws Exception {
                String log = tuple._2;
                String [] logs = log.split(" ");
                String action = logs[5];

                if (type.equals("view")){
                    if("view".equals(action)) {
                        return true;
                    } else {
                        return false;
                    }
                }

                if (type.equals("register")){
                    if("register".equals(action)) {
                        return true;
                    } else {
                        return false;
                    }
                }
                return true;
            }
        });

        return javaPairDStream;
    }

    /**
     * 启动流式上下文对象
     * @param jscc
     */
    private static void startJscc(JavaStreamingContext jscc){
        jscc.start();
        jscc.awaitTermination();
        jscc.close();
    }

    /**
     * 实时pv
     */
    private static void pv(){
        JavaPairDStream<String,String> javaPairDStream = filterJavaPairDstream(SparkJobConstant.SPARK_LOG_TYPE_VIEW);

        JavaPairDStream<String,Long>mapPairDStream=javaPairDStream.mapToPair(new PairFunction<Tuple2<String,String>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, String> tuple) throws Exception {
                String []logs = tuple._2.split(" ");
                String pageid = logs[3];
                return new Tuple2<String, Long>(pageid,1l);
            }
        });

        JavaPairDStream<String,Long>resultPairDStream = mapPairDStream.reduceByKey(new Function2<Long, Long, Long>() {
            @Override
            public Long call(Long v1, Long v2) throws Exception {
                return v1 + v2;
            }
        });

        resultPairDStream.print();
        startJscc(jscc);
    }


    /**
     * 每个页面实时uv
     * p1 1001,1002,1003  p2 1001,1002,1005 p3 1001 1004 1002 xianzai
     */
    private static void pageuv(){
        JavaPairDStream<String,String> javaPairDStream = filterJavaPairDstream(SparkJobConstant.SPARK_LOG_TYPE_VIEW);

        JavaDStream<String>javadstream =  javaPairDStream.map(new Function<Tuple2<String,String>, String>() {
            @Override
            public String call(Tuple2<String, String> v1) throws Exception {
                String [] logSplited =v1._2.split(" ");
                Long pageid = Long.valueOf(logSplited[3]);
                Long userid = Long.valueOf("null".equalsIgnoreCase(logSplited[2]) ? "-1" : logSplited[2]);
                return pageid+"_" +userid;
            }
        });

        //去重
        JavaDStream<String>distictDstream = javadstream.transform(new Function<JavaRDD<String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaRDD<String> v1) throws Exception {
                return v1.distinct();
            }
        });
        //累加
        JavaPairDStream<Long,Long>pageUvDstream =distictDstream.mapToPair(new PairFunction<String, Long, Long>() {
            @Override
            public Tuple2<Long, Long> call(String s) throws Exception {
                String []logs = s.split("_");
                return new Tuple2<Long, Long>(Long.valueOf(logs[0]),1l);
            }
        });

        pageUvDstream.print();
        startJscc(jscc);
    }

    /**
     * 实时uv
     */

    private static void uv(){

    }

    /**
     * 实时用户注册数
     */
    private static void regUv(){
        JavaPairDStream<String,String> javaPairDStream = filterJavaPairDstream(SparkJobConstant.SPARK_LOG_TYPE_REG);
        JavaDStream<Long> javaDsteam = javaPairDStream.count();
        javaDsteam.print();
        startJscc(jscc);

    }

    /**
     * 实时用户跳出数
     */
    private static void breakUv(){

        JavaPairDStream<String,String> javaPairDStream = filterJavaPairDstream(SparkJobConstant.SPARK_LOG_TYPE_VIEW);

        JavaPairDStream<Long, Long> useridDStream = javaPairDStream.mapToPair(

                new PairFunction<Tuple2<String,String>, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, Long> call(Tuple2<String, String> tuple)
                            throws Exception {
                        String log = tuple._2;
                        String[] logSplited = log.split(" ");
                        Long userid = Long.valueOf("null".equalsIgnoreCase(logSplited[2]) ? "-1" : logSplited[2]);
                        return new Tuple2<Long, Long>(userid, 1L);
                    }

                });

        JavaPairDStream<Long, Long> useridCountDStream = useridDStream.reduceByKey(

                new Function2<Long, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }

                });
        //过滤跳出的用户
        JavaPairDStream<Long, Long> jumpUserDStream = useridCountDStream.filter(

                new Function<Tuple2<Long,Long>, Boolean>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Boolean call(Tuple2<Long, Long> tuple) throws Exception {
                        if(tuple._2 == 1) {
                            return true;
                        } else {
                            return false;
                        }
                    }

                });

        JavaDStream<Long> jumpUserCountDStream = jumpUserDStream.count();

        jumpUserCountDStream.print();

    }

    /**
     * 用户实时点击板块统计
     */
    private static void selection(){

        JavaPairDStream<String,String> javaPairDStream = filterJavaPairDstream(SparkJobConstant.SPARK_LOG_TYPE_VIEW);

        JavaPairDStream<String, Long> sectionDStream = javaPairDStream.mapToPair(

                new PairFunction<Tuple2<String,String>, String, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, Long> call(Tuple2<String, String> tuple)
                            throws Exception {
                        String log = tuple._2;
                        String[] logSplited = log.split(" ");

                        String section = logSplited[4];

                        return new Tuple2<String, Long>(section, 1L);
                    }

                });

        JavaPairDStream<String, Long> sectionPvDStream = sectionDStream.reduceByKey(

                new Function2<Long, Long, Long>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Long call(Long v1, Long v2) throws Exception {
                        return v1 + v2;
                    }

                });

        sectionPvDStream.print();
        startJscc(jscc);

    }

    public static void main(String[] args) {

    }
}
