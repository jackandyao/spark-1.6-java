package com.jhp.study.spark.stream.base.socket;

import com.google.common.base.Optional;
import com.jhp.study.spark.constant.SparkJobConstant;
import com.jhp.study.spark.util.SparkContextUtil;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jack on 2017/10/5.
 * 过滤广告点击中存在的黑名单 就是对于一个广告点击的批处理数据 需要把你里面的黑名单给过滤掉
 *  nc -l -p 8888 127.0.0.1
 *
 */
public class AdvBlackListFilterSpark {

    /**
     * 过滤广告点击中的黑名单 为简单 规定广告日志 形如 日期 用户：20151011 jhp
     * 需要创建一个黑名单
     */
    private static void filterAdvBlackList(){
        //如果这里是在本地测试的话 需要把local[n] 这个值设置大于1的数据 否则无法接受到数据
        SparkConf sparkConf = new SparkConf().setMaster(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL_2).setAppName(SparkJobConstant.SPARK_JOB_STREAM_APP);
        JavaStreamingContext jscc = new JavaStreamingContext(sparkConf, Durations.seconds(5));

        //创建黑名单
        List<Tuple2<String,Boolean>> blackList = new ArrayList<Tuple2<String, Boolean>>();
        blackList.add(new Tuple2<String, Boolean>("tom",true));
        blackList.add(new Tuple2<String, Boolean>("jack",true));
        final JavaPairRDD<String,Boolean>blackRDD = jscc.sparkContext().parallelizePairs(blackList);

        //这里使用socket模拟数据发送
        JavaReceiverInputDStream<String> javaReceiverInputDStream = jscc.socketTextStream("127.0.0.1",8888);
        System.out.println("============接受输入流数据==========");
        javaReceiverInputDStream.print();
        //转换数据格式 (username, date username)
        JavaPairDStream<String,String>javaPairDStream =javaReceiverInputDStream.mapToPair(new PairFunction<String, String, String>() {
            @Override
            public Tuple2<String, String> call(String adsClickLog) throws Exception {
                return new Tuple2<String, String>(
                        adsClickLog.split(" ")[1], adsClickLog);
            }
        });

        //可以使用强大的转换函数transform
        JavaDStream<String> whiteClickLogDStream=javaPairDStream.transform(new Function<JavaPairRDD<String, String>, JavaRDD<String>>() {
            @Override
            public JavaRDD<String> call(JavaPairRDD<String, String> userAdsClickLogRDD) throws Exception {
                //这里不能使用全链接 因为并不是所有的用户都存在黒名单中 使用左外链接
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> joinedRDD = userAdsClickLogRDD.leftOuterJoin(blackRDD);
                //进行黑名单过滤
                JavaPairRDD<String, Tuple2<String, Optional<Boolean>>> filteredRDD = joinedRDD.filter(new Function<Tuple2<String, Tuple2<String, Optional<Boolean>>>, Boolean>() {
                    @Override
                    public Boolean call(Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple) throws Exception {
                        if (tuple._2._2().isPresent() && tuple._2._2.get()){
                            return false;
                        }
                        return true;
                    }
                });
                //转换我们想要的数据格式
                JavaRDD<String> validAdsClickLogRDD = filteredRDD.map(

                        new Function<Tuple2<String,Tuple2<String,Optional<Boolean>>>, String>() {

                            private static final long serialVersionUID = 1L;

                            @Override
                            public String call(
                                    Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple)
                                    throws Exception {
                                return tuple._2._1;
                            }

                        });
                return validAdsClickLogRDD;
            }
        });
        System.out.println("========生成白名单===========");
        //为了触发job
        whiteClickLogDStream.print();
        jscc.start();
        jscc.awaitTermination();
        jscc.close();
    }

    public static void main(String[] args) {
        filterAdvBlackList();
        Tuple2<String, Tuple2<String, Optional<Boolean>>> tuple =null;
    }
}
