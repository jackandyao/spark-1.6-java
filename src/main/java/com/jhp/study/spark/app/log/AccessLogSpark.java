package com.jhp.study.spark.app.log;

import com.jhp.study.spark.app.log.sort.AccessLogSortKey;
import com.jhp.study.spark.constant.SparkJobConstant;
import com.jhp.study.spark.util.SparkContextUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.storage.StorageLevel;
import scala.Tuple2;

import java.util.List;

/**
 * Created by jack on 2017/10/2.
 */
public class AccessLogSpark {

    private static void accessLog(){
        String textFile ="/soft/spark-study-java-1.6x/src/main/resources/access.log";
        JavaSparkContext javaSparkContext = SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_APP);
        JavaRDD<String> textRDD = javaSparkContext.textFile(textFile);
        textRDD.persist(StorageLevel.MEMORY_ONLY());
        JavaPairRDD<String,AccessLogInfo> mapAccessLogRDD = mapAccessLogToPair(textRDD);
        JavaPairRDD<String,AccessLogInfo> reduceRDD = reduceByDeviceIdRDD(mapAccessLogRDD);
        JavaPairRDD<AccessLogSortKey,String> sortRDD = secondSortRDD(reduceRDD);
        // 执行二次排序操作，按照上行流量、下行流量以及时间戳进行倒序排序
        JavaPairRDD<AccessLogSortKey ,String> sortedAccessLogRDD = sortRDD.sortByKey(false);
        // 获取top10数据
        List<Tuple2<AccessLogSortKey, String>> top10DataList = sortedAccessLogRDD.take(10);
        for(Tuple2<AccessLogSortKey, String> data : top10DataList) {
            System.out.println(data._2 + ": " + data._1);
        }
        javaSparkContext.close();
    }

    // 数据基本封装
    private static JavaPairRDD<String,AccessLogInfo> mapAccessLogToPair(JavaRDD<String> textRdd) {

        JavaPairRDD<String,AccessLogInfo> accessLogRDD = textRdd.mapToPair(new PairFunction<String, String, AccessLogInfo>() {
            @Override
            public Tuple2<String, AccessLogInfo> call(String accessLog) throws Exception {
                // 根据\t对日志进行切分
                String[] accessLogSplited = accessLog.split("\t");
                // 获取四个字段
                long timestamp = Long.valueOf(accessLogSplited[0]);
                String deviceID = accessLogSplited[1];
                long upTraffic = Long.valueOf(accessLogSplited[2]);
                long downTraffic = Long.valueOf(accessLogSplited[3]);
                //组装成map类型
                AccessLogInfo accessLogInfo = new AccessLogInfo(timestamp,upTraffic,downTraffic);
                return new Tuple2<String, AccessLogInfo>(deviceID,accessLogInfo);
            }
        });
        return accessLogRDD;
    }

    //封装数据求和
    private static JavaPairRDD<String,AccessLogInfo> reduceByDeviceIdRDD (JavaPairRDD<String,AccessLogInfo> mapRDD){
        JavaPairRDD<String,AccessLogInfo> reduceByRDD=mapRDD.reduceByKey(new Function2<AccessLogInfo, AccessLogInfo, AccessLogInfo>() {
            @Override
            public AccessLogInfo call(AccessLogInfo accessLogInfo1, AccessLogInfo accessLogInfo2) throws Exception {
                //求和
                long timestamp = accessLogInfo1.getTimestamp() < accessLogInfo2.getTimestamp() ?
                        accessLogInfo1.getTimestamp() : accessLogInfo2.getTimestamp();
                long upTraffic = accessLogInfo1.getUpTraffic() + accessLogInfo2.getUpTraffic();
                long downTraffic = accessLogInfo1.getDownTraffic() + accessLogInfo2.getDownTraffic();
                //组装
                AccessLogInfo accessLogInfo = new AccessLogInfo();
                accessLogInfo.setTimestamp(timestamp);
                accessLogInfo.setUpTraffic(upTraffic);
                accessLogInfo.setDownTraffic(downTraffic);
                return accessLogInfo;
            }
        });
        return reduceByRDD;
    }


    //求和数据二次排序
    private static JavaPairRDD<AccessLogSortKey,String> secondSortRDD(JavaPairRDD<String,AccessLogInfo> reduceByRDD){
        JavaPairRDD<AccessLogSortKey,String> sortRDD = reduceByRDD.mapToPair(new PairFunction<Tuple2<String,AccessLogInfo>, AccessLogSortKey, String>() {
            @Override
            public Tuple2<AccessLogSortKey, String> call(Tuple2<String, AccessLogInfo> tuple) throws Exception {
                // 获取tuple数据
                String deviceID = tuple._1;
                AccessLogInfo accessLogInfo = tuple._2;

                // 将日志信息封装为二次排序key
                AccessLogSortKey accessLogSortKey = new AccessLogSortKey(
                        accessLogInfo.getUpTraffic(),
                        accessLogInfo.getDownTraffic(),
                        accessLogInfo.getTimestamp());
                //组装二次排序
                return new Tuple2<AccessLogSortKey, String>(accessLogSortKey,deviceID);
            }
        });
        return sortRDD;
    }

    public static void main(String[] args) {
        accessLog();
    }
}
