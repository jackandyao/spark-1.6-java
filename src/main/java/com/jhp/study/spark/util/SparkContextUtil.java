package com.jhp.study.spark.util;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;

/**
 * Created by jack on 2017/9/30.
 * spark 上下文对象获取
 */
public class SparkContextUtil {

    static JavaSparkContext sparkContext = null;

    public static JavaSparkContext getSparkContextUtil(String clusterType,String appName){
        if (sparkContext == null){
            sparkContext = new JavaSparkContext(getSparkConf(clusterType,appName));
        }
        return sparkContext;
    }

    private static SparkConf getSparkConf(String clusterType,String appName){
        SparkConf sparkConf =new SparkConf();
        if (clusterType =="local"){
            sparkConf.setAppName(appName).setMaster("local");
        }
        else{
            sparkConf.setAppName(appName);
        }
        return sparkConf;
    }
}
