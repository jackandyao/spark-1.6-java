package com.jhp.study.spark.sql.app.student;

import com.jhp.study.spark.constant.SparkJobConstant;
import com.jhp.study.spark.util.SparkContextUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jack on 2017/10/2.
 * 模拟查询满足条件的学生基本信息
 * 数据分为学生信息表和学生分数表最后通过链接查询获取所有的信息表
 */
public class StudentInfoSQLSpark {
    public static void main(String[] args) {
        JavaSparkContext javaSparkContext = SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_APP);


        SQLContext sqlContext = new SQLContext(javaSparkContext);

        String studentJsonFile = "/soft/spark-study-java-1.6x/src/main/resources/student.json";

        String scoreJsonFile ="/soft/spark-study-java-1.6x/src/main/resources/score.json";

        DataFrame scoreFrame = sqlContext.read().json(scoreJsonFile);

        DataFrame studentFrame = sqlContext.read().json(studentJsonFile);

        scoreFrame.registerTempTable("score_info");
        String scoresql ="select name,score from score_info where score>=80";

        DataFrame scoresqlDf =sqlContext.sql(scoresql);
        scoresqlDf.show();
        //用集合保存满足查询条件的学生
        JavaRDD<Row> scoreRow = scoresqlDf.javaRDD();

        List<String>studentNames = scoreRow.map(new Function<Row, String>() {
            @Override
            public String call(Row v1) throws Exception {
                return v1.getString(0);
            }
        }).collect();

        //注册学生基本信息表
        studentFrame.registerTempTable("student_info");

        String studentsql = "select name,age from student_info where name in (";
        for(int i = 0; i < studentNames.size(); i++) {
            studentsql += "'" + studentNames.get(i) + "'";
            if(i < studentNames.size() - 1) {
                studentsql += ",";
            }
        }
        studentsql += ")";

        DataFrame studentDf = sqlContext.sql(studentsql);

//        scoresqlDf.join(studentDf).javaRDD();

        JavaPairRDD<String,Integer> studentMapRDD = studentDf.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(0),Integer.valueOf(String.valueOf(row.getLong(1))));
            }
        });

        JavaPairRDD<String,Integer> scoreMapRDD = scoresqlDf.javaRDD().mapToPair(new PairFunction<Row, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(Row row) throws Exception {
                return new Tuple2<String, Integer>(row.getString(0),Integer.valueOf(String.valueOf(row.getLong(1))));
            }
        });

        JavaPairRDD<String,Tuple2<Integer,Integer>>pairRDD = studentMapRDD.join(scoreMapRDD);

        JavaRDD<Row> rowRDD = pairRDD.map(new Function<Tuple2<String,Tuple2<Integer,Integer>>, Row>() {
            @Override
            public Row call(Tuple2<String, Tuple2<Integer, Integer>> tuple) throws Exception {
                return RowFactory.create(tuple._1,tuple._2._1,tuple._2._2);
            }
        });
        //创建数据表的元数据
        List<StructField> structFields = new ArrayList<StructField>();
        structFields.add(DataTypes.createStructField("name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("age",DataTypes.IntegerType,true));
        structFields.add(DataTypes.createStructField("score",DataTypes.IntegerType,true));

        //
        StructType structType = DataTypes.createStructType(structFields);

        DataFrame student_score_df = sqlContext.createDataFrame(rowRDD,structType);
        student_score_df.write().format("json").save("/soft/spark-study-java-1.6x/src/main/resources/student_score_info.json");

        javaSparkContext.close();
    }
}
