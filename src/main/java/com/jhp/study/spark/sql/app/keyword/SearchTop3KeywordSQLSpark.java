package com.jhp.study.spark.sql.app.keyword;

import com.jhp.study.spark.constant.SparkJobConstant;
import com.jhp.study.spark.util.SparkContextUtil;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.broadcast.Broadcast;
import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.hive.HiveContext;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import scala.Tuple2;

import java.util.*;

/**
 * Created by jack on 2017/10/3.
 * 查询每日搜索最高的几个词 按照搜索uv
 */
public class SearchTop3KeywordSQLSpark {

    private static void top3KeyWord(){
        JavaSparkContext javaSparkContext = SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_APP);
        SQLContext sqlContext = new SQLContext(javaSparkContext);
        HiveContext hiveContext = new HiveContext(javaSparkContext.sc());

        //构造查询条件
        Map<String, List<String>> queryParamMap = new HashMap<String, List<String>>();
        queryParamMap.put("city", Arrays.asList("上海"));
        queryParamMap.put("platform", Arrays.asList("android"));
        queryParamMap.put("version", Arrays.asList("1.0","2.0"));

        //查询条件通过广播进行共享 优化网络传输性能
        final Broadcast<Map<String, List<String>>> broadcastParam = javaSparkContext.broadcast(queryParamMap);

        String searchLog ="/soft/spark-study-java-1.6x/src/main/resources/search_keyword.log";
        JavaRDD<String> javaRDD = javaSparkContext.textFile(searchLog);

        //过滤满足条件的RDD
        JavaRDD<String>filterRDD = javaRDD.filter(new Function<String, Boolean>() {
            @Override
            public Boolean call(String log) throws Exception {
                // 切割原始日志，获取城市、平台和版本
                String[] logSplited = log.split("\t");
                String city = logSplited[3];
                String platform = logSplited[4];
                String version = logSplited[5];

                // 与查询条件进行比对，任何一个条件，只要该条件设置了，且日志中的数据没有满足条件
                // 则直接返回false，过滤该日志
                // 否则，如果所有设置的条件，都有日志中的数据，则返回true，保留日志
                Map<String, List<String>> queryParamMap = broadcastParam.value();

                List<String> cities = queryParamMap.get("city");
                if(cities.size() > 0 && !cities.contains(city)) {
                    return false;
                }

                List<String> platforms = queryParamMap.get("platform");
                if(platforms.size() > 0 && !platforms.contains(platform)) {
                    return false;
                }

                List<String> versions = queryParamMap.get("version");
                if(versions.size() > 0 && !versions.contains(version)) {
                    return false;
                }

                return true;
            }
        });

        // 过滤出来的原始日志，映射为(日期_搜索词, 用户)的格式
//        2017-09-02_双截棍:lyn
//        2017-09-02_皮鞋:liaijun
//        2017-09-01_袜子:liaijun
//        2017-09-02_皮鞋:wangping
//        2017-09-01_双截棍:lyn
//        2017-09-02_双截棍:liaijun
//        2017-09-02_皮鞋:szh
//        2017-09-01_皮鞋:yaojie
//        2017-09-02_袜子:yaojie
//        2017-09-01_皮鞋:wangping        J
 JavaPairRDD<String, String> dateKeywordUserRDD = filterRDD.mapToPair(

                new PairFunction<String, String, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, String> call(String log) throws Exception {
                        String[] logSplited = log.split("\t");
                        String date = logSplited[0];
                        String user = logSplited[1];
                        String keyword = logSplited[2];
                        return new Tuple2<String, String>(date + "_" + keyword, user);
                    }

                });
        //打印信息
        System.out.println("=============映射为(日期_搜索词, 用户)的格式==================");

        dateKeywordUserRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> tuple) throws Exception {
                System.out.println(tuple._1 +":" +tuple._2);
            }
        });
        //进行分组
//        2017-09-02_双截棍:lyn
//        2017-09-02_双截棍:liaijun
//        2017-09-02_双截棍:lyn
//        2017-09-02_双截棍:szh
//        2017-09-02_双截棍:liaijun
//        2017-09-02_双截棍:zhangjun
//        2017-09-02_双截棍:liaijun
//        2017-09-02_双截棍:szh
        JavaPairRDD<String,Iterable<String>> groupRDD = dateKeywordUserRDD.groupByKey();
        groupRDD.foreach(new VoidFunction<Tuple2<String, Iterable<String>>>() {
            @Override
            public void call(Tuple2<String, Iterable<String>> tuple) throws Exception {
                String date_kw =tuple._1;
                Iterator<String> iterator= tuple._2.iterator();
                while (iterator.hasNext()){
                    System.out.println(date_kw+":" +iterator.next().toString());
                }
            }
        });

        //对每个搜索关键字的用户进行去重
//        2017-09-01_钱包:6
//        2017-09-02_双截棍:7
//        2017-09-01_高跟鞋:7
//        2017-09-02_钱包:7
//        2017-09-02_高跟鞋:8
//        2017-09-02_机器学习:6
//        2017-09-01_蕾丝裙:8
//        2017-09-01_机器学习:6
//        2017-09-02_吊带裙:8
//        2017-09-01_皮鞋:7
//        2017-09-01_连衣裙:6
//        2017-09-01_大数据:7
        JavaPairRDD<String,Long> distictRDD = groupRDD.mapToPair(new PairFunction<Tuple2<String,Iterable<String>>, String, Long>() {
            @Override
            public Tuple2<String, Long> call(Tuple2<String, Iterable<String>> tuple) throws Exception {
                String date_kw = tuple._1;
                Iterator<String> iterator = tuple._2.iterator();
                List<String> distictUsers=  new ArrayList<String>();
                while (iterator.hasNext()){
                    String username = iterator.next().toString();
                    if(!distictUsers.contains(username)) {
                        distictUsers.add(username);
                    }
                }
                long uv = distictUsers.size();
                return new Tuple2<String, Long>(date_kw,uv);
            }
        });

        System.out.println("===============搜索关键字进行用户去重============");
        distictRDD.foreach(new VoidFunction<Tuple2<String, Long>>() {
            @Override
            public void call(Tuple2<String, Long> tuple) throws Exception {
                System.out.println(tuple._1+":" +tuple._2);
            }
        });
        // 将每天每个搜索词的uv数据，转换成DataFrame
//        +----------+-------+---+
//                |      date|keyword| uv|
//                +----------+-------+---+
//                |2017-09-01|     钱包|  6|
//        |2017-09-02|    双截棍|  7|
//        |2017-09-01|    高跟鞋|  7|
//        |2017-09-02|     钱包|  7|
//        |2017-09-02|    高跟鞋|  8|
//        |2017-09-02|   机器学习|  6|
//        |2017-09-01|    蕾丝裙|  8|
//        |2017-09-01|   机器学习|  6|
//        |2017-09-02|    吊带裙|  8|
//        |2017-09-01|     皮鞋|  7|
//        |2017-09-01|    连衣裙|  6|
        JavaRDD<Row> rowRDD = distictRDD.map(new Function<Tuple2<String,Long>, Row>() {
            @Override
            public Row call(Tuple2<String, Long> dateKeywordUv) throws Exception {
                String date = dateKeywordUv._1.split("_")[0];
                String keyword = dateKeywordUv._1.split("_")[1];
                long uv = dateKeywordUv._2;
                return RowFactory.create(date,keyword,uv);
            }
        });
        // 打印
        rowRDD.foreach(new VoidFunction<Row>() {
            @Override
            public void call(Row row) throws Exception {
                System.out.println(row);
            }
        });

        List<StructField> structFields = Arrays.asList(
                DataTypes.createStructField("date", DataTypes.StringType, true),
                DataTypes.createStructField("keyword", DataTypes.StringType, true),
                DataTypes.createStructField("uv", DataTypes.LongType, true));


        StructType structType = DataTypes.createStructType(structFields);

        DataFrame dataFrame = sqlContext.createDataFrame(rowRDD,structType);
//        dataFrame.show();
        dataFrame.registerTempTable("daily_keyword_uv");
        // 这里的查询语句是需要放到hive上执行 所以这里实际也是应该使用hivecontext
        //这里还使用了开窗函数
        DataFrame dailyTop3KeywordDF = sqlContext.sql(""
                + "SELECT date,keyword,uv "
                + "FROM ("
                + "SELECT "
                + "date,"
                + "keyword,"
                + "uv,"
                + "row_number() OVER (PARTITION BY date ORDER BY uv DESC) rank "
                + "FROM daily_keyword_uv"
                + ") tmp "
                + "WHERE rank<=3");

//        dailyTop3KeywordDF.show();

        // 将DataFrame转换为RDD，然后映射，计算出每天的top3搜索词的搜索uv总数
        JavaRDD<Row> dailyTop3KeywordRDD = dailyTop3KeywordDF.javaRDD();
        // 2017-09-02,双截棍_3
        JavaPairRDD<String, String> top3DateKeywordUvRDD = dailyTop3KeywordRDD.mapToPair(
                new PairFunction<Row, String, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<String, String> call(Row row)
                            throws Exception {
                        String date = String.valueOf(row.get(0));
                        String keyword = String.valueOf(row.get(1));
                        Long uv = Long.valueOf(String.valueOf(row.get(2)));
                        return new Tuple2<String, String>(date, keyword + "_" + uv);
                    }

                });

        JavaPairRDD<String, Iterable<String>> top3DateKeywordsRDD = top3DateKeywordUvRDD.groupByKey();
        //搜索关键词 uv 求和
        JavaPairRDD<Long, String> uvDateKeywordsRDD = top3DateKeywordsRDD.mapToPair(
                new PairFunction<Tuple2<String,Iterable<String>>, Long, String>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Tuple2<Long, String> call(
                            Tuple2<String, Iterable<String>> tuple)
                            throws Exception {
                        String date = tuple._1;

                        Long totalUv = 0L;
                        String dateKeywords = date;

                        Iterator<String> keywordUvIterator = tuple._2.iterator();
                        while(keywordUvIterator.hasNext()) {
                            String keywordUv = keywordUvIterator.next();

                            Long uv = Long.valueOf(keywordUv.split("_")[1]);
                            totalUv += uv;

                            dateKeywords += "," + keywordUv;
                        }

                        return new Tuple2<Long, String>(totalUv, dateKeywords);
                    }

                });


        // 按照每天的总搜索uv进行倒序排序
        JavaPairRDD<Long, String> sortedUvDateKeywordsRDD = uvDateKeywordsRDD.sortByKey(false);

        // 再次进行映射，将排序后的数据，映射回原始的格式，Iterable<Row>
        JavaRDD<Row> sortedRowRDD = sortedUvDateKeywordsRDD.flatMap(

                new FlatMapFunction<Tuple2<Long,String>, Row>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterable<Row> call(Tuple2<Long, String> tuple)
                            throws Exception {
                        String dateKeywords = tuple._2;
                        String[] dateKeywordsSplited = dateKeywords.split(",");

                        String date = dateKeywordsSplited[0];

                        List<Row> rows = new ArrayList<Row>();
                        rows.add(RowFactory.create(date,
                                dateKeywordsSplited[1].split("_")[0],
                                Long.valueOf(dateKeywordsSplited[1].split("_")[1])));
                        rows.add(RowFactory.create(date,
                                dateKeywordsSplited[2].split("_")[0],
                                Long.valueOf(dateKeywordsSplited[2].split("_")[1])));
                        rows.add(RowFactory.create(date,
                                dateKeywordsSplited[3].split("_")[0],
                                Long.valueOf(dateKeywordsSplited[3].split("_")[1])));

                        return rows;
                    }

                });

        // 将最终的数据，转换为DataFrame，并保存到Hive表中
        DataFrame finalDF = sqlContext.createDataFrame(sortedRowRDD, structType);

        finalDF.saveAsTable("daily_top3_keyword_uv");



        javaSparkContext.close();


    }

    public static void main(String[] args) {
        top3KeyWord();
    }
}
