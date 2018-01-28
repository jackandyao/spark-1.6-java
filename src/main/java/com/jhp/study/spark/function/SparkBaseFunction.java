package com.jhp.study.spark.function;

import com.jhp.study.spark.constant.SparkJobConstant;
import com.jhp.study.spark.util.SparkContextUtil;
import org.apache.spark.Accumulator;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;

import java.util.*;

/**
 * Created by jack on 2017/9/30.
 * spark 常用函数API
 * accumulator,reduce,
 */
public class SparkBaseFunction {

    static JavaSparkContext javaSparkContext = null;

    //测试累加器
    private static void testAccumulator(){
        javaSparkContext =SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_FUNCTION_APP);
        System.out.println(javaSparkContext);
        final Accumulator<Integer> accumulator = javaSparkContext.accumulator(0);
        List<Integer> list = Arrays.asList(1,2,3,4,5,7,8,9);
        JavaRDD<Integer> integerJavaRDD = javaSparkContext.parallelize(list);

        integerJavaRDD.foreach(new VoidFunction<Integer>() {
            private static final long serialVersionUID = 1L;
            @Override
            public void call(Integer integer) throws Exception {
                accumulator.add(integer);
            }
        });
        System.out.println(accumulator.value());
        javaSparkContext.close();
    }

    // 测试元素求和
    private static void testReduce(){
        javaSparkContext = SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_FUNCTION_APP);
        List<Integer> list = Arrays.asList(1,3,5,7,9);
        JavaRDD<Integer>javaRDD = javaSparkContext.parallelize(list);
        int result = javaRDD.reduce(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer n1, Integer n2) throws Exception {
                return n1 + n2;
            }
        });
        System.out.println(result);
        javaSparkContext.close();
    };

    //测试把数据从远程拉到本地计算 不适合数据量大的数据
    private static void testCollect(){
        javaSparkContext = SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_FUNCTION_APP);
        List<Integer> list = Arrays.asList(1,3,5,7,9);
        JavaRDD<Integer>javaRDD = javaSparkContext.parallelize(list);
        javaSparkContext.parallelize(list);
        javaRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer num) throws Exception {
                return num *2;
            }
        });

        List<Integer> collectRdd = javaRDD.collect();
        for(Integer n:collectRdd){
            System.out.println(n);
        }
        javaSparkContext.close();
    }

    private static void testCount(){
        javaSparkContext = SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_FUNCTION_APP);
        List<Integer> list = Arrays.asList(1,2,3,4);
        JavaRDD<Integer>integerJavaRDD = javaSparkContext.parallelize(list);
        long count =integerJavaRDD.count();
        System.out.println(count);
        javaSparkContext.close();
    }

    private static void testTake(){
        javaSparkContext= SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_FUNCTION_APP);
        List<Integer> list = Arrays.asList(11,2,4,8,9,5,0,7);
        JavaRDD<Integer>integerJavaRDD = javaSparkContext.parallelize(list);
        List<Integer>topList = integerJavaRDD.take(4);
        for (Integer i : topList){
            System.out.println(i);
        }
        javaSparkContext.close();
    }

    private static void testSaveText(){
        javaSparkContext= SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_FUNCTION_APP);
        List<Integer> list = Arrays.asList(11,2,4,8,9,5,0,7);
        JavaRDD<Integer>integerJavaRDD = javaSparkContext.parallelize(list);
        JavaRDD<Integer>mapJavaRDD = integerJavaRDD.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1*2;
            }
        });

        mapJavaRDD.saveAsTextFile("/soft/spark-study-java-1.6x/src/main/resources/save.txt");
        javaSparkContext.close();
    }

    private static void testMap(){
        javaSparkContext = SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_FUNCTION_APP);
        List<Integer> list = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> intRdd = javaSparkContext.parallelize(list);
        JavaRDD<Integer> mapRdd = intRdd.map(new Function<Integer, Integer>() {
            @Override
            public Integer call(Integer v1) throws Exception {
                return v1 * 5;
            }
        });

        mapRdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println(integer);
            }
        });
        javaSparkContext.close();
    }


    private static void testFlatMap(){
        javaSparkContext = SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_FUNCTION_APP);
        List<String> stringList = Arrays.asList("hello word","hello jhp","hello abc","abc word");
        JavaRDD<String> stringJavaRDD = javaSparkContext.parallelize(stringList);
        JavaRDD<String> flatMapJavaRDD=stringJavaRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                System.out.println("s:"+s);
                return Arrays.asList(s.split(" "));
            }
        });

        flatMapJavaRDD.foreach(new VoidFunction<String>() {
            @Override
            public void call(String s) throws Exception {
                System.out.println(s);
            }
        });

        javaSparkContext.close();
    }

    private static void testFilter(){
        javaSparkContext= SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_FUNCTION_APP);
        List<Integer> list = Arrays.asList(1,3,5,7,6,8,10);
        JavaRDD<Integer> integerJavaRDD = javaSparkContext.parallelize(list);
        JavaRDD<Integer> filterRdd = integerJavaRDD.filter(new Function<Integer, Boolean>() {
            @Override
            public Boolean call(Integer v1) throws Exception {
                return v1%2==0;
            }
        });

        filterRdd.foreach(new VoidFunction<Integer>() {
            @Override
            public void call(Integer integer) throws Exception {
                System.out.println("filter result:"+integer);
            }
        });
        javaSparkContext.close();
    }
    //生产环境中 尤其当数据量比较大的时候 一般不能直接使用这个API 因为这个API很消耗性能 需要通过一些转换手段 来优化
    //比如 广播共享 做预聚合
    private static void testJoin(){
        javaSparkContext = SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_FUNCTION_APP);
        List<Tuple2<Integer,String>> studentList = Arrays.asList(new Tuple2<Integer, String>(1,"jhp"),new Tuple2<Integer, String>(2,"lyn")
        ,new Tuple2<Integer, String>(3,"yy"));

        List<Tuple2<Integer,Integer>> scoreList = Arrays.asList(new Tuple2<Integer, Integer>(1,100),new Tuple2<Integer, Integer>(2,85)
                ,new Tuple2<Integer, Integer>(3,90));

        JavaPairRDD<Integer,String> studentPairRDD = javaSparkContext.parallelizePairs(studentList);
        JavaPairRDD<Integer,Integer> scorePairRDD = javaSparkContext.parallelizePairs(scoreList);
        JavaPairRDD<Integer,Tuple2<String,Integer>>joinRDD = studentPairRDD.join(scorePairRDD);
        joinRDD.foreach(new VoidFunction<Tuple2<Integer, Tuple2<String, Integer>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<String, Integer>> tuple) throws Exception {
                System.out.println("student id:"+tuple._1);
                System.out.println("student name:"+tuple._2._1);
                System.out.println("student score:"+tuple._2._2);
            }
        });
        javaSparkContext.close();
    }


    private static void testReduceByKey(){
        javaSparkContext = SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_FUNCTION_APP);
        List<Tuple2<String,Integer>> clssList = Arrays.asList(new Tuple2<String, Integer>("jhp",99),
                new Tuple2<String, Integer>("jhp",100),new Tuple2<String, Integer>("lyn",88),new Tuple2<String, Integer>("lyn",92)
        ,new Tuple2<String, Integer>("yy",95));

        JavaPairRDD<String,Integer> javaPairRDD =javaSparkContext.parallelizePairs(clssList);
        JavaPairRDD<String,Integer> reduceByKeyRDD =javaPairRDD.reduceByKey(new Function2<Integer, Integer, Integer>() {
            @Override
            public Integer call(Integer v1, Integer v2) throws Exception {
                return v1 + v2;
            }
        });
        reduceByKeyRDD.foreach(new VoidFunction<Tuple2<String, Integer>>() {
            @Override
            public void call(Tuple2<String, Integer> tuple) throws Exception {
                System.out.println(tuple._1+":" + tuple._2);
            }
        });
        javaSparkContext.close();
    }

    private static void testCountByKey(){
        javaSparkContext = SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_FUNCTION_APP);
        // 模拟集合
        List<Tuple2<String, String>> scoreList = Arrays.asList(
                new Tuple2<String, String>("class1", "leo"),
                new Tuple2<String, String>("class2", "jack"),
                new Tuple2<String, String>("class1", "marry"),
                new Tuple2<String, String>("class2", "tom"),
                new Tuple2<String, String>("class2", "david"));

        // 并行化集合，创建JavaPairRDD
        JavaPairRDD<String, String> studentsPairRdd = javaSparkContext.parallelizePairs(scoreList);
        Map<String,Object> map = studentsPairRdd.countByKey();
        for (Map.Entry<String,Object> studentCount:map.entrySet()){
            System.out.println(studentCount.getKey() + ":" + studentCount.getValue());
        }
        javaSparkContext.close();
    }

    private static void testGroupByKey(){
        javaSparkContext = SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_FUNCTION_APP);
        // 模拟集合
        List<Tuple2<String, Integer>> scoreList = Arrays.asList(
                new Tuple2<String, Integer>("class1", 80),
                new Tuple2<String, Integer>("class2", 75),
                new Tuple2<String, Integer>("class1", 90),
                new Tuple2<String, Integer>("class2", 65));
        JavaPairRDD<String,Integer> javaPairRDD = javaSparkContext.parallelizePairs(scoreList);
        JavaPairRDD<String ,Iterable<Integer>> groupRdd = javaPairRDD.groupByKey();
        groupRdd.foreach(new VoidFunction<Tuple2<String, Iterable<Integer>>>() {
            @Override
            public void call(Tuple2<String, Iterable<Integer>> tuple) throws Exception {
                System.out.println("class: "+tuple._1);
                Iterator<Integer> iter = tuple._2.iterator();
                while (iter.hasNext()){
                    System.out.println(iter.next());
                }
                System.out.println("==================================");
            }

        });
        javaSparkContext.close();
    }

    private static void testSortByKey(){
        javaSparkContext = SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_FUNCTION_APP);
        // 模拟集合
        List<Tuple2<Integer, String>> scoreList = Arrays.asList(
                new Tuple2<Integer, String>(65, "leo"),
                new Tuple2<Integer, String>(50, "tom"),
                new Tuple2<Integer, String>(100, "marry"),
                new Tuple2<Integer, String>(80, "jack"));

        JavaPairRDD<Integer,String> javaPairRDD =javaSparkContext.parallelizePairs(scoreList);
        JavaPairRDD<Integer,String> sortRdd =javaPairRDD.sortByKey();
        sortRdd.foreach(new VoidFunction<Tuple2<Integer, String>>() {
            @Override
            public void call(Tuple2<Integer, String> tuple) throws Exception {
                System.out.println(tuple._1 +":" +tuple._2);
            }
        });
        javaSparkContext.close();
    }



    private static void testCogroup(){
        javaSparkContext = SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_FUNCTION_APP);
        List<Tuple2<Integer, String>> studentList = Arrays.asList(
                new Tuple2<Integer, String>(1, "leo"),
                new Tuple2<Integer, String>(2, "jack"),
                new Tuple2<Integer, String>(3, "tom"));

        List<Tuple2<Integer, Integer>> scoreList = Arrays.asList(
                new Tuple2<Integer, Integer>(1, 100),
                new Tuple2<Integer, Integer>(2, 90),
                new Tuple2<Integer, Integer>(3, 60),
                new Tuple2<Integer, Integer>(1, 70),
                new Tuple2<Integer, Integer>(2, 80),
                new Tuple2<Integer, Integer>(3, 50));
        JavaPairRDD<Integer,String> studentPairRdd = javaSparkContext.parallelizePairs(studentList);
        JavaPairRDD<Integer,Integer> scorePairRdd = javaSparkContext.parallelizePairs(scoreList);

        JavaPairRDD<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> studentScores =  studentPairRdd.cogroup(scorePairRdd);
        studentScores.foreach(new VoidFunction<Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>>>() {
            @Override
            public void call(Tuple2<Integer, Tuple2<Iterable<String>, Iterable<Integer>>> t) throws Exception {
                System.out.println("student id: " + t._1);
                System.out.println("student name: " + t._2._1);
                System.out.println("student score: " + t._2._2);
                System.out.println("===============================");
            }
        });
        javaSparkContext.close();
    }

    // 三个参数 第一个参数 是每个key的初始化值 第二个参数 如何进行shuffle map-side的本地聚合 第三个参数 Combiner Function，如何进行shuffle reduce-side的全局聚合
    private static void testAggregateByKey(){
        javaSparkContext = SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_FUNCTION_APP);
        List<String> stringList = Arrays.asList("hello word","hello jhp","hello abc","abc word");
        JavaRDD<String> stringJavaRDD = javaSparkContext.parallelize(stringList);
        JavaRDD<String> flatMapJavaRDD=stringJavaRDD.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public Iterable<String> call(String s) throws Exception {
                System.out.println("s:"+s);
                return Arrays.asList(s.split(" "));
            }
        });

        JavaPairRDD<String,Integer> javaPairRDD = flatMapJavaRDD.mapToPair(new PairFunction<String, String, Integer>() {
            @Override
            public Tuple2<String, Integer> call(String s) throws Exception {
                return new Tuple2<String, Integer>(s,1);
            }
        });

        JavaPairRDD<String, Integer> wordCounts = javaPairRDD.aggregateByKey(
                0,

                new Function2<Integer, Integer, Integer>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Integer call(Integer v1, Integer v2)
                            throws Exception {
                        return v1 + v2;
                    }

                },

                new Function2<Integer, Integer, Integer>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Integer call(Integer v1, Integer v2)
                            throws Exception {
                        return v1 + v2;
                    }

                });
        List<Tuple2<String, Integer>> wordCountList = wordCounts.collect();
        for(Tuple2<String, Integer> wordCount : wordCountList) {
            System.out.println(wordCount);
        }
        javaSparkContext.close();
    }

    private static void testCartesian(){
        javaSparkContext = SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_FUNCTION_APP);
        List<String> clothes = Arrays.asList("夹克", "T恤", "皮衣", "风衣");
        JavaRDD<String> clothesRDD = javaSparkContext.parallelize(clothes);

        List<String> trousers = Arrays.asList("皮裤", "运动裤", "牛仔裤", "休闲裤");
        JavaRDD<String> trousersRDD = javaSparkContext.parallelize(trousers);
        JavaPairRDD<String,String> javaPairRDD = clothesRDD.cartesian(trousersRDD);
        javaPairRDD.foreach(new VoidFunction<Tuple2<String, String>>() {
            @Override
            public void call(Tuple2<String, String> t) throws Exception {
                System.out.println(t._1 +":" +t._2);
            }
        });
        javaSparkContext.close();
    }
    // coalesce算子，功能是将RDD的partition缩减，减少
    // 将一定量的数据，压缩到更少的partition中去
    // 建议的使用场景，配合filter算子使用
    // 使用filter算子过滤掉很多数据以后，比如30%的数据，出现了很多partition中的数据不均匀的情况
    // 此时建议使用coalesce算子，压缩rdd的partition数量
    // 从而让各个partition中的数据都更加的紧凑
    private static void testCoalesce(){
        javaSparkContext = SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_FUNCTION_APP);
        List<String> staffList = Arrays.asList("张三", "李四", "王二", "麻子",
                "赵六", "王五", "李大个", "王大妞", "小明", "小倩");
        JavaRDD<String> staffRDD = javaSparkContext.parallelize(staffList, 6);
        JavaRDD<String> staffRDD2 = staffRDD.mapPartitionsWithIndex(
                new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
                List<String> list = new ArrayList<String>();
                while(iterator.hasNext()) {
                    String staff = iterator.next();
                    list.add("部门[" + (index + 1) + "], " + staff);
                }
                return list.iterator();
            }
        },true);

        List<String> list = staffRDD2.collect();
        System.out.println("===========没有压缩RDD=======");
        for (String  str :list){
            System.out.println(str);
        }

        System.out.println("===========压缩RDD=======");
        JavaRDD<String> coalescRDD = staffRDD2.coalesce(3);
        JavaRDD<String> coalescResultRDD = coalescRDD.mapPartitionsWithIndex(new Function2<Integer, Iterator<String>, Iterator<String>>() {
            @Override
            public Iterator<String> call(Integer index, Iterator<String> iterator) throws Exception {
                List<String> list = new ArrayList<String>();
                while(iterator.hasNext()) {
                    String staff = iterator.next();
                    list.add("部门[" + (index + 1) + "], " + staff);
                }
                return list.iterator();
            }
        },true);

        for (String str :coalescResultRDD.collect()){
            System.out.println(str);
        }
        javaSparkContext.close();
    }

    private static void testDistinct(){
        javaSparkContext = SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_FUNCTION_APP);

        List<String> accessLogs = Arrays.asList(
                "user1 2016-01-01 23:58:42",
                "user1 2016-01-01 23:58:43",
                "user1 2016-01-01 23:58:44",
                "user2 2016-01-01 12:58:42",
                "user2 2016-01-01 12:58:46",
                "user3 2016-01-01 12:58:42",
                "user4 2016-01-01 12:58:42",
                "user5 2016-01-01 12:58:42",
                "user6 2016-01-01 12:58:42",
                "user6 2016-01-01 12:58:45");
        JavaRDD<String> accessLogsRDD = javaSparkContext.parallelize(accessLogs);

        JavaRDD<String> mapRDD = accessLogsRDD.map(new Function<String, String>() {
            @Override
            public String call(String v1) throws Exception {
                System.out.println("map:" +v1);
                return v1.split(" ")[0];
            }
        });

        JavaRDD<String> distinctRDD = mapRDD.distinct();
        int uv = distinctRDD.collect().size();
        System.out.println("uv:"+uv);
        javaSparkContext.close();
    }

    private static void testIntersection(){
        javaSparkContext = SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_FUNCTION_APP);
        List<String> project1MemberList = Arrays.asList("张三", "李四", "王二", "麻子");
        JavaRDD<String> project1MemberRDD = javaSparkContext.parallelize(project1MemberList);

        List<String> project2MemberList = Arrays.asList("张三", "王五", "李四", "小倩");
        JavaRDD<String> project2MemberRDD = javaSparkContext.parallelize(project2MemberList);

        JavaRDD<String> projectIntersectionRDD = project1MemberRDD.intersection(project2MemberRDD);

        for(String member : projectIntersectionRDD.collect()) {
            System.out.println(member);
        }

        javaSparkContext.close();
    }

    private static void testMapPartitions(){
        javaSparkContext = SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_FUNCTION_APP);
        // 准备一下模拟数据
        List<String> studentNames = Arrays.asList("张三", "李四", "王二", "麻子");
        JavaRDD<String> studentNamesRDD = javaSparkContext.parallelize(studentNames, 2);

        final Map<String, Double> studentScoreMap = new HashMap<String, Double>();
        studentScoreMap.put("张三", 278.5);
        studentScoreMap.put("李四", 290.0);
        studentScoreMap.put("王二", 301.0);
        studentScoreMap.put("麻子", 205.0);

        // mapPartitions
        // 类似map，不同之处在于，map算子，一次就处理一个partition中的一条数据
        // mapPartitions算子，一次处理一个partition中所有的数据

        // 推荐的使用场景
        // 如果你的RDD的数据量不是特别大，那么建议采用mapPartitions算子替代map算子，可以加快处理速度
        // 但是如果你的RDD的数据量特别大，比如说10亿，不建议用mapPartitions，可能会内存溢出
        JavaRDD<Double> flatRDD = studentNamesRDD.mapPartitions(new FlatMapFunction<Iterator<String>, Double>() {
            @Override
            public Iterable<Double> call(Iterator<String> iterator) throws Exception {
                // 因为算子一次处理一个partition的所有数据
                // call函数接收的参数，是iterator类型，代表了partition中所有数据的迭代器
                // 返回的是一个iterable类型，代表了返回多条记录，通常使用List类型

                List<Double> studentScoreList = new ArrayList<Double>();

                while(iterator.hasNext()) {
                    String studentName = iterator.next();
                    Double studentScore = studentScoreMap.get(studentName);
                    studentScoreList.add(studentScore);
                }

                return studentScoreList;
            }
        });

        for(Double studentScore: flatRDD.collect()) {
            System.out.println(studentScore);
        }

        javaSparkContext.close();
    }

    private static void testMapPartitionsWithIndex(){
        javaSparkContext =SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_FUNCTION_APP);
        List<String> studentNames = Arrays.asList("张三", "李四", "王二", "麻子");
        JavaRDD<String> studentNamesRDD = javaSparkContext.parallelize(studentNames, 2);

        // 这里，parallelize并行集合的时候，指定了numPartitions是2
        // 也就是说，四个同学，会被分成2个班
        // 但是spark自己判定怎么分班

        // 如果你要分班的话，就必须拿到班级号
        // mapPartitionsWithIndex这个算子来做，这个算子可以拿到每个partition的index
        // 也就可以作为我们的班级号

        JavaRDD<String> studentWithClassRDD = studentNamesRDD.mapPartitionsWithIndex(

                new Function2<Integer, Iterator<String>, Iterator<String>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterator<String> call(Integer index, Iterator<String> iterator)
                            throws Exception {
                        List<String> studentWithClassList = new ArrayList<String>();

                        while(iterator.hasNext()) {
                            String studentName = iterator.next();
                            String studentWithClass = studentName + "_" + (index + 1);
                            studentWithClassList.add(studentWithClass);
                        }

                        return studentWithClassList.iterator();
                    }

                }, true);

        for(String studentWithClass : studentWithClassRDD.collect()) {
            System.out.println(studentWithClass);
        }
        javaSparkContext.close();
    }

    // repartition算子，用于任意将rdd的partition增多，或者减少
    // 与coalesce不同之处在于，coalesce仅仅能将rdd的partition变少
    // 但是repartition可以将rdd的partiton变多

    // 建议使用的场景
    // 一个很经典的场景，使用Spark SQL从hive中查询数据时
    // Spark SQL会根据hive对应的hdfs文件的block数量还决定加载出来的数据rdd有多少个partition
    // 这里的partition数量，是我们根本无法设置的

    // 有些时候，可能它自动设置的partition数量过于少了，导致我们后面的算子的运行特别慢
    // 此时就可以在Spark SQL加载hive数据到rdd中以后
    // 立即使用repartition算子，将rdd的partition数量变多

    // 案例
    // 公司要增加新部门
    // 但是人员还是这么多，所以我们只能使用repartition操作，增加部门
    // 将人员平均分配到更多的部门中去
    private static void testRepartition(){
        javaSparkContext = SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_FUNCTION_APP);
        List<String> staffList = Arrays.asList("张三", "李四", "王二", "麻子",
                "赵六", "王五", "李大个", "王大妞", "小明", "小倩");
        JavaRDD<String> staffRDD = javaSparkContext.parallelize(staffList, 3);

        JavaRDD<String> staffRDD2 = staffRDD.mapPartitionsWithIndex(

                new Function2<Integer, Iterator<String>, Iterator<String>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterator<String> call(Integer index, Iterator<String> iterator)
                            throws Exception {
                        List<String> list = new ArrayList<String>();

                        while(iterator.hasNext()) {
                            String staff = iterator.next();
                            list.add("部门[" + (index + 1) + "], " + staff);
                        }

                        return list.iterator();
                    }

                }, true);
        System.out.println("=========没有扩充RDD=====");
        for(String staffInfo : staffRDD2.collect()) {
            System.out.println(staffInfo);
        }

        JavaRDD<String> staffRDD3 = staffRDD2.repartition(6);

        JavaRDD<String> staffRDD4 = staffRDD3.mapPartitionsWithIndex(

                new Function2<Integer, Iterator<String>, Iterator<String>>() {

                    private static final long serialVersionUID = 1L;

                    @Override
                    public Iterator<String> call(Integer index, Iterator<String> iterator)
                            throws Exception {
                        List<String> list = new ArrayList<String>();

                        while(iterator.hasNext()) {
                            String staff = iterator.next();
                            list.add("部门[" + (index + 1) + "], " + staff);
                        }

                        return list.iterator();
                    }

                }, true);

        System.out.println("==========RDD分区扩充===========");
        for(String staffInfo : staffRDD4.collect()) {
            System.out.println(staffInfo);
        }
        javaSparkContext.close();
    }

    // 这个有点问题
    private static void testSample(){
        javaSparkContext = SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_FUNCTION_APP);
        List<String> staffList = Arrays.asList("张三", "李四", "王二", "麻子",
                "赵六", "王五", "李大个", "王大妞", "小明", "小倩");
        JavaRDD<String> staffRDD = javaSparkContext.parallelize(staffList);

        // sample算子
        // 可以使用指定的比例，比如说0.1或者0.9，从RDD中随机抽取10%或者90%的数据
        // 从RDD中随机抽取数据的功能
        // 推荐不要设置第三个参数，feed

        JavaRDD<String> luckyStaffRDD = staffRDD.sample(false, 0.3);
        for(String staff : luckyStaffRDD.collect()) {
            System.out.println(staff);
        }

        javaSparkContext.close();
    }

    private static void testTakeSampled(){
        javaSparkContext = SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_FUNCTION_APP);
        List<String> staffList = Arrays.asList("张三", "李四", "王二", "麻子",
                "赵六", "王五", "李大个", "王大妞", "小明", "小倩");
        JavaRDD<String> staffRDD = javaSparkContext.parallelize(staffList);

        // takeSample算子
        // 与sample不同之处，两点
        // 1、action操作，sample是transformation操作
        // 2、不能指定抽取比例，只能是抽取几个

        List<String> luckyStaffList = staffRDD.takeSample(false, 3);

        for(String luckyStaff : luckyStaffList) {
            System.out.println(luckyStaff);
        }
        javaSparkContext.close();
    }

    private static void testUnion(){
        javaSparkContext = SparkContextUtil.getSparkContextUtil(SparkJobConstant.SPARK_CLUSTER_MODEL_LOCAL,SparkJobConstant.SPARK_JOB_FUNCTION_APP);
        List<String> department1StaffList = Arrays.asList("张三", "李四", "王二", "麻子");
        JavaRDD<String> department1StaffRDD = javaSparkContext.parallelize(department1StaffList);

        List<String> department2StaffList = Arrays.asList("赵六", "王五", "小明", "小倩");
        JavaRDD<String> department2StaffRDD = javaSparkContext.parallelize(department2StaffList);

        JavaRDD<String> departmentStaffRDD = department1StaffRDD.union(department2StaffRDD);

        for(String staff : departmentStaffRDD.collect()) {
            System.out.println(staff);
        }

        javaSparkContext.close();
    }

    private static void testBroadCast(){}


    public static void main(String []args){
//        testAccumulator();
//        testReduce();
//        testCollect();
//          testCount();
//        testTake();
//        testSaveText();
//        testMap();
//        testFlatMap();
//        testFilter();
//        testJoin();
//        testReduceByKey();
//        testCountByKey();
//        testGroupByKey();
//        testSortByKey();
//        testCogroup();
//        testAggregateByKey();
//        testCartesian();
//        testCoalesce();
//        testDistinct();
//        testIntersection();
//        testMapPartitions();
//        testMapPartitionsWithIndex();
//        testRepartition();
//        testUnion();
//        testSample();
        testTakeSampled();

    }



}
