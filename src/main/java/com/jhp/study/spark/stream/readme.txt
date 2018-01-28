spark 读取Kafka的数据的不同形式

1 direct:数据并行化读取 高性能 一次仅一次事务机制
2 receiver:spark链接zookeeper,zookeeper读取kakfa中的数据,效率比较底下 同时数据存在丢失的风险性 如果不想数据丢失 需要开启数据预写入功能 这样
一来写入性能就比较低了

当然 生产环境中 基本都是要使用Kafka作为spark的数据源头 所以我们一定要对这种流式数据计算比较熟悉

http://www.apache.org/dyn/closer.lua/flume/1.7.0/apache-flume-1.6.0-bin.tar.gz