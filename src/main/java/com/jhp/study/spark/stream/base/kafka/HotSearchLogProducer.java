package com.jhp.study.spark.stream.base.kafka;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.Random;

/**
 * 访问日志Kafka Producer
 * @author Administrator
 *
 */
public class HotSearchLogProducer extends Thread {

	private static Random random = new Random();
	static String [] user ={"jhp","lyn","yy","yaojie","liaijun","szh","wangping","zhangjun"};
	static String [] kw ={"双截棍","袜子","皮鞋","连衣裙","高跟鞋","蕾丝裙","吊带裙","钱包","大数据","人工智能","机器学习"};

	private Producer<Integer, String> producer;
	private String topic;

	public HotSearchLogProducer(String topic) {
		this.topic = topic;
		producer = new Producer<Integer, String>(createProducerConfig());  
	}
	
	private ProducerConfig createProducerConfig() {
		Properties props = new Properties();
		props.put("serializer.class", "kafka.serializer.StringEncoder");
		props.put("metadata.broker.list", "localhost:9092,localhost:9093,localhost:9094");
		return new ProducerConfig(props);  
	}
	
	public void run() {
		int counter = 0;

		while(true) {
			for(int i = 0; i < 100; i++) {
				String log = null;
				producer.send(new KeyedMessage<Integer, String>(topic, getAccessLog()));
				counter++;
				if(counter == 100) {
					counter = 0;
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}  
				}
			}
		}
	}
	
	private static String getAccessLog() {
		StringBuffer buffer = new StringBuffer("");
		String uname = user[random.nextInt(user.length)];
		String keyword = kw[random.nextInt(kw.length)];
		return buffer.append(uname).append(" ").append(keyword).append(" ").toString();
	}

	
	public static void main(String[] args) {
		HotSearchLogProducer producer = new HotSearchLogProducer("hot_kw_log");
		producer.start();
	}
	
}
