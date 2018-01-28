package com.jhp.study.spark.sql.app.keyword;

import java.io.FileOutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.*;

/**
 * 模拟访问应用日志生成  日期 用户 搜索词 城市 平台 版本 : 2017-09-10 jhp abc shanghai android 1.0
 */
public class SearchLogDataGenerator {
	
	public static void main(String[] args) throws Exception {

		String [] date ={"2017-09-01","2017-09-02"};
		String [] user ={"jhp","lyn","yy","yaojie","liaijun","szh","wangping","zhangjun"};
		String [] kw ={"双截棍","袜子","皮鞋","连衣裙","高跟鞋","蕾丝裙","吊带裙","钱包","大数据","人工智能","机器学习"};
		String [] city ={"上海","南京"};
		String []platform ={"android","iphone"};
		String [] version ={"1.0","2.0"};

		Random random = new Random();
		

		
		StringBuffer buffer = new StringBuffer("");  
		
		for(int i = 0; i < 1500; i++) {
			String time = date[random.nextInt(date.length)];
			String uname = user[random.nextInt(user.length)];
			String keyword = kw[random.nextInt(kw.length)];
			String ct = city[random.nextInt(city.length)];
			String pt = platform[random.nextInt(platform.length)];
			String vs = version[random.nextInt(version.length)];

			buffer.append(time).append("\t")
					.append(uname).append("\t")
					.append(keyword).append("\t")
					.append(ct).append("\t")
					.append(pt).append("\t")
					.append(vs).append("\n");
		}
		
		PrintWriter pw = null;
		try {
			pw = new PrintWriter(new OutputStreamWriter(
					new FileOutputStream("/soft/spark-study-java-1.6x/src/main/resources/search_keyword.log")));
			pw.write(buffer.toString());
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			pw.close();
		}
	}
	

	
}
