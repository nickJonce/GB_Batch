package com.hp.it.test;

import java.io.File;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.hp.it.driver.GBParserDriver;

public class GBTestMain {

	static Logger logger = Logger.getLogger(GBParserDriver.class);	
	
	public static void main(String[] args) {
	
		String inputFolder = args[0];

		SparkConf sparkConf = new SparkConf().setAppName("Spark GBParser");
		
		// To get this to work when running in Windows
		if (System.getProperty("os.name").toLowerCase().indexOf("win") >= 0)
		{
			sparkConf.setMaster("local[*]");
			File workaround = new File(".");
			System.getProperties().put("hadoop.home.dir", workaround.getAbsolutePath());
		}
		
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		Configuration conf = new Configuration();
		conf.set("textinputformat.record.delimiter", "eod: tester");
		
		JavaRDD<String> _sourceRDD = sc
				.newAPIHadoopFile(inputFolder, TextInputFormat.class, LongWritable.class, Text.class, conf).values()
				.map(source -> source.toString());
		
		_sourceRDD.collect().forEach(source -> System.out.println(source));
		
//		System.out.println(_sourceRDD.first());
		
	}

}
