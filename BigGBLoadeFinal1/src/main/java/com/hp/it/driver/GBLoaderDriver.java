package com.hp.it.driver;

import java.io.File;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import com.hp.it.parserxml.ParserConfigXml;
import com.hp.it.transformers.Constants;
import com.hp.it.transformers.ReadFileHelper;
import com.hp.it.utils.db.XmlSchema;

/**
 * DataFrame format eg:
 * +--------------------+---------------+----------+-------------+
 * |          oneRowData|          stamp| tableName| currentTime |
 * +--------------------+---------------+----------+-------------+
 * |et34rthd.207_17|E...|et34rthd.207_17| elecj_set| 124323232233|
 * |et34rthd.207_17|e...|et34rthd.207_17|elecj_test| 344343434344|
 * +--------------------+---------------+----------+-------------+
 */

public class GBLoaderDriver {
	
	static Logger logger = Logger.getLogger(GBLoaderDriver.class);
	
	public static void main(String[] args) {
		
		//Just calculate the time have used
		Long starttime=System.currentTimeMillis();
		
		//use log4j do logs
		PropertyConfigurator.configure(Constants.LOG4J_FILENAME);
		
		logger.info("GB loader Start");
		SparkConf sparkConf = new SparkConf().setAppName("Spark GBLoader");
		
		sparkConf.set("spark.sql.shuffle.partitions","500");
		sparkConf.set("spark.defalut.parallelism","37");
		
		// To get this to work when running in Windows
		if (System.getProperty("os.name").toLowerCase().indexOf("win") >= 0)
		{
			sparkConf.setMaster("local[*]");
			File workaround = new File(".");
			System.getProperties().put("hadoop.home.dir", workaround.getAbsolutePath());
		}
		
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		
		SQLContext sqlContext = new SQLContext(sc);
		logger.info("Spark Master: " + sparkConf.get("spark.master"));
		logger.info("Default Parallelism at: " + sc.defaultParallelism());
		
		//Get All the properties and All tables columns info.
		logger.info("Begin to get ColumMetaData from DB");
		XmlSchema xmlSchema = new XmlSchema();
		try{
			xmlSchema = ParserConfigXml.getAllTheInfoFromConfigXml();
		}catch(Exception e){
			return;
		}
		/**
		 * get all the 'GenerateFilePath' need to read from config.properties and put all the path into a list. 
		 * File content:
		 * /MANF/data/outputdir/Gradebook/year=2017/month=02/day=07
		 * /MANF/data/outputdir/Gradebook/year=2017/month=02/day=08
		 */
		List<String> listpath = ReadFileHelper.getAllGeneratePath(xmlSchema.getConfigInfo().get("needGenerateFile").get("GenerateFilePath"));
		logger.debug("the path file name :" +xmlSchema.getConfigInfo().get("needGenerateFile").get("GenerateFilePath"));
		
		/**
		 * if the listpath is not null ,load all parquet file into vertica. 
		 */
		if(listpath!=null){
			logger.info("Begin to load data into vertica DB");
			ReadFileHelper.loadIntoVertica(listpath,xmlSchema,sqlContext);
		} else {
			logger.info("GenerateFilePath not exist,this batch end");
		}
		
		sc.stop();
		
		//Just calculate the time have used
		Long endtime=System.currentTimeMillis();
		Long usetime=(endtime-starttime)/1000;
		logger.info("usetime="+usetime);
		
	}
	
}
