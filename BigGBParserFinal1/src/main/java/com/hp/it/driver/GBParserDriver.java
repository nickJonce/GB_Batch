package com.hp.it.driver;

import java.io.File;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import com.hp.it.parserxml.ParserConfigXml;
import com.hp.it.transformers.Constants;
import com.hp.it.util.Helper;
import com.hp.it.utils.db.XmlSchema;

public class GBParserDriver {
	
	static Logger logger = Logger.getLogger(GBParserDriver.class);	
	
	public static void main(String[] args) {
		
		PropertyConfigurator.configure(Constants.LOG4J_FILENAME);
		
		String inputFolder = args[0];
		String outputFolder = args[1];
		
		SparkConf sparkConf = new SparkConf().setAppName("Spark GBParser");
		
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
		
		XmlSchema xs = new XmlSchema();
		
		try{
			xs = ParserConfigXml.getAllTheInfoFromConfigXml();
		}catch(Exception e){
			return;
		}
		
		logger.debug("Load GBConfig.xml into class of XmlSchema");
		
		//Get yesterday's date
		String yesterdayDate=new SimpleDateFormat(xs.getDateformat()).format(new Date(System.currentTimeMillis() - 86400000L));
		
		//Read the filename of 'UnprocessedFilesPath.log2017-06-26'(filter the execute path according to log such as : ExecutePath.log2017-05-07 file) 
		List<String> unProcessedFilePath = Helper.getSourceData(xs,inputFolder,yesterdayDate);
		logger.debug("Loaded unexecute path into unProcessedFilePath");
		
		Helper.ReadFileAndGenerateParquetFile(unProcessedFilePath,sc,xs,outputFolder,sqlContext,yesterdayDate);
		logger.debug("Finished generate parquet File");
		sc.close();
	    
	}

}