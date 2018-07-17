package com.hp.it.util;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.sql.DataFrame;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;

import com.hp.it.test.CalcuTheFileNum;
import com.hp.it.transformers.Constants;
import com.hp.it.transformers.RDDBatchStamp;
import com.hp.it.transformers.RDDFlatMap_Vertica;
import com.hp.it.transformers.RDDGenerateForVertica;
import com.hp.it.transformers.RDDMapRepeationStamp;
import com.hp.it.transformers.RDDRepetitionStamp;
import com.hp.it.utils.db.ColumnMetaData;
import com.hp.it.utils.db.XmlSchema;
import com.hp.it.validation.ValidationFunction;
import com.hp.it.validation.Validator;

public class GenerateParquetWay {

	static Logger logger = Logger.getLogger(GenerateParquetWay.class);

	public static void greateThenNumOfRDDLimit(JavaSparkContext sc, List<String> listpath, String outputdir,
			XmlSchema xs, String yesterdayDate, Configuration conf, SQLContext sqlContext) {

		JavaRDD<String> sourceRDD = null;
		for (String path : listpath) {
			
			JavaRDD<String> _sourceRDD = sc
					.newAPIHadoopFile(path, TextInputFormat.class, LongWritable.class, Text.class, conf).values()
					.map(source -> source.toString());
			
			if (sourceRDD == null) {
				sourceRDD = _sourceRDD;
			} else {
				sourceRDD = sourceRDD.union(_sourceRDD);
			}
			
		}
//		sourceRDD.cache();
		
		if (sourceRDD != null) {
			try {
				
				//TODO add info get all the stamp to search whether this stamp is exit in DB or not.
////			List<String> batchOfStampValues = sourceRDD.map(littleFile -> littleFile.trim()).map(new RDDBatchStamp())
////			  	.filter(source -> source != null).collect();
////			//This list is for validation,this batch can't content stamp is repeat.
////			List<String> needValidaitionStamps = new ArrayList<String>(); 
////			batchOfStampValues.forEach(everyStampValue -> Helper.findThisBatchAllTheStampAppearTwice(batchOfStampValues,everyStampValue,needValidaitionStamps));
//			
//			//if this batch is appear twice with same stamp should ignore the info.
//			JavaRDD<String> distinctStampRDD = sourceRDD.map(source -> source.toString().trim()).filter(source -> source!=null).distinct();
//			
//			// after distinct rdd get all the stamp.
//			List<String> batchOfStampValues = distinctStampRDD.map(new RDDBatchStamp()).filter(source -> source!=null).collect();
//			
////			System.out.println("Begin get repeation values @@@@@@@@@@@@@@@@@@@@");
//			//if distinct also have stamp value is repetition.
//			List<String> needValidaitionStamps = new ArrayList<String>();  
//			batchOfStampValues.forEach(everyStampValue -> Helper.findThisBatchAllTheStampAppearTwice(batchOfStampValues,everyStampValue,needValidaitionStamps));
//			
////			System.out.println("##############&&&&&&&&&&&&&##############");
////			if(needValidaitionStamps.size()>0){
//////				System.out.println("SIGN");
////				needValidaitionStamps.forEach(source -> System.out.println(source));
////			}
//			
//			//needValidaitionStamps size is not empty, so need 
//			if( needValidaitionStamps.size() > 0 ){
//				JavaRDD<String> repetitionStampValue = distinctStampRDD.map(new RDDMapRepeationStamp(needValidaitionStamps,true)).filter(source -> source!=null);
////				repetitionStampValue.collect().forEach(str -> System.out.println(str));
//				List<String> distinctStamps = Helper.getDistinctStampValue(repetitionStampValue.collect());
//				repetitionStampValue.unpersist();
//				distinctStampRDD=distinctStampRDD.map(new RDDMapRepeationStamp(needValidaitionStamps,false)).filter(source -> source!=null).union(sc.parallelize(distinctStamps));
//			}
				
				// get the currentTime for validation
				long currentTime = System.currentTimeMillis();
				Date currentDate = new Date(currentTime);
				SimpleDateFormat df = new SimpleDateFormat(xs.getDatetimeformate());
				TimeZone timeZone = TimeZone.getTimeZone("GMT+8:00");
				df.setTimeZone(timeZone);
				String currentMinute = df.format(currentDate);
				
				Date currentDataFrameMinute = df.parse(currentMinute);
				
//				JavaRDD<String> _validationRDD = sourceRDD
//						.map(new Validator(xs, currentMinute)).filter(source -> source != null);
				
				JavaRDD<String> _validationRDD = sourceRDD.map(source -> source.trim()).distinct()
						.map(new Validator(xs, currentMinute)).filter(source -> source != null);
				
				logger.debug("Have Converted bz2 file into RDD");
				
				if(!_validationRDD.isEmpty()){
					// below code is calculate how much the num of path folder is contains.
//					CalcuTheFileNum.generateTheFileForCount(listpath, String.valueOf(_validationRDD.count()));
					String partitionDM = getPartitionDM_Date(_validationRDD.first(), xs);
					
					generatePathFileForLoad(partitionDM, xs);
					logger.debug("Have load parquet file path to the "
						+ xs.getConfigInfo().get("needGenerateFile").get("GenerateFilePath"));
					
					// generate DataFrame and generate parquet files
					Dataset<Row> dataframe = sqlContext
							.createDataFrame(
									_validationRDD.flatMap(new RDDFlatMap_Vertica())
											.map(new RDDGenerateForVertica(partitionDM,
													String.valueOf(currentDataFrameMinute.getTime()))),
											getSchemaforVerticaLoad());
				
					logger.debug("DataFrame have generate successed");
//					dataframe.write().jdbc(url, table, connectionProperties);
					
					dataframe.write().partitionBy("year", "month", "day", "table").mode(SaveMode.Append).parquet(outputdir);
					logger.debug("Have generate parquet file");
					
					dataframe.unpersist();
				}
				
				generateProcessedFilePath(listpath, xs, yesterdayDate);
				logger.debug("Record path have execute to "
						+ xs.getConfigInfo().get("needGenerateFile").get("ExecuteFilePath"));
				
				sourceRDD.unpersist();
				_validationRDD.unpersist();
				
			} catch (ParseException e) {
				logger.error("When convert the Date appear error :" + e.getMessage());
			}
			
		}

	}

	private static StructType getSchemaforVerticaLoad() {

		StructType schema = null;

		List<StructField> _fields = new ArrayList<StructField>();

		_fields.add(DataTypes.createStructField("year", DataTypes.StringType, true));
		_fields.add(DataTypes.createStructField("month", DataTypes.StringType, true));
		_fields.add(DataTypes.createStructField("day", DataTypes.StringType, true));
		_fields.add(DataTypes.createStructField("table", DataTypes.StringType, true));
		_fields.add(DataTypes.createStructField("oneRowData", DataTypes.StringType, true));
		_fields.add(DataTypes.createStructField("stamp", DataTypes.StringType, true));
		_fields.add(DataTypes.createStructField("tableName", DataTypes.StringType, true));
		_fields.add(DataTypes.createStructField("currentTime", DataTypes.StringType, true));
		
		schema = DataTypes.createStructType(_fields);
		
		return schema;
		
	}

	public static void generateProcessedFilePath(List<String> listpath, XmlSchema xs, String yesterdayDate) {
		for (String path : listpath) {
			if (path.trim().equals("")) {
				logger.debug("unProcessedFilePathList is empty");
				return;
			}
			String pathYearName = path.substring(path.length() - 15, path.length() - 5);
			recordPathHaveExecute(path, xs, pathYearName);
			logger.debug("successs generate file" + path);
		}
	}

	private static void recordPathHaveExecute(String inputPath, XmlSchema xs, String pathYearName) {
		BufferedWriter out = null;
		try {

			File filepath = new File(xs.getConfigInfo().get("needGenerateFile").get("ExecuteFilePath") + pathYearName);
			logger.debug(pathYearName);
			if (!filepath.exists()) {
				filepath.createNewFile();
			}

			out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filepath, true)));
			out.write(inputPath + "\n");
			out.flush();
		} catch (IOException e) {
			logger.error("Record path that have finish generate paquet file error inputPath is " + inputPath + ":"
					+ e.getMessage());
		} finally {
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					logger.error(
							"Record path that have finish generate paquet file error inputPath when colse out error:"
									+ e.getMessage());
				}
			}
		}
	}

	/***
	 * Get the date decoded from the stamplink field. This is used to partiion
	 * the data written to disk
	 * 
	 * @author heng
	 */
	public static String getPartitionDM_Date(String first, XmlSchema xs) {

		String _partition_dm_date = null;

		SimpleDateFormat partitionDmFormat = new SimpleDateFormat(xs.getDataPartitioningDatepattern());

		String[] arrayFirstValue = first.split(Constants.NEWLINE);

		for (String line : arrayFirstValue) {
			
			if(line == null || line.trim().equals(Constants.EMPTYSTRING)){
				continue;
			}
			
			String keyword = line.replaceFirst("^(\\w+):.*", "$1").trim();
			String value = line.replaceFirst("^(\\w+):(.*)", "$2").trim();

			if (xs.getMasterTablesInfo().containsKey(keyword)) {
				
				String[] arrayValue = value.split(Constants.PIPE);
				
				_partition_dm_date = partitionDmFormat
						.format(ValidationFunction.ConvertStampToPartitionDm(arrayValue[0]));
				
				break;

			}
		}
		return _partition_dm_date;
	}

	/**
	 * generate a new file and write generated parquet file path into this text
	 * file to load parquet file into vertica.
	 * 
	 * @param partitionDM
	 * @param prop
	 */
	public static void generatePathFileForLoad(String partitionDM, XmlSchema xs) {

		String[] arrayPartitionDM = partitionDM.split(Constants.HYPHEN);

		String year = arrayPartitionDM[0];
		String month = arrayPartitionDM[1];
		String day = arrayPartitionDM[2];

		BufferedReader br = null;
		BufferedWriter out = null;
		
		try {
			
			File filepath = new File(xs.getConfigInfo().get("needGenerateFile").get("GenerateFilePath"));
			if (!filepath.exists()) {
				filepath.createNewFile();
			}

			br = new BufferedReader(new FileReader(filepath));
			String line = null;
			List<String> listPath = new ArrayList<String>();
			String generateNewPath = xs.getConfigInfo().get("needGenerateFile").get("NewFilePath").trim() + "/"
					+ "year=" + year + "/" + "month=" + month + "/" + "day=" + day;
			
			if (br.ready()) {
				while ((line = br.readLine()) != null) {
					listPath.add(line.trim());
				}
			}
			
			if (listPath.isEmpty() || listPath == null) {
				out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filepath, true)));
				out.write(generateNewPath + "\n");
			} else {
				if (!listPath.contains(generateNewPath)) {
					out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filepath, true)));
					out.write(generateNewPath + "\n");
				}
			}
			
		} catch (IOException e) {
			logger.error("Generate path file for load :" + e.getMessage());
		} finally {
			
			if (out != null) {
				try {
					out.close();
				} catch (IOException e) {
					logger.error("Generate path file for load :" + e.getMessage());
				}
			}

			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					logger.error("Generate path file for load :" + e.getMessage());
				}
			}

		}
	}

}
