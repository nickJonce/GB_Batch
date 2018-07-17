package com.hp.it.transformers;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.functions;

import com.hp.it.test.CalcuTheNumOfRepeatStamp;
import com.hp.it.utils.db.XmlSchema;

public class ReadFileHelper {

	static Logger logger = Logger.getLogger(ReadFileHelper.class);
	
	/**
	 * Get all the unexecute parquet file path (though the 'GenerateFilePath' from the config.properties)
	 * @param pathLocation
	 * @return listpath
	 */
	public static List<String> getAllGeneratePath(String pathLocation) {
		
		File filepath = new File(pathLocation);
		
		if (!filepath.exists()) {
			return null;
		}
		
		List<String> listpath = new ArrayList<String>();
		BufferedReader br = null;
		String line = null;
		try {
			br = new BufferedReader(new FileReader(filepath));
			if (br.ready()) {
				while ((line = br.readLine()) != null) {
					listpath.add(line);
				}
			}
		} catch (Exception e) {
			logger.error("Load Part when read GenerateFilePath file path occur error:" + e.getMessage());
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					logger.error("when colse the BufferRead occur error:" + e.getMessage());
				}
			}
		}
		return listpath;
	}

	/**
	 * Read Parquet File convert it into dataframe and then load it into vertica
	 * @param listpath
	 * @param eTestXMLSchema
	 * @param sqlcontext
	 * @param prop
	 */
	public static void loadIntoVertica(List<String> listpath, XmlSchema XmlSchema, SQLContext sqlcontext) {
		
		Configuration conf = new Configuration();
		conf.addResource(new Path("/HADOOP_HOME/conf/hdfs-site.xml"));
		
		FileSystem hdfs = null;
		
		try {
			
			hdfs = FileSystem.get(conf);
			
			// All the master table parquet file path
			List<String> masterInfo = new ArrayList<String>();
			
			// All the detail table parquet file path
			List<String> detailInfo = new ArrayList<String>();
			
			// loop the path from config.properties of this properties 'GenerateFilePath' and get all the parquet file not end with ".done"
			LoaderHelper.getAllTheParquetFile(listpath,XmlSchema,masterInfo,detailInfo,hdfs);
			
			/**
			 * According the parquet file path to read parquet file convert it
			 * into dataframe
			 */
			logger.debug("------------------------The num of parquet file have Read begin-----------------------");
			logger.debug(masterInfo.size());
			logger.debug(detailInfo.size());
			logger.debug(listpath.size());
			logger.debug("------------------------The num of parquet file have Read end-----------------------");
			
			//TODO Need put those path info to a map,eg: Map<TimeStamps from file,parquetFilePaths>
			Map<String,List<String>> mapContainOfColumnTime = LoaderHelper.recordAllTheParquetFileColumnOfTime(masterInfo,detailInfo);
			
			if (!masterInfo.isEmpty() || !detailInfo.isEmpty()) {
				
				String[] arrayMaster = masterInfo.toArray(new String[masterInfo.size()]);
				Dataset<Row> masterDataFrame = sqlcontext.read().parquet(arrayMaster);
				
				String[] arrayDetail = detailInfo.toArray(new String[detailInfo.size()]);
				Dataset<Row> detailDataFrame = sqlcontext.read().parquet(arrayDetail);
				
				// below dfMaster dataframe is presulfided master DataFrame.
				Dataset<Row> dfMaster = null;
				// below DataFrame is been combine by master and detail DataFrame.
				Dataset<Row> dfAll = null;
				
				// This attribute format is Map<stamp,min(generateTime)> for remove duplicates stamp have load into DB(just for detail).
				Map<String, String> stampValueMap = new HashMap<String, String>();
				
				//Though the parquet file judge the dataframe have line or not.
				Long masterDataFrameNum = masterDataFrame.count();
				Long detailDataFrameNum = detailDataFrame.count();
				
				//TODO remove Define this List is for log repetition data
				List<String> listForLogRepetitionData = new ArrayList<String>();
				
				//one of the dataframe have data should executed.
				if (masterDataFrameNum > 0 || detailDataFrameNum > 0) {
					
					long thisBatchNumRepeat=0;
					//Deal with the master DataFrame Info.
					if (masterDataFrameNum > 0) {
						// get all the stamp(ignore the stamp is same)
						List<Row> listAllStamp = masterDataFrame.select("stamp").collectAsList();
						// get all the stamp and distinct it
						List<Row> listDistinctStamp = masterDataFrame.select("stamp").distinct().collectAsList();
						
						//TODO remove Do log for repetition data
						listDistinctStamp.forEach(row -> listForLogRepetitionData.add(row.toString().substring(1,row.toString().length()-1)));
						
						thisBatchNumRepeat= listAllStamp.size()-listDistinctStamp.size();
						
						if (listDistinctStamp.size() < listAllStamp.size()) {
							dfMaster = masterDataFrame.groupBy("stamp", "tableName").agg(
									functions.min("currentTime").as("currentTime"),
									functions.min("oneRowData").as("oneRowData"));
						} else {
							dfMaster = masterDataFrame;
						}
						dfAll = dfMaster.select("stamp", "tableName", "currentTime", "oneRowData");
					}
					
//					dfMaster.persist(StorageLevel.DISK_ONLY());
					//Deal with the detail DataFrame Info
					if (detailDataFrameNum > 0) {
						if (dfAll != null) {
							dfAll = dfAll.unionAll(detailDataFrame.select("stamp", "tableName", "currentTime", "oneRowData"));
						} else {
							dfAll = detailDataFrame.select("stamp", "tableName", "currentTime", "oneRowData");
						}
					}
					
					//Below function is to get all the stamp value and corresponding insert_dm from DB and current MasterDataFrame
					//just for detail table remove duplicates.
					ValidationStampIsExit validationStampIsExit = new ValidationStampIsExit();
					stampValueMap = validationStampIsExit.judgeEveryStampIsExitOrNotInDB(
							dfAll.select("stamp", "tableName", "currentTime").distinct().collectAsList(), XmlSchema);
					
					// Get all the stamps from dfAll , select all the stamps from db and judge the stamp is exit on DB ,
					// if exit , put it into below duplicatedStampList list.
					List<String> duplicatedStampList = validationStampIsExit.getMasterStampValue();
					
					// logs --will remove after test
//					CalcuTheNumOfRepeatStamp.calcuTheRepeatStampValue(thisBatchNumRepeat,duplicatedStampList,listForLogRepetitionData);
					
					logger.debug("Begin to Loader data into vertica");
					
					/**
					 * stampValueMap> stamp:time duplicatedStampList:stamp
					 */
					try {
						
						logger.debug(duplicatedStampList.isEmpty());
						logger.debug(stampValueMap.isEmpty());
						
						dfAll.repartition(XmlSchema.getCoalesceLoaderDBNum(),new Column("currentTime")).javaRDD().
								foreachPartition(new RDDLoaderIntoDBNew(stampValueMap, XmlSchema, duplicatedStampList,mapContainOfColumnTime));
						
					} catch (Exception e) {
						logger.error("When loader DataFrame Info into DB and rename parquet file name appear error message is :" + e.getMessage());
					} finally {
						dfAll.unpersist();
					}
					logger.debug("Data have success load into DB");
				}
			}
		} catch (IOException e) {
			logger.error(e.getMessage());
		} catch (IllegalArgumentException e) {
			logger.error(e.getMessage());
		}

	}

}
