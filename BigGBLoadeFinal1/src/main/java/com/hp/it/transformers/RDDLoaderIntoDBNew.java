package com.hp.it.transformers;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.function.VoidFunction;
import org.apache.spark.sql.Row;

import com.hp.it.utils.db.XmlSchema;
import com.vertica.jdbc.VerticaConnection;
import com.vertica.jdbc.VerticaCopyStream;

public class RDDLoaderIntoDBNew implements VoidFunction<Iterator<Row>> {
	
	private static final long serialVersionUID = -2669111674702497950L;
	
	static Logger logger = Logger.getLogger(RDDLoaderIntoDB.class);
	
	private Map<String,String> stampValueMap;
	
	private XmlSchema xmlSchema;
	
	private List<String> masterStamps;
	
	private Map<String,List<String>> mapContainOfColumnTime;
	
	public RDDLoaderIntoDBNew(Map<String, String> _stampValueMap, XmlSchema _xmlSchema , List<String> _masterStamps ,Map<String,List<String>> _mapContainOfColumnTime) {
		super();
		this.stampValueMap = _stampValueMap;
		this.xmlSchema = _xmlSchema;
		this.masterStamps = _masterStamps;
		this.mapContainOfColumnTime = _mapContainOfColumnTime;
	}
	
	public void call(Iterator<Row> rows) throws Exception {
		
		Connection conn = null;
		
		Properties myProp = new Properties();
		
		myProp.put("user",xmlSchema.getConfigInfo().get("dbConnection").get("dbUser"));
		myProp.put("password",xmlSchema.getConfigInfo().get("dbConnection").get("dbEncryptedPassword"));
		myProp.put("AutoCommit",xmlSchema.getConfigInfo().get("dbConnection").get("autoCommit")); 
		
		//This value is to get this partition all the current times
		List<String> listCurrentPartitionTimes = new ArrayList<String>();
		
		try{
			
			conn = DriverManager.getConnection(
					xmlSchema.getConfigInfo().get("dbConnection").get("dbURL"),
					myProp
					);
			
			logger.info("--Get the Vertica Connection--");
			
			Map<String,VerticaCopyStream> masterVerticaMapStream = new HashMap<String,VerticaCopyStream>();
			
			Map<String,VerticaCopyStream> detailVerticaMapStream = new HashMap<String,VerticaCopyStream>();
			
			String exceptinpath=xmlSchema.getConfigInfo().get("needGenerateFile").get("HPETestExceptionPath");
			String rejectionpath=xmlSchema.getConfigInfo().get("needGenerateFile").get("HPETestRejectPath");
			
			File exceptionfile=new File(exceptinpath);
			File rejectionfile=new File(rejectionpath);
			
			if(!exceptionfile.exists()){
				exceptionfile.createNewFile();
			}
			
			if(!rejectionfile.exists()){
				rejectionfile.createNewFile();
			}
			
			for(String masterTableName : xmlSchema.getMasterTablesInfo().keySet()){
				String copyQueryMaster = "COPY mfg_pms." + masterTableName.trim() + " FROM LOCAL STDIN "
                        + "DELIMITER '|' DIRECT exceptions '"+exceptinpath+"' REJECTED DATA '"+rejectionpath+"'";
				
				VerticaCopyStream masterStream = new VerticaCopyStream((VerticaConnection) conn, copyQueryMaster);
				masterStream.start();
				masterVerticaMapStream.put(masterTableName, masterStream);
			}
			
			for(String detailTableName : xmlSchema.getDetailTablesInfo().keySet()){
				String copyQueryDetail = "COPY mfg_pms." + detailTableName.trim() + " FROM LOCAL STDIN "
                        + "DELIMITER '|' DIRECT exceptions '"+exceptinpath+"' REJECTED DATA '"+rejectionpath+"'";
				
				VerticaCopyStream detailStream = new VerticaCopyStream((VerticaConnection) conn, copyQueryDetail);
				detailStream.start();
				detailVerticaMapStream.put(detailTableName, detailStream);
			}
			
			logger.info("--stream Start--");
			
			while(rows.hasNext()){
				String lineInfo = rows.next().toString();
				if(lineInfo.equals("")||lineInfo==null){
					continue;
				}
				String newline = lineInfo.substring(1, lineInfo.length()-1);
				String[] line = newline.split(Constants.COMMA);
				
				String stamp = line[0];
				String tableName = line[1];
				String currentTime = line[2];
				String oneRowData = line[3];
				
				logger.debug(oneRowData+"\n"+stamp+"\n"+tableName+"\n"+currentTime);
				
				if(!listCurrentPartitionTimes.contains(currentTime)){
					listCurrentPartitionTimes.add(currentTime);
				}
				
				if(masterVerticaMapStream.containsKey(tableName)){
					InputStream in_nocode_master = new ByteArrayInputStream((oneRowData+"\n").getBytes());
					if(!masterStamps.contains(stamp)){
						masterVerticaMapStream.get(tableName).addStream(in_nocode_master);
					}
				}else if(detailVerticaMapStream.containsKey(tableName)){
					InputStream in_nocode_detail = new ByteArrayInputStream((oneRowData+"\n").getBytes());
					if(stampValueMap.containsKey(stamp)&&stampValueMap.get(stamp).equals(currentTime)){
						detailVerticaMapStream.get(tableName).addStream(in_nocode_detail);
					}
				}
				
			}
		
		logger.info("--stream have finished added--");
		for(VerticaCopyStream masterStream : masterVerticaMapStream.values()){
			masterStream.execute();
		}
		
		for(VerticaCopyStream detailStream : detailVerticaMapStream.values()){
			detailStream.execute();
		}
		
		logger.info("--stream executed--");
		conn.commit();
		logger.info("--conn commit success--");
		
		/**
		 * Because of one partition possible have all the some generate time data
		 * if above table data have success load into DB 
		 * then need to find those parquet file that content those data
		 * and rename them.
		 */
		Configuration conf = new Configuration();
		conf.addResource(new Path("/HADOOP_HOME/conf/hdfs-site.xml"));
		FileSystem hdfs = null;
		hdfs = FileSystem.get(conf);
		for(String eleOfTime:listCurrentPartitionTimes){
			for(String parquetFilePath: mapContainOfColumnTime.get(eleOfTime)){
				LoaderHelper.appendParquetFileDoneThatHaveLoadedIntoDB(parquetFilePath,hdfs);
			}
		}
		if(hdfs!=null){
			hdfs.close();
		}
		
		}catch(Exception e){
			conn.rollback();
			logger.error("----------------------------------------------");
			logger.error("Load into vertica appear error:"+e.getMessage());
			logger.error("----------------------------------------------");
			throw new RuntimeException(e);
		}finally{
			
			if(conn!=null){
				try{
					conn.close();
				}catch (SQLException e) {
					logger.error("when close conn appear error message:"+e.getMessage());
				}
			}
			
		}
				
		/**
		 * Because of one partition possible have all the some generate time data
		 * if above table data have success load into DB 
		 * then need to find those parquet file that content those data
		 * and rename them.
		 */
//		Configuration conf = new Configuration();
//		conf.addResource(new Path("/HADOOP_HOME/conf/hdfs-site.xml"));
//		FileSystem hdfs = null;
//		try{
//			hdfs = FileSystem.get(conf);
//			
//			for(String eleOfTime:listCurrentPartitionTimes){
//				for(String parquetFilePath: mapContainOfColumnTime.get(eleOfTime)){
//					LoaderHelper.appendParquetFileDoneThatHaveLoadedIntoDB(parquetFilePath,hdfs);
//				}			
//			}
//			
//		}catch(Exception e){
//			logger.error("When append Paruquet '.done' have appear error :" + e.getMessage());
//		}finally{
//			if(hdfs!=null){
//				hdfs.close();
//			}
//		}
		
	}

	
}
