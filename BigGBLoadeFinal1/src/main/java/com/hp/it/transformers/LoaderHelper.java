package com.hp.it.transformers;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.parquet.avro.AvroParquetReader;

import com.hp.it.utils.db.Table;
import com.hp.it.utils.db.XmlSchema;

public class LoaderHelper {

	static Logger logger = Logger.getLogger(LoaderHelper.class);

	/**
	 * This Function is to append the parquet file ".done".
	 * 
	 * @param listCurrentPartitionTableNames
	 * @param parquetPath
	 */

	public static void appendParquetFileDoneThatHaveLoadedIntoDB(String parquetPath, FileSystem hdfs) {

		try {

			Path newParquetFile = new Path(parquetPath + ".done");
			hdfs.rename(new Path(parquetPath), newParquetFile);
			logger.debug("Have success rename this parquet path:" + parquetPath);

		} catch (Exception e) {
			logger.error("when append '.done' to parquet file appear error : " + e.getMessage());
			throw new RuntimeException(e);
		}

	}

	/**
	 * Get all the master and detail parquet file path and should limit size.
	 * 
	 * @param listpath
	 * @param XmlSchema
	 * @param masterInfo
	 * @param detailInfo
	 * @param limitDetailSizeMap
	 * @param hdfs
	 * @throws IllegalArgumentException
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public static void getAllTheParquetFile(List<String> listpath, XmlSchema XmlSchema, List<String> masterInfo,
			List<String> detailInfo, FileSystem hdfs)
			throws IllegalArgumentException, FileNotFoundException, IOException {

		String masterTablePath = null;

		// This value is to store every table file size,for compared.
//		Map<String, Long> limitDetailSizeMap = new HashMap<String,Long>();

		long maxDetailSize = 0;
		
		long masterTableParquetFilesNum = 0;

		for (String path : listpath) {

			/**
			 * Below code is to judge master table folder is exist or not
			 * /MANF/data/outputdir/eTest/year=2017/month=02/day=07/
			 * table=elecj_set
			 */
			for (Table table : XmlSchema.getMasterTablesInfo().values()) {

				masterTablePath = path + "/table=" + table.getTableName().trim();
				FileStatus newtempmaster[] = null;

				if (hdfs.exists(new Path(masterTablePath))) {
					// if folder exit,below this path put all the file to a
					// array
					newtempmaster = hdfs.listStatus(new Path(masterTablePath));
				}

				// if this array isn't null
				if (newtempmaster != null) {
					Path parquetMasterFile = null;
					// loop this array and judge parquet file is
					// generate 5 min ago,and not end with '.done'
					// if suit put this file into list of 'masterInfo'
					for (int i = 0; i < newtempmaster.length; i++) {

						parquetMasterFile = new Path(newtempmaster[i].getPath().toString());

						String parquetMasterPath = parquetMasterFile.toString()
								.substring(parquetMasterFile.toString().lastIndexOf('.') + 1);
						
						// Just read the parquet file that end with ".parquet"
						if (!parquetMasterPath.equalsIgnoreCase("parquet")) {
							continue;
						}
						
						FileStatus fsmaster = hdfs.getFileStatus(parquetMasterFile);
						long currentMasterTime = System.currentTimeMillis();
						long modificationMasterTime = fsmaster.getModificationTime();
						
						// only read the parquet file that have generated 5
						// minute later
						if (((currentMasterTime - modificationMasterTime) / 1000 / 60) > 5) {
							if(masterTableParquetFilesNum<=Long.valueOf(5000)){
								masterInfo.add(parquetMasterFile.toString().trim());
								masterTableParquetFilesNum++;
							}else{
								break;
							}
						}

					}

				}
				if(masterTableParquetFilesNum>=Long.valueOf(5000)){
					break;
				}
				
			}
			
			if(masterTableParquetFilesNum>=Long.valueOf(5000)){
				break;
			}

		}

		/**
		 * Below code is to judge all the detail table folder is exist or not
		 * /MANF/data/outputdir/eTest/year=2017/month=02/day=07/ table=elecj_pad
		 * /MANF/data/outputdir/eTest/year=2017/month=02/day=07/
		 * table=elecj_test
		 * /MANF/data/outputdir/eTest/year=2017/month=02/day=07/
		 * table=elecj_detail
		 * /MANF/data/outputdir/eTest/year=2017/month=02/day=07/
		 * table=elecj_subfamily ............... Loop all the detail table and
		 * same processing mode with master table.
		 */

		if (masterTableParquetFilesNum < Long.valueOf(5000)) {
			for (String path : listpath) {
				
				for (Table table : XmlSchema.getDetailTablesInfo().values()) {

					String detailTableName = table.getTableName().trim();

					String detailTablePath = path + "/table=" + detailTableName;
					
					FileStatus newtempdetial[] = null;

					// if detail table name is not exist in this map should put key and value into it Map<detailTableName,null>
//					if (!limitDetailSizeMap.containsKey(detailTableName)) {
//						limitDetailSizeMap.put(detailTableName, null);
//					}

					if (hdfs.exists(new Path(detailTablePath))) {
						// if folder exit,below this path put all the file to a array
						newtempdetial = hdfs.listStatus(new Path(detailTablePath));
					}

					if (newtempdetial != null) {

						Path parquetDetailFile = null;

						for (int i = 0; i < newtempdetial.length; i++) {

							parquetDetailFile = new Path(newtempdetial[i].getPath().toString());
							String parquetDetialPath = parquetDetailFile.toString()
									.substring(parquetDetailFile.toString().lastIndexOf('.') + 1);

							// just read the parquet file that end with .parquet
							if (!parquetDetialPath.equalsIgnoreCase("parquet")) {
								continue;
							}

							Long fileSize = newtempdetial[i].getLen();
							
							maxDetailSize=maxDetailSize+fileSize;
							
//							if (limitDetailSizeMap.get(detailTableName) != null) {
//								Long newValue = limitDetailSizeMap.get(detailTableName) + fileSize;
//								limitDetailSizeMap.put(detailTableName, newValue);
//							} else {
//								limitDetailSizeMap.put(detailTableName, fileSize);
//							}
							
//							//TODO need add validation to judge the table name is elecj_resistor or not.
//							if(limitDetailSizeMap.get(detailTableName).equals("pwax_elecj_resistor")){
//								if (limitDetailSizeMap.get(detailTableName) < (XmlSchema.getLimitOfParquetFileSize()*1024*1024*4)) {
//									FileStatus fsdetail = hdfs.getFileStatus(parquetDetailFile);
//									long currentDetailTime = System.currentTimeMillis();
//									long modificationDetailTime = fsdetail.getModificationTime();
//									// only read the parquet file that have generated 5 minute later
//									if ((currentDetailTime - modificationDetailTime) / 1000 / 60 > 5) {
//										detailInfo.add(parquetDetailFile.toString().trim());
//									}
//								} else {
//									break;
//								}
//							}else{
								if (maxDetailSize < (XmlSchema.getLimitOfParquetFileSize()*1024*1024)) {
									FileStatus fsdetail = hdfs.getFileStatus(parquetDetailFile);
									long currentDetailTime = System.currentTimeMillis();
									long modificationDetailTime = fsdetail.getModificationTime();
									// only read the parquet file that have generated 5 minute later
									if ((currentDetailTime - modificationDetailTime) / 1000 / 60 > 5) {
										detailInfo.add(parquetDetailFile.toString().trim());
									}
								} else {
									break;
								}
								
//							}
						}
					}
					if(maxDetailSize > (XmlSchema.getLimitOfParquetFileSize()*1024*1024)){
						break;
					}
				}
				
				if(maxDetailSize > (XmlSchema.getLimitOfParquetFileSize()*1024*1024)){
					break;
				}
				
			}
		}

	}

	/**
	 * Record all the parquet file column of time.
	 * 
	 * @param masterInfo
	 * @param detailInfo
	 * @return
	 */
	@SuppressWarnings("deprecation")
	public static Map<String, List<String>> recordAllTheParquetFileColumnOfTime(List<String> masterInfo,
			List<String> detailInfo) {

		Map<String, List<String>> mapContainOfColumnTime = new HashMap<String, List<String>>();

		List<String> listAllThePath = new ArrayList<String>();

		listAllThePath.addAll(masterInfo);
		listAllThePath.addAll(detailInfo);

		for (String parquetFilePath : listAllThePath) {

			AvroParquetReader<GenericRecord> reader = null;
			try {
				reader = new AvroParquetReader<GenericRecord>(new Path(parquetFilePath));
				GenericRecord record;
				String currentTime = null;
				while ((record = reader.read()) != null) {
					currentTime = (String) record.get("currentTime");
					if (currentTime != null) {
						break;
					}
				}

				if (currentTime != null) {
					if (mapContainOfColumnTime.containsKey(currentTime)) {
						mapContainOfColumnTime.get(currentTime).add(parquetFilePath);
					} else {
						mapContainOfColumnTime.put(currentTime, new ArrayList<String>());
						mapContainOfColumnTime.get(currentTime).add(parquetFilePath);
					}
				}

			} catch (Exception e) {
				logger.error("Read Parquet file have appear error:" + e.getMessage());
			} finally {
				if (reader != null) {
					try {
						reader.close();
					} catch (IOException e) {
						logger.error("Colse reader have appear error message:" + e.getMessage());
					}
				}
			}

		}

		return mapContainOfColumnTime;

	}

}
