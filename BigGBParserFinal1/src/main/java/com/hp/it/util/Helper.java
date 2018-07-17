package com.hp.it.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SQLContext;

import com.hp.it.transformers.Constants;
import com.hp.it.utils.db.XmlSchema;

public class Helper {

	static Logger logger = Logger.getLogger(Helper.class);
	
	/**
	 * @param xs
	 * @param inputFolder
	 * @param yesterdayDate
	 * @return List<String> UnexecutePaths
	 */
	public static List<String> getSourceData(XmlSchema xs, String inputFolder, String yesterdayDate) {
		
		//Content all the UnExecutePath.
		List<String> filesToBeProcessed = new ArrayList<String>();;
		
		//Use FS stream to judge the file is exit or not.
		Configuration conf = new Configuration();
		conf.addResource(new Path(xs.getConfigInfo().get("needGenerateFile").get("HDFSCONF_PATH")));
		
		try {
			
			FileSystem hdfs = FileSystem.get(conf);
			
			//Judge the file is exit or not,if exit read all of unexecute path in this file(Need according to ExecutePath.log to fileter the execute path).
			logger.info("Begin to read the type of bz2 file and convert it into RDD");
			
			//TODO
//			String filePath = xs.getConfigInfo().get("needGenerateFile").get("UnprocessedFilesPath") + yesterdayDate;
//			String filePath = xs.getConfigInfo().get("needGenerateFile").get("UnprocessedFilesPath");
//			if ( Helper.doesFileExist(filePath) ) {
//				logger.debug("Unprocessed files present at: " + filePath);
			//Loader All the file that is begin char of "U"
			filesToBeProcessed = Helper.getListOfFilesToBeProcessed(hdfs,xs);
//			}
			
			//below are the current time folder name.
			if (!inputFolder.trim().equals("allTheInputdirHaveNoFile")) {
				for (String path : xs.getConfigInfo().get("allTheInputdirPath").values()) {
					if(hdfs.isDirectory(new Path(path + inputFolder))){
						filesToBeProcessed.add(path + inputFolder);
					}
				}
			}
			
		} catch (IOException e) {
			logger.error("When use FS Stream to judge file path is exist,appear error message is:"+e.getMessage());
		}
		
		return filesToBeProcessed;
		
	}
	
	/**
	 * Judge the UnprocessedFilesPath.log is exit or not.
	 * @param path
	 * @return
	 */
	public static boolean doesFileExist(String path) {
		File file = new File(path);
		if (file.exists() && !file.isDirectory()) {
			return true;
		} else {
			return false;
		}
	}
	
	/**
	 * This Function is to Read file of 'UnFinishedFilePath.log' to get all the line paths.
	 * @param path
	 * @param fs
	 * @return
	 */
	public static List<String> getListOfFilesToBeProcessed(FileSystem fs,XmlSchema xs) {
		
		List<String> tempListPath = new ArrayList<String>();
		
		String tempPath = xs.getConfigInfo().get("needGenerateFile").get("TempFolderPath");
		
//		System.out.println(tempPath);
		
		File tempFile = new File(tempPath);
		
        for(File temp : tempFile.listFiles()) {
            
        	if(temp.isFile()) {
            	
            	String fileName = temp.getName();
            	
//            	System.out.println(fileName);
            	if(fileName.substring(0,1).equalsIgnoreCase("U")){
            		String filePath = tempPath+fileName;
            		tempListPath.addAll(readEveryFileAndPutPathIntoIt(fs,filePath,xs));
            	}
            	
            }
            
        }
		
        return tempListPath;
		
	}
	
	public static List<String> readEveryFileAndPutPathIntoIt(FileSystem fs,String filePath,XmlSchema xs){
		
		File file = new File(filePath);
		
		List<String> list = new ArrayList<String>();
		BufferedReader br = null;
			
			try {
				
				br = new BufferedReader(new FileReader(file));
				
				String line = null;
				
				if (br.ready()) {
					while ((line = br.readLine()) != null) {
						if(fs.isDirectory(new Path(line.trim()))&&JudgeTheUnexectePathIsExecuteOrNot(line,xs)){
							list.add(line.trim());
						}
					}
				}
				
			} catch (FileNotFoundException e) {
				logger.error(e.getMessage());
			} catch (IOException e) {
				logger.error(e.getMessage());
			} finally {
				if (br != null) {
					try {
						br.close();
					} catch (IOException e) {
						logger.error("Read UnFinishedFilePath error when colse BufferedReader:" + e.getMessage());
					}
				}
			}
			return list;
			
	}
	
	/**
	 * @param inputPaths
	 * @param sc
	 * @param xs
	 */
	public static void ReadFileAndGenerateParquetFile(List<String> inputPaths , JavaSparkContext sc ,XmlSchema xs,String outputFolder,SQLContext sqlContext,String yesterdayDate){
		
		Configuration conf = new Configuration();
		conf.set("textinputformat.record.delimiter", "eod: tester");
		
		List<String> listpaths = new ArrayList<String>();
		for(String path : inputPaths){
			
			listpaths.add(path);
			
			if(listpaths.size()>=Integer.parseInt(xs.getNumOfRDDLimit())){
				GenerateParquetWay.greateThenNumOfRDDLimit(sc,listpaths,outputFolder,xs,yesterdayDate,conf,sqlContext);
				listpaths.clear();
			}
		    
		}
		
		if(!listpaths.isEmpty()){
			GenerateParquetWay.greateThenNumOfRDDLimit(sc,listpaths,outputFolder,xs,yesterdayDate,conf,sqlContext);
		}
		
		String unexecuteFilePaths = xs.getConfigInfo().get("needGenerateFile").get("TempFolderPath");
		String needMoveFilePaths = xs.getConfigInfo().get("needGenerateFile").get("TempBackFilePath");
		
//		System.out.println(unexecuteFilePaths);
//		System.out.println(needMoveFilePaths);
		
		File files = new File(unexecuteFilePaths);
		for(File tempfile : files.listFiles()) {
			String everyFileName = tempfile.getName();
			if(everyFileName.substring(0,1).equals("U")){
				File file = new File(unexecuteFilePaths+everyFileName);
				file.renameTo(new File(needMoveFilePaths+everyFileName));
			}
		}
		
		logger.debug("Have success backup file to backup folder");
//		String backupFilePath = xs.getConfigInfo().get("needGenerateFile").get("BackFilePath")+yesterdayDate;
//		File file = new File(unexecuteFilePath);
//		if (file.exists()) {
//			file.renameTo(new File(backupFilePath));
//			logger.debug("If have "+unexecuteFilePath+" backup it to " +backupFilePath);
//		}
		
		sc.close();
		logger.debug("Close the SparkContext");
		
	}
	
	/**
	 * Judge this path is executed or not (according to file of 'ExecutePath.log').
	 * @param path
	 * @return
	 */
	public static boolean JudgeTheUnexectePathIsExecuteOrNot(String path,XmlSchema xs){
		
		int unExecutePathLength = path.length();
		
		if(unExecutePathLength>15){
			
			String yearName = path.substring(unExecutePathLength-15, unExecutePathLength-5);
			
			String fileName = xs.getConfigInfo().get("needGenerateFile").get("ExecuteFilePath");
			
			File file = new File(fileName+yearName);
			
			if(file.exists()){
				
				try {
					
					BufferedReader	br = new BufferedReader(new FileReader(file));
					String line = null;
					
					if (br.ready()) {
						while ((line = br.readLine()) != null) {
							if(line.trim().equals(path)){
								return false;
							}
						}
					}
					
				} catch (Exception e) {
					logger.error("When use the BufferedReader appear error message is :" + e.getMessage());
				}
				
			}
			
			return true;
			
		} else {
			
			return false;
			
		}
		
	}
	
	/**
	 * 
	 * @param listStamps
	 * @param stampValue
	 * @return
	 */
	public static void findThisBatchAllTheStampAppearTwice(List<String> listStamps,String stampValue,List<String> needStampsValue){
		
		int stampCount=0;
		
		for(String stamp:listStamps){
			if(stampValue.equalsIgnoreCase(stamp)){
				stampCount+=1;
			}
		}
		
		if(stampCount>1){
			needStampsValue.add(stampValue);
		}
		
	}
	
	/**
	 * get all the distinct stamps.
	 * @param repetitionStamps
	 * @return
	 */
	public static List<String> getDistinctStampValue(List<String> repetitionStamps){
		
		List<String> listDistinctStamps = new ArrayList<String>();
		
		Map<String,String> mapDistinctStamps= new HashMap<String,String>();
		
		for(String source : repetitionStamps){
			
			if(source == null || source.trim().equals(Constants.EMPTYSTRING) || source.trim().equals(Constants.KAHUNA_VALUE)){
				return null;
			}
			
			//convert this little file every line to a array.
			String[] lines = source.split(Constants.NEWLINE);
			
			String stampValue = null;
			
			for(String line : lines){
				// if this line is empty,remove this line.
				if (line.trim().equals("") || line == null ) {
					continue;
				}
				
				String keyword = line.replaceFirst("^(\\w+):.*", "$1").trim();
				
				if(keyword.equalsIgnoreCase(Constants.STAMP_SIGN)){
					stampValue = line.replaceFirst("^(\\w+):(.*)", "$2").trim();
				}
				
			}
			
			if(stampValue!=null&&!mapDistinctStamps.containsKey(stampValue)){
				mapDistinctStamps.put(stampValue, source);
			}
			
		}
		
		mapDistinctStamps.values().forEach(source -> listDistinctStamps.add(source));
		
		return listDistinctStamps;
		
	}
	
}
