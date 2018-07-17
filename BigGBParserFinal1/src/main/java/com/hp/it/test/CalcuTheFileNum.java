package com.hp.it.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.List;

import org.apache.log4j.Logger;

public class CalcuTheFileNum {

	static Logger logger = Logger.getLogger(CalcuTheFileNum.class);	
	
	public static void generateTheFileForCount(List<String> listpath,String num){
		
		File filevalue = new File("/opt/apps/MANF/Gradebook/logs/CountParser.log");
		
		BufferedWriter out = null;
		
		try {
			if (!filevalue.exists()) {
				filevalue.createNewFile();
			}
			
			out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filevalue, true)));
			
			for(String path : listpath){
				out.write(path+"&");
			}
			
			out.write("#"+num+"\n");
			out.flush();
			
		} catch (IOException e) {
			logger.error("Unsuccess recode the count of folder error message is :"+e.getMessage());
		}finally{
			if (out!=null){
				try {
					out.close();
				} catch (IOException e) {
					logger.error("When colse the IO appear error :" +e.getMessage());
				}
			}
		}
		
	}
	
}