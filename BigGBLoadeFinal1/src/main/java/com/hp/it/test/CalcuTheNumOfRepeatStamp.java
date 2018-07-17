package com.hp.it.test;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import org.apache.log4j.Logger;

public class CalcuTheNumOfRepeatStamp {
	
	static Logger logger = Logger.getLogger(CalcuTheNumOfRepeatStamp.class);	
	
	public static void calcuTheRepeatStampValue(Long value,List<String> list,List<String> listForLogRepetitionData){
		
		File filevalue = new File("/opt/apps/MANF/Gradebook/logs/CountLoader.log");
		
		BufferedWriter out = null;
		
		listForLogRepetitionData.retainAll(list);
		
		try {
			if (!filevalue.exists()) {
				filevalue.createNewFile();
			}
			
			out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filevalue, true)));
			
			Date date = new Date();
			
			SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			
			String time = sdf.format(date);
			
			if (!value.toString().equals("0")){
				out.write(value.toString()+"#"+time+"\n");
			}
			
			for (String stampValue: listForLogRepetitionData){
				out.write(stampValue+"#"+time+"\n");
			}
			
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
