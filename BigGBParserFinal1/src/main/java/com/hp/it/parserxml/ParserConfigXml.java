package com.hp.it.parserxml;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.log4j.Logger;

import com.hp.it.utils.db.Loader;
import com.hp.it.utils.db.XmlSchema;

public class ParserConfigXml {
	
	static Logger logger = Logger.getLogger(ParserConfigXml.class);	
	
	public static XmlSchema getAllTheInfoFromConfigXml(){
		
//		InputStream inputStream = ParserConfigXml.class.getClassLoader().getResourceAsStream("GBConfig.xml");
		
		InputStream inputStream = null;
		
		String loadContent =null;
		
		XmlSchema xmlSchema = new XmlSchema();
		
		Loader loader = new Loader();
		
		try {
			
			inputStream = new FileInputStream("/opt/apps/MANF/Gradebook/resources/GBConfig.xml");
			
			loadContent = inputStream2String(inputStream);
			
			xmlSchema = loader.loadData(loadContent);
			
		} catch (IOException e) {
			logger.error("When parser ConfigXml on class ParserConfigXml appear error:"+e.getMessage());
		}
		
		return xmlSchema;
		
	}
	
	public static String inputStream2String(InputStream in) throws IOException {
		StringBuffer out = new StringBuffer();
		byte[] b = new byte[4096];
		for (int n; (n = in.read(b)) != -1;) {
			out.append(new String(b, 0, n));
		}
		return out.toString();
	}
	
}
