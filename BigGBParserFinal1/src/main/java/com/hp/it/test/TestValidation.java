package com.hp.it.test;

import org.apache.spark.api.java.function.Function;

import com.hp.it.transformers.Constants;

public class TestValidation implements Function<String, String> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1340678295639285332L;

	public TestValidation() {
		super();
		// TODO Auto-generated constructor stub
	}

	@Override
	public String call(String source) throws Exception {
		// TODO Auto-generated method stub
		
		if(source == null || source.trim().equals(Constants.EMPTYSTRING) || source.trim().equals(Constants.KAHUNA_VALUE)){
			return null;
		}
		
		String[] lines = source.split(Constants.NEWLINE);
		
		for(String line : lines){
			String keyword = line.replaceFirst("^(\\w+):.*", "$1").trim();
			if(keyword.equals("stamp")){
				return line.replaceFirst("^(\\w+):(.*)", "$2").trim();
			}
		}

		return null;
	}
	
}
