package com.hp.it.transformers;

import java.util.List;

import org.apache.spark.api.java.function.Function;

public class RDDRepetitionStamp implements Function<String, Boolean>{
	
	private static final long serialVersionUID = -7826571150460409953L;
	
	private List<String> stampsValueHaveAppearTwice;
	
	private Boolean flag;
	
	public RDDRepetitionStamp(List<String> _stampsValueHaveAppearTwice,Boolean _flag) {
		this.stampsValueHaveAppearTwice=_stampsValueHaveAppearTwice;
		this.flag=_flag;
	}
	
	@Override
	public Boolean call(String source) throws Exception {
		
		if(source == null || source.trim().equals(Constants.EMPTYSTRING) || source.trim().equals(Constants.KAHUNA_VALUE)){
			return null;
		}
		
		String[] lines = source.split(Constants.NEWLINE); 
		
		String stampValue = null;
		
		for (String line : lines) {
			
			// if this line is empty,remove this line.
			if (line.trim().equals("") || line == null ) {
				continue;
			}
			
			String keyword = line.replaceFirst("^(\\w+):.*", "$1").trim();
			
			if(keyword.equalsIgnoreCase(Constants.STAMP_SIGN)){
				stampValue = line.replaceFirst("^(\\w+):(.*)", "$2").trim();
			}
			
		}
		
		if(stampValue!=null){
			if(stampsValueHaveAppearTwice.contains(stampValue)){
				return flag;
			}else{
				return !flag;
			}
		}else{
			return false;
		}
	
	}
	
}
