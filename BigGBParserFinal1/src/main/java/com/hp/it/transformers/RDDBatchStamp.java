package com.hp.it.transformers;

import org.apache.spark.api.java.function.Function;

public class RDDBatchStamp implements Function<String, String> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = -4210262583516048515L;
	
	@Override
	public String call(String source) throws Exception {
		
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
		
		if(stampValue!=null){
			return stampValue; 
		}else{
			return null;
		}

	}
	
}
