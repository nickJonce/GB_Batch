package com.hp.it.transformers;

import java.util.List;

import org.apache.spark.api.java.function.Function;

public class RDDMapRepeationStamp implements Function<String, String> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 4711829243094957627L;

	private List<String> stampsValueHaveAppearTwice;

	private boolean sign;

	public RDDMapRepeationStamp(List<String> _stampsValueHaveAppearTwice, boolean _sign) {
		super();
		this.stampsValueHaveAppearTwice = _stampsValueHaveAppearTwice;
		this.sign = _sign;
	}

	@Override
	public String call(String source) throws Exception {

		if (source == null || source.trim().equals(Constants.EMPTYSTRING)
				|| source.trim().equals(Constants.KAHUNA_VALUE)) {
			return null;
		}

		String[] lines = source.split(Constants.NEWLINE);

		String stampValue = null;

		for (String line : lines) {

			// if this line is empty,remove this line.
			if (line.trim().equals("") || line == null) {
				continue;
			}

			String keyword = line.replaceFirst("^(\\w+):.*", "$1").trim();

			if (keyword.equalsIgnoreCase(Constants.STAMP_SIGN)) {
				stampValue = line.replaceFirst("^(\\w+):(.*)", "$2").trim();
			}

		}

		if (sign) {

			if (stampsValueHaveAppearTwice.contains(stampValue)) {
				return source;
			} else {
				return null;
			}

		} else {

			if(!stampsValueHaveAppearTwice.contains(stampValue)){
				return source;
			}else{
				return null;
			}
			
		}
	}

}
