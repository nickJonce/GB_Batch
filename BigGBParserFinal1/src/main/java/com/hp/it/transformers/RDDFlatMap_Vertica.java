package com.hp.it.transformers;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.spark.api.java.function.FlatMapFunction;

public class RDDFlatMap_Vertica implements FlatMapFunction<String, String>{

	/**
	 * 
	 */
	private static final long serialVersionUID = 6011757198662267032L;

	public RDDFlatMap_Vertica() {
		super();
		// TODO Auto-generated constructor stub
	}
	
	public Iterator<String> call(String lines) throws Exception {
		// TODO Auto-generated method stub
		List<String> list = new ArrayList<String>();
		if (lines.trim().length() == 0) {
			return list.iterator();
		}
		String[] eTestArray = lines.split("\n");

		for (String eTestLine : eTestArray) {
			if (eTestLine.equals("") || eTestLine == null) {
				continue;
			}
			list.add(eTestLine);
		}
		return list.iterator();
	}

}
