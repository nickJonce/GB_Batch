package com.hp.it.test;

import java.io.IOException;

import org.apache.avro.generic.GenericRecord;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.avro.AvroParquetReader;

public class TestReadParquetFileByUseJava {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) {
		
		String input = args[0];
		
	    AvroParquetReader<GenericRecord> reader =null;
		try {
			reader = new AvroParquetReader<GenericRecord>(new Path(input));
	        GenericRecord record ;
	        while ((record = reader.read())!= null){
	            System.out.println(record);
	        }
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}finally{
			try {
				if(reader!=null){
					reader.close();
				}
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	    
		
	}

}
