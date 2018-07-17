package com.hp.it.test;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.hp.it.transformers.Constants;

public class TestClass {

	static Logger logger = Logger.getLogger(TestClass.class);

	public static void main(String[] args) {

		PropertyConfigurator.configure(Constants.LOG4J_FILENAME);

		logger.debug("Create the SparkConf");
		SparkConf sparkConf = new SparkConf().setAppName("Spark BigGB Generate");
		logger.debug("Success create the SparkConf");
		
		// To get this to work when running in Windows
		if (System.getProperty("os.name").toLowerCase().indexOf("win") >= 0) {
			sparkConf.setMaster("local[*]");
			File workaround = new File(".");
			System.getProperties().put("hadoop.home.dir", workaround.getAbsolutePath());
		}

		logger.debug("Begin to created the JavaSparkContext");
		JavaSparkContext sc = new JavaSparkContext(sparkConf);
		logger.debug("Success created the JavaSparkContext");

		List<String> listpath = getAllTheFSPathNeedToConvertToRDD(args[0]);
		logger.debug("Finished get all the paths");

		List<String> stampValuesList = getAllTheStampValues(args[1]);
		logger.debug("Finished get all the stamp values");

		if (!listpath.isEmpty()) {

			if (stampValuesList.isEmpty()) {
				return;
			}

			File filepath = new File(args[2]);
			BufferedWriter out = null;
			try {
				if (!filepath.exists()) {

					filepath.createNewFile();
					out = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(filepath, true)));

				}

				logger.debug(" Begin to create the class of 'Configuration' to read bz2 file ");
				Configuration conf = new Configuration();
				conf.set("textinputformat.record.delimiter", "eod: tester");

				for (String path : listpath) {

					if (!stampValuesList.isEmpty()) {

						JavaPairRDD<LongWritable, Text> _sourceRDD = sc.newAPIHadoopFile(path, TextInputFormat.class,
								LongWritable.class, Text.class, conf);

						JavaRDD<String> sourceRDD = _sourceRDD.values().map(source -> source.toString())
								.map(new TestValidation()).filter(source -> source != null);

						List<String> listCurrentSourceRDD = sourceRDD.collect();
						
						List<String> listTemp = new ArrayList<String>();
						for (String stamp:stampValuesList) {
							if (listCurrentSourceRDD.contains(stamp)) {
								out.write(stamp + "#" + path + "\n");
								listTemp.add(stamp);
							}
						}
						stampValuesList.removeAll(listTemp);
						
						
					} else {
						break;
					}

				}
				out.flush();
			} catch (IOException e) {
				e.printStackTrace();
			} finally {
				if (out != null) {
					try {
						out.close();
					} catch (IOException e) {
						logger.error("When close the FileStream appear error is :" + e.getMessage());
					}
				}
			}

		}
	}

	/**
	 * Get all the path From needReadWhichDate
	 * 
	 * @param needReadWhichDate
	 * @return
	 */
	public static List<String> getAllTheFSPathNeedToConvertToRDD(String needReadWhichDate) {
		// read all the path and put it into a list
		List<String> listpath = new ArrayList<String>();
		File filepath = new File(needReadWhichDate);
		BufferedReader br = null;
		String line = null;

		try {
			br = new BufferedReader(new FileReader(filepath));
			if (br.ready()) {
				while ((line = br.readLine()) != null) {
					listpath.add(line);
				}
			}

		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return listpath;
	}

	/**
	 * Get all the stamp value from
	 * 
	 * @param stampValueFile
	 * @return
	 */
	public static List<String> getAllTheStampValues(String stampValueFile) {

		List<String> stampValuesList = new ArrayList<String>();

		File filepath = new File(stampValueFile);

		BufferedReader br = null;

		String line = null;

		try {
			br = new BufferedReader(new FileReader(filepath));
			if (br.ready()) {
				while ((line = br.readLine()) != null) {
					stampValuesList.add(line);
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return stampValuesList;
	}

}