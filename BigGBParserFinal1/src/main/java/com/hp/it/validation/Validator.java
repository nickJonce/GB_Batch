package com.hp.it.validation;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.spark.api.java.function.Function;

import com.hp.it.transformers.Constants;
import com.hp.it.utils.db.ColumnMetaData;
import com.hp.it.utils.db.Table;
import com.hp.it.utils.db.XmlSchema;

public class Validator implements Function<String, String> {
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 8488260868340501735L;
	
	private XmlSchema xmlSchema;
	private String currentTime;
	
	public Validator(XmlSchema _xmlSchema,String _currentTime) {
		super();
		this.xmlSchema = _xmlSchema;
		this.currentTime = _currentTime;
	}
	
	/**
	 * this function contain three step 
	 * Step 1: Validation.
	 * Step 2: Generate new little file value.
	 * Step 3: If file is illegal generate this value to some place and return null, else return this new value.
	 */
	@Override
	public String call(String source) throws Exception {
		
		if(source == null || source.trim().equals(Constants.EMPTYSTRING) || source.trim().equals(Constants.KAHUNA_VALUE)){
			return null;
		}
		
		/**
		 * Record each small file properties and init those attribute.
		 */
		// If validation fails, flag = true and return null
		boolean validation_fail = false;
		
		// contain all row of the first value in front of the ':'.
		List<String> listKeyword = new ArrayList<String>();
		
		// contain all row of the first value after the ':'.
		List<String> listValue = new ArrayList<String>();
		
		// the value must be one of the 'raw_pipe','raw','noraw'.
		String rawloadervalue = null;
		
		// Value of stamp to calculate the partition_dm value
		String stamp = null;
		
		//according to test_type value to determine file type
		String test_type = null;
		
		// if partitioningFromcol is not equal with stamp use this field to
		// calculate the partition_dm value
		String partitioningFromcolValue = null;
		
		// convert the stamp or other value into partiton_dm
		String partitionDm = null;
		
		int errorNum = 0;  // if the file is illegal record which line is error.
		
		String errorFolderName = null;  // the errorFolderName get from config.properties.
		
		String scrambledCodeFirstValue = null;
		
		/**
		 * Step 1: Validation the every file value and if it is illegal , put
		 * the switch of flag value is ture and don't deal with it Direct return
		 * null
		 */
		String[] lines = source.split(Constants.NEWLINE);  // split this little file info by "/n"
		
		for (String line : lines) {
				
				// record which line is error.
				if (!validation_fail) {
					errorNum = errorNum + 1;
				}
				
				// if this line is empty,remove this line.
				if (line.trim().equals("") || line == null ) {
					continue;
				}
				
				if(scrambledCodeFirstValue==null){
					scrambledCodeFirstValue=line;
				}
				
				// judge whether every line is contain :
				if(!line.contains(":")){
					validation_fail = true;
					errorFolderName = xmlSchema.getConfigInfo().get("errorFileName").get("ERROR_PerFileDoNotHaveSemicolon");
					
					if(scrambledCodeFirstValue!=null){
						if(scrambledCodeFirstValue.length()>30){
							stamp = scrambledCodeFirstValue.substring(0,26);
						}else{
							stamp = scrambledCodeFirstValue;
						}
					}
					
//					Pattern pattern = Pattern.compile("@^C(.*)^@Ì");
//					Matcher matcher = pattern.matcher(line);
//					while(matcher.find()){
//						stamp = matcher.group(1);
//					}
					
					break;
				}
				
				// before frist ":" value is keyword,after frist ":" is value
				String keyword = line.replaceFirst("^(\\w+):.*", "$1").trim();
				String value = null;
				if (xmlSchema.getDetailTablesInfo().containsKey(keyword)) {
					value = line.replaceFirst("^(\\w+):(.*)", "$2");
				} else {
					value = line.replaceFirst("^(\\w+):(.*)", "$2").trim();
				}
				
				/**
			     * if the front line information illegally don't do those validation
				 * just put value into list
				 */
				if (!validation_fail) {
					//<1> validation:To determine whether the keyword combination by characters or Numbers,keyword must match "[A-Za-z][A-Za-z_0-9/\\s]*".
					if (!Pattern.compile("[A-Za-z][A-Za-z_0-9/\\s]*").matcher(keyword).matches()) {
						validation_fail = true;
						errorFolderName = xmlSchema.getConfigInfo().get("errorFileName").get("ERROR_IllegalCharacter");
						continue;
					}
					//<2> validation: If masterTable keyword appear twice if the value is not same filter this file.
					if (!listKeyword.isEmpty() && listKeyword.contains(keyword)
							&& !xmlSchema.getDetailTablesInfo().keySet().contains(keyword)) {
						if (!listKeyword.get(listKeyword.indexOf(keyword)).toString().equalsIgnoreCase(value.toString())) {
							validation_fail = true;
							errorFolderName = xmlSchema.getConfigInfo().get("errorFileName").get("ERROR_AppearTwice");
							continue;
						}
					}
					//<3> validation: Judge the keyword 'raw_loader' value is right or not.
					if (keyword.equalsIgnoreCase("raw_loader")) {
						if (!(value.equalsIgnoreCase("raw_pipe") || value.equalsIgnoreCase("noraw")
								|| value.equalsIgnoreCase("raw"))) {
							validation_fail = true;
							errorFolderName = xmlSchema.getConfigInfo().get("errorFileName").get("ERROR_ValueOfRawLoaderIncorrect");
							continue;
						} else {
							rawloadervalue = value;
						}
					}

				}
				
				// if keyword equals with the dataPartitioningFromcol(value from GBConfig.xml generate is 'stamp') save this value for next step.
				if (xmlSchema.getDataPartitioningFromcol().equalsIgnoreCase(keyword)) {
					if (keyword.equalsIgnoreCase("stamp")) {
						stamp = value;
					} else {
						partitioningFromcolValue = value;
					}
				}
				//<6> validtion if this file is not Etest file filter this file
				if (keyword.equalsIgnoreCase("test_type")) {
					test_type = value.trim();
				}
				//put all 'keyword' and 'value' into List for Validtion.
				listKeyword.add(keyword);
				listValue.add(value);
		}
		
		//<7> validation : this little file must contain all four keyword 'database','raw_loader','test_type','stamp' if don't filter this little file.Begin
		if (!validation_fail && !listKeyword.isEmpty() && !(listKeyword.contains("database") && listKeyword.contains("raw_loader")
				&& listKeyword.contains("test_type") && listKeyword.contains("stamp"))) {
			errorFolderName = xmlSchema.getConfigInfo().get("errorFileName").get("ERROR_TableNotContentAllFourKeyword");
			validation_fail = true;
		}
		
		//<7> validation : this little file must contain all four keyword 'database','raw_loader','test_type','stamp' if don't filter this little file.End
		//According to the situation get the partition_dm values
		if (!validation_fail) {
			SimpleDateFormat partitionDmFormat = new SimpleDateFormat(xmlSchema.getDataPartitioningDatepattern());
			if (xmlSchema.getDataPartitioningCol().equals("")||xmlSchema.getDataPartitioningFromcol().isEmpty()) {
				// if DataPartitioningCol value is '' partition_dm set current time
				partitionDm = partitionDmFormat.format(Calendar.getInstance().getTime());
			} else if (xmlSchema.getDataPartitioningFromcol().equals("stamp")) {
				// if DataPartitioningCol value is stamp convert stamp to the partition_dm
				if (stamp != null && stamp.trim() != "") {
					partitionDm = partitionDmFormat.format(ValidationFunction.ConvertStampToPartitionDm(stamp));
				} else {
					validation_fail = true;
					errorFolderName = xmlSchema.getConfigInfo().get("errorFileName").get("ERROR_StampValueIsNull");
				}
			} else {
				//if the DataPartitioningCol is not null or not equals with "stamp" judege partitioningFromcolValue value.
				if (partitioningFromcolValue == null) {
					/** 
					 * in front of the verification through then do this validation
					 * If the value of dataPartitioningFromcol in xml is not empty,not
					 * equals with "stamp",and per file can't find dataPartitioningFromcol
					 * value, the file is illegal
					 */
					errorFolderName = xmlSchema.getConfigInfo().get("errorFileName").get("ERROR_PartitionFromcolValueNotFindInLittleFile");
					validation_fail = true;
				}else{
					// partitioningFromcolValue have value put this values into partitionDm
					partitionDm = partitioningFromcolValue;
				}
			}
		}
		
		//Add validation if this little file , test_type is null.
		if(test_type==null&&!validation_fail){
			errorFolderName = xmlSchema.getConfigInfo().get("errorFileName").get("ERROR_TestTypeIsNull");
			validation_fail = true;
		}
		
		/**
		 * Step 2:
		 * 
		 * After the validation if the switch of flag is false put the table
		 * field value of stamp and partition_dm into everyline suit location
		 * such as convert element of string into new string
		 * 
		 * database: mfg 
		 * raw_loader: RAW_PIPE 
		 * test_type: elecj_set 
		 * stamp: et01hthd.b04_12 
		 * exprmt: ETEST 
		 * station_id: et12 
		 * prog_rev: 3.3.2.11.6Test 6T 
		 * elecj_pad: |et11oiesed.b25_15|2648301000106353|1927|4127|4124|3|2688.26|
		 * elecj_detail: |et11oiesed.b25_15|2648301000106353|1927|00100|Arch_ID1|2108|
		 * 
		 * into
		 * 
		 * elecj_set:et01hthd.b04_12|et11oiesed.b25_15|2648301000106353|1927|4127|4124|3|2688.26|2016-01-04
		 * elecj_pad:et01hthd.b04_12|et11oiesed.b25_15|2648301000106353|1927|4127|4124|3|2688.26|2016-01-04
		 * elecj_detail:et01hthd.b04_12|et11oiesed.b25_15|2648301000106353|1927|00100|Arch_ID1|2108|2016-01-04
		 */
		

		// the new string value need to be return
		String processNewSource = null;
		
		// if validation is through,generate the new string
		if (!validation_fail) {
		// create a masterlist to store every file master columns value info
//		List<String> listMaster = new ArrayList<String>(xmlSchema.getMasterTablesInfo().get(test_type).getColumns().size()); 
		List<String> listMaster = new ArrayList<String>();
		String masterTableName = null;
		for(Table table:xmlSchema.getMasterTablesInfo().values()){
			//TODO
			if(test_type.equals(table.getTestType().trim())){
				masterTableName=table.getTableName();
			}
		}
		
		xmlSchema.getMasterTablesInfo().get(masterTableName).getColumns().forEach(column -> listMaster.add(null));
		
		// new joining together master table info
		StringBuffer sbmaster = new StringBuffer(); 
		
		// new joining together detail table info
		StringBuffer sbdetail = new StringBuffer(); 
		
	
			//loop this little file value and generate every lines info.
			for (String line : lines) {
				//if this line is empty filter this line
				if (line.equals("") || line == null) {
					continue;
				}
				
				// before frist ":" value is keyword,after frist ":" is value
				String keyword = line.replaceFirst("^(\\w+):.*", "$1").trim();
				String value = null;
				if (xmlSchema.getDetailTablesInfo().containsKey(keyword)) {
					value = line.replaceFirst("^(\\w+):(.*)", "$2");
				} else {
					value = line.replaceFirst("^(\\w+):(.*)", "$2").trim();
				}
				
				
				//<4> judge value of validation is right or not.
				if (xmlSchema.getDetailTablesInfo().keySet().contains(keyword) && !rawloadervalue.equalsIgnoreCase("noraw")) {
					//<4> validation: validation this detail table is contain column 'partition_dm' or not.
					if (ValidationFunction.VerdictPartitionDMIsExit(xmlSchema,keyword)) {
						validation_fail = true;
						errorFolderName = xmlSchema.getConfigInfo().get("errorFileName").get("ERROR_NotFoundPartionDmOnTable");
						continue;
					}
				}
				
				//<8> validation: Judge detail table columns  size  whether is equals with  size of file per line deatil info value ,Just work on detail info of file
				//get the raw_loader value for validation
				if(rawloadervalue!=null&&xmlSchema.getDetailTablesInfo().keySet().contains(keyword)){
					if(rawloadervalue.equalsIgnoreCase("raw_pipe")){
						char sign = '|';
						int number = ValidationFunction.CountNumber(value, sign);
						if (number != xmlSchema.getDetailTablesInfo().get(keyword).getColumns().size()) {
							validation_fail = true;
							errorFolderName = xmlSchema.getConfigInfo().get("errorFileName").get("ERROR_DetailColumnNumIncorrect");
							continue;
						}
					}
					if(rawloadervalue.equalsIgnoreCase("raw")){
						char sign = ' ';
						int number = ValidationFunction.CountNumber(value, sign);
						if (number != xmlSchema.getDetailTablesInfo().get(keyword).getColumns().size()) {
							validation_fail = true;
							errorFolderName = xmlSchema.getConfigInfo().get("errorFileName").get("ERROR_DetailColumnNumIncorrect");
							continue;
						}
					}
				}

				//if this line keyword is detail table name get into and generate new detail line,else generate master line info.
				if (xmlSchema.getDetailTablesInfo().keySet().contains(keyword)
						&& (rawloadervalue.equalsIgnoreCase("raw_pipe") || rawloadervalue.equalsIgnoreCase("raw"))) {
					//init the stamp and partition_dm positition value.
					int stampPosition = 0;
					int partitionDmPosition = 0;
					
					//get column positition of 'stamp','partition_dm' convince for generate new value.
					List<ColumnMetaData> detailColumnList = xmlSchema.getDetailTablesInfo().get(keyword).getColumns();
					for (int i = 0; i < detailColumnList.size(); i++) {
						if (detailColumnList.get(i).getColumnName().equalsIgnoreCase("stamp")) {
							stampPosition = detailColumnList.get(i).getOrdinalPosition();
						}
						if (detailColumnList.get(i).getColumnName().equalsIgnoreCase(xmlSchema.getDataPartitioningCol())) {
							partitionDmPosition = detailColumnList.get(i).getOrdinalPosition();
						}
					}
					
					//get the new line value
					String newValue = ValidationFunction.AcquireNewString(stampPosition, partitionDmPosition, value, detailColumnList, partitionDm,
							stamp, rawloadervalue);
					
					//append all the detail new line info
//					sbdetail.append(keyword + ": " + newValue + "\n");
					sbdetail.append(xmlSchema.getDetailTablesInfo().get(keyword).getTableName()+ ": " + newValue + "\n");
					
				}else{
					List<String> masterTableColumnsName = new ArrayList<String>();
					xmlSchema.getMasterTablesInfo().get(masterTableName).getColumns().forEach(ele -> masterTableColumnsName.add(ele.getColumnName()));
					//put all the master column info into listMaster
					if (masterTableColumnsName.contains(keyword)) {
						for (int i = 0; i < masterTableColumnsName.size(); i++) {
							// set the field value into listMaster
							if (xmlSchema.getMasterTablesInfo().get(masterTableName).getColumns().get(i).getColumnName()
									.equalsIgnoreCase(keyword)) {
								//Need execute the special ASCII characters.
								if(value.equals("\\")){
									listMaster.set(i,"\\\\");
								}else{
									listMaster.set(i, value);
								}
								
							}
							// set the field of data_source_id_fg into generate
							// master table file begin
							if (xmlSchema.getMasterTablesInfo().get(masterTableName).getColumns().get(i).getColumnName()
									.equalsIgnoreCase("data_source_id")) {
								if (xmlSchema.getData_source_id_fg()) {
									listMaster.set(i, xmlSchema.getData_source_id());
								}
							}
							// set the field of data_source_id_fg into generate
							// master table file end
							if (xmlSchema.getMasterTablesInfo().get(masterTableName).getColumns().get(i).getColumnName()
									.equalsIgnoreCase(xmlSchema.getDataPartitioningCol())) {
								listMaster.set(i, partitionDm);
							}
							// set value into mastertable column 'insert_dm' and
							// 'update_dm' value
							
							if (xmlSchema.getMasterTablesInfo().get(masterTableName).getColumns().get(i).getColumnName()
									.equalsIgnoreCase("INSERT_DM")) {
								listMaster.set(i,currentTime);
							}
							if (xmlSchema.getMasterTablesInfo().get(masterTableName).getColumns().get(i).getColumnName()
									.equalsIgnoreCase("UPDATE_DM")) {
								listMaster.set(i,currentTime);
							}
							
						}
					}
								
				}
		
			}
			
			//generate master new line info,begin
			sbmaster.append(xmlSchema.getMasterTablesInfo().get(masterTableName).getTableName() + ": ");
			for (int i = 0; i < listMaster.size(); i++) {
				sbmaster.append((listMaster.get(i) == null ? "" : listMaster.get(i)) + "|");
			}
			String MasterInfo = sbmaster.toString();
			MasterInfo = MasterInfo.substring(0, MasterInfo.length() - 1);
			//generate master new line info,end
			
			//generate the new little file values begin
			StringBuffer sbMaster = new StringBuffer();
			String newSource = sbMaster.append(MasterInfo).append("\n").append(sbdetail).toString();
			processNewSource = ValidationFunction.RemoveEmptyLine(newSource);
			//generate the new little file values end
			
			
		}
		
	
		/**
		 * Step 3: if the flag is false return the new string that generate by
		 * the step 2
		 */
		if (!validation_fail) {
			return processNewSource;
		} else {
			Configuration conf = new Configuration();
			conf.addResource(new Path(xmlSchema.getConfigInfo().get("needGenerateFile").get("HDFSCONF_PATH")));
			FileSystem hdfs = FileSystem.get(conf);
			String fileerror = xmlSchema.getConfigInfo().get("needGenerateFile").get("SUM_ERRORFILENAME");
			Path hdfsfileerror = new Path(fileerror);
			boolean ifexit = hdfs.exists(hdfsfileerror);
			String file = null;
			FSDataOutputStream fout = null;
			
			if (stamp != null) {
				if (stamp.trim() != "") {
					file = xmlSchema.getConfigInfo().get("needGenerateFile").get("ERROR_FILEPATH") + errorFolderName + "/" + stamp + ".txt";
				} else {
					file = xmlSchema.getConfigInfo().get("needGenerateFile").get("ERROR_FILEPATH") + errorFolderName + "/" + "StampValueIsNull.txt";
				}
			} else {
				file = xmlSchema.getConfigInfo().get("needGenerateFile").get("ERROR_FILEPATH") + errorFolderName + "/" + "StampValueIsNull.txt";
			}
			
			if (!ifexit) {
				fout = hdfs.create(hdfsfileerror, false);
			} else {
				fout = hdfs.append(hdfsfileerror);
			}
			
			String errorSource = "errorFileName: " + stamp + ".txt" + " ;  errorFilePath: " + file
					+ " ;  errorMassage: " + errorFolderName + " ;  errorRow: " + errorNum + "\n";
			fout.write(errorSource.getBytes());
			fout.close();
			Path hdfsFile = new Path(file);
			FSDataOutputStream out = null;
			
			if(!hdfs.exists(hdfsFile)){
				out = hdfs.create(hdfsFile);
			}else{
				out = hdfs.append(hdfsFile);
			}
			
			out.write((source + "\n").getBytes());
			out.close();
			
			return null;
			
		}


	}

}
