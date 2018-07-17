package com.hp.it.test;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.Locale;
import java.util.TimeZone;

public class Test {

	public static void main(String[] args) {
		
		TimeZone timeZone = TimeZone.getTimeZone("GMT+8:00");
		SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");  
		simpleDateFormat.setTimeZone(timeZone);
		System.out.println(simpleDateFormat.format(new Date()));
		
//		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//		dateFormat.setTimeZone(TimeZone.getTimeZone("GMT-8"));
		
//		Calendar cal = Calendar.getInstance();
//		System.out.println(cal);
//		
//		TimeZone zone = TimeZone.getTimeZone("GMT+8:00");
//		Calendar caltest = Calendar.getInstance(zone);
//		System.out.println(caltest);
		
//		ZoneId shanghaiTime = ZoneId.of("Asia/Shanghai");
//		America/Phoenix
//		ZoneId zone = ZoneId.of("America/Phoenix");
//		LocalDateTime localDateAndTime = LocalDateTime.now();
//		ZonedDateTime finalDateTime = ZonedDateTime.of(localDateAndTime,zone);
		
//		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
//		System.out.println(finalDateTime.format(formatter));
		
//		  ZoneId zone = ZoneId.of("Europe/Paris");
//        LocalDateTime date = LocalDateTime.now();
//        ZonedDateTime zdt1 = ZonedDateTime.of(date,zone);
//        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
//        System.out.println("Zone in Europe/Paris,start of the day:" + zdt1.format(formatter));
		
//		 Calendar calendar = Calendar.getInstance(Locale.CHINA);
		
//		Calendar calendar = Calendar.getInstance();
//	    Date date = calendar.getTime();
//	    SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
//	    String dateString = dateFormat.format(date);
//	    System.out.println(dateString);
		
	}
//	public static void main(String[] args) {
//
//		
//		XmlSchema xmlSchema = ParserConfigXml.getAllTheInfoFromConfigXml();
//		
//		String source1 = "test_type: print_sample_suite" + Constants.NEWLINE + "exprmt: SRAYEOLAUDIT001"
//				+ Constants.NEWLINE + "firmware_version_tx:  " + Constants.NEWLINE + "lifetime_printer_page_ct:  "
//				+ Constants.NEWLINE + "operator_id: LJIE" + Constants.NEWLINE + "plot_cd: 1" + Constants.NEWLINE
//				+ "prgm_rev: 5.3" + Constants.NEWLINE + "print_dm: 2018-01-03 13:16:12" + Constants.NEWLINE
//				+ "print_sample_id: k1bt118103kluyj" + Constants.NEWLINE + "print_sample_type_tx: Garnet"
//				+ Constants.NEWLINE + "print_file_nm:  " + Constants.NEWLINE + "printer_serial_nr_tx: "
//				+ Constants.NEWLINE + "printer_suite_ct: 0" + Constants.NEWLINE + "reason_cd:  " + Constants.NEWLINE
//				+ "source_cd: Pwax 1" + Constants.NEWLINE + "station_id: MR08" + Constants.NEWLINE
//				+ "suite_id: EOL_suite" + Constants.NEWLINE + "test_req_id: WK01" + Constants.NEWLINE
//				+ "print_sample_pen_dtl: |k1bt118103kluyj|1|1|2648301000580577|1|2|" + Constants.NEWLINE
//				+ "print_sample_data: |k1bt118103kluyj|elapsed_time||||" + Constants.NEWLINE
//				+ "print_sample_pen_data: |k1bt118103kluyj|1|1|Pulsewidth_0_K|.767|||" + Constants.NEWLINE
//				+ "print_sample_pen_data: |k1bt118103kluyj|1|1|Pulsewidth_0_C|.564|||" + Constants.NEWLINE
//				+ "stamp: k1bt_180103_klwvq" + Constants.NEWLINE + "raw_loader: RAW_PIPE" + Constants.NEWLINE
//				+ "database: mfg";
//		
//		
//		String source = "database:                       mfg																													" + Constants.NEWLINE + 
//				"print_sample_id:                k1bs118103mfqxk																												" + Constants.NEWLINE + 
//				"analysis_dm:                    2018-01-03 15:34:22.000																										" + Constants.NEWLINE + 
//				"stamp:                          piuq000_180103_mgijx																										 	" + Constants.NEWLINE + 
//				"test_type:                      pwst_sample																													" + Constants.NEWLINE + 
//				"raw_loader:                     RAW_PIPE																						" + Constants.NEWLINE + 
//				"test_start_dm:                  2018-01-03 15:32:28			" + Constants.NEWLINE + 
//				"station_id:                     piuq                                                                                                                          " + Constants.NEWLINE + 
//				"exprmt:                         NONE                                                                                                                          " + Constants.NEWLINE + 
//				"test_req_id:                    NONE                                                                                                                          " + Constants.NEWLINE + 
//				"prgm_rev:                       v4.16                                                                                                                         " + Constants.NEWLINE + 
//				"operator_id:                    \\                                                                                                                             " + Constants.NEWLINE + 
//				"customer_id:                    NONE                                                                                                                          " + Constants.NEWLINE + 
//				"media_tx:                       HP Multipurpose                                                                                                               " + Constants.NEWLINE + 
//				"scanner_serial_nr:              NOT SUPPORTED                                                                                                                 " + Constants.NEWLINE + 
//				"printer_serial_nr:              NONE                                                                                                                          " + Constants.NEWLINE + 
//				"printer_id:                     NONE                                                                                                                          " + Constants.NEWLINE + 
//				"print_file_nm:                  Motivator Garnet 1.0                                                                                                          " + Constants.NEWLINE + 
//				"instr_file_path_nm:             C:\\Inspector 4.16\\PWax\\InstructionFiles\\Motivator_Stingray_Garnet_NH_1.0_A.inst.xml                                           " + Constants.NEWLINE + 
//				"part_link_id:                   0                                                                                                                             " + Constants.NEWLINE + 
//				"lot_link_id:                    0                                                                                                                             " + Constants.NEWLINE + 
//				"scan_res_x_vl:                  600.0000                                                                                                                      " + Constants.NEWLINE + 
//				"scan_res_y_vl:                  600.0000                                                                                                                      " + Constants.NEWLINE + 
//				"test_result_cd:                 1P                                                                                                                            " + Constants.NEWLINE + 
//				"comment_tx:                     SCALING                                                                                                                       " + Constants.NEWLINE + 
//				"pwst_part_detail:               |k1bs118103mfqxk|2018-01-03 15:34:22.000|11|NONE||NONE|1|1|1|                                                                 " + Constants.NEWLINE + 
//				"pwst_part_obj:                  |k1bs118103mfqxk|2018-01-03 15:34:22.000|11|0|A|14|B1M1D0A|                                                                   " + Constants.NEWLINE + 
//				"pwst_obj_meas:                  |k1bs118103mfqxk|2018-01-03 15:34:22.000|14|WF_NozHealth_Weak_50-70|B1M1D0A|NA|1|0|0|60|7|0|9|0.000000|P|||                   " + Constants.NEWLINE + 
//				"pwst_obj_meas:                  |k1bs118103mfqxk|2018-01-03 15:34:22.000|14|WF_NozHealth_Weak_50-70|B1M1D0A|NA|1|0|0|69|7|0|9|0.000000|P|||                   " + Constants.NEWLINE + 
//				"pwst_nozzle_meas:               |k1bs118103mfqxk|2018-01-03 15:34:22.000|11|14|WF_NozHealth_Weak_50-70|B1M1D0A|block6|1|0|82030|20|NA|0|9|39.515816|P|1|||    " + Constants.NEWLINE + 
//				"pwst_obj_meas:                  |k1bs118103mfqxk|2018-01-03 15:34:22.000|14|WF_NozHealth_Weak_50-70|B1M1D0A|NA|1|0|0|21|7|0|9|16.310472|NA|||                 " + Constants.NEWLINE + 
//				"pwst_obj_meas:                  |k1bs118103mfqxk|2018-01-03 15:34:22.000|14|WF_NozHealth_Weak_50-70|B1M1D0A|NA|1|0|0|22|7|0|9|3.567660|NA|||                  " + Constants.NEWLINE + 
//				"pwst_obj_meas:                  |k1bs118103mfqxk|2018-01-03 15:34:22.000|14|WF_NozHealth_Weak_50-70|B1M1D0A|NA|1|0|0|56|7|0|9|1.000000|P|||                   " + Constants.NEWLINE + 
//				"pwst_nozzle_meas:               |k1bs118103mfqxk|2018-01-03 15:34:22.000|11|14|WF_NozHealth_Weak_50-70|B1M1D0A|block6|1|0|82030|120|NA|0|9|39.515816|F|1|||   " + Constants.NEWLINE +                                 
//				"pwst_obj_meas:                  |k1bs118103mfqxk|2018-01-03 15:34:22.000|14|WF_NozHealth_Weak_50-70|B1M1D0A|NA|1|0|0|57|7|0|9|0.000000|P|||                   " + Constants.NEWLINE + 
//				"pwst_obj_meas:                  |k1bs118103mfqxk|2018-01-03 15:34:22.000|14|WF_NozHealth_Weak_50-70|B1M1D0A|NA|1|0|0|58|7|0|9|0.000000|P|||                   " + Constants.NEWLINE + 
//				"pwst_obj_meas:                  |k1bs118103mfqxk|2018-01-03 15:34:22.000|14|WF_NozHealth_Weak_50-70|B1M1D0A|NA|1|0|0|25|7|0|9|8.000000|NA|||                  " + Constants.NEWLINE + 
//				"pwst_obj_meas:                  |k1bs118103mfqxk|2018-01-03 15:34:22.000|14|WF_NozHealth_Weak_50-70|B1M1D0A|NA|1|0|0|26|7|0|9|117.195313|NA|||                " + Constants.NEWLINE + 
//				"pwst_obj_meas:                  |k1bs118103mfqxk|2018-01-03 15:34:22.000|14|WF_NozHealth_Weak_50-70|B1M1D0A|NA|1|0|0|27|7|0|9|14.649414|NA|||                 " + Constants.NEWLINE + 
//				"pwst_obj_meas:                  |k1bs118103mfqxk|2018-01-03 15:34:22.000|14|WF_NozHealth_Weak_50-70|B1M1D0A|NA|1|0|0|28|7|0|9|3.164517|NA|||                  " + Constants.NEWLINE + 
//				"pwst_obj_meas:                  |k1bs118103mfqxk|2018-01-03 15:34:22.000|14|WF_NozHealth_Weak_50-70|B1M1D0A|NA|1|0|0|29|7|0|9|1.127395|NA|||                  " + Constants.NEWLINE + 
//				"pwst_obj_meas:                  |k1bs118103mfqxk|2018-01-03 15:34:22.000|14|WF_NozHealth_Weak_50-70|B1M1D0A|NA|1|0|0|65|7|0|9|97.301819|NA|||                 " + Constants.NEWLINE + 
//				"pwst_nozzle_meas:               |k1bs118103mfqxk|2018-01-03 15:34:22.000|11|14|WF_NozHealth_Weak_50-70|B1M1D0A|block4|1|0|81459|66|NA|0|9|83.947678|NA|1|||   " + Constants.NEWLINE + 
//				"pwst_obj_meas:                  |k1bs118103mfqxk|2018-01-03 15:34:22.000|14|WF_NozHealth_Weak_50-70|B1M1D0A|NA|1|0|0|67|7|0|9|4.744013|NA|||                  " + Constants.NEWLINE + 
//				"pwst_obj_meas:                  |k1bs118103mfqxk|2018-01-03 15:34:22.000|14|WF_NozHealth_Weak_50-70|B1M1D0A|NA|1|0|0|68|7|0|9|4.047602|NA|||                  " + Constants.NEWLINE + 
//				"pwst_obj_meas:                  |k1bs118103mfqxk|2018-01-03 15:34:22.000|14|WF_NozHealth_Weak_50-70|B1M1D0A|NA|1|0|0|61|7|0|9|0.000000|P|||                   " + Constants.NEWLINE + 
//				"pwst_obj_meas:                  |k1bs118103mfqxk|2018-01-03 15:34:22.000|14|WF_NozHealth_Weak_50-70|B1M1D0A|NA|1|0|0|64|7|0|9|0.000000|P|||                   " + Constants.NEWLINE + 
//				"pwst_obj_meas:                  |k1bs118103mfqxk|2018-01-03 15:34:22.000|14|WF_NozHealth_Weak_50-70|B1M1D0A|NA|1|0|0|401|7|0|9|0.000000|P|||                  " + Constants.NEWLINE + 
//				"pwst_obj_meas:                  |k1bs118103mfqxk|2018-01-03 15:34:22.000|14|WF_NozHealth_Weak_50-70|B1M1D0A|NA|1|0|0|403|7|0|9|0.000000|P|||                  " + Constants.NEWLINE + 
//				"pwst_obj_meas:                  |k1bs118103mfqxk|2018-01-03 15:34:22.000|14|WF_NozHealth_Weak_50-70|B1M1D0A|NA|1|0|0|402|7|0|9|0.000000|P|||                  " + Constants.NEWLINE + 
//				"pwst_obj_meas:                  |k1bs118103mfqxk|2018-01-03 15:34:22.000|14|WF_NozHealth_Weak_50-70|B1M1D0A|NA|1|0|0|404|7|0|9|0.000000|P|||                  " + Constants.NEWLINE + 
//				"pwst_obj_meas:                  |k1bs118103mfqxk|2018-01-03 15:34:22.000|14|WF_NozHealth_Weak_50-70|B1M1D0A|NA|1|0|0|400|7|0|9|0.000000|P|||                  " + Constants.NEWLINE + 
//				"pwst_obj_meas:                  |k1bs118103mfqxk|2018-01-03 15:34:22.000|1|PSCG_Slot|B1M1D0A|NA|1|1|0|997|7|0|9|-1|NA|||                                      " ;
//
//		if(source == null || source.trim().equals(Constants.EMPTYSTRING) || source.trim().equals(Constants.KAHUNA_VALUE)){
//			
//		}
//		
//		/**
//		 * Record each small file properties and init those attribute.
//		 */
//		// If validation fails, flag = true and return null
//		boolean validation_fail = false;
//		
//		// contain all row of the first value in front of the ':'.
//		List<String> listKeyword = new ArrayList<String>();
//		
//		// contain all row of the first value after the ':'.
//		List<String> listValue = new ArrayList<String>();
//		
//		// the value must be one of the 'raw_pipe','raw','noraw'.
//		String rawloadervalue = null;
//		
//		// Value of stamp to calculate the partition_dm value
//		String stamp = null;
//		
//		//according to test_type value to determine file type
//		String test_type = null;
//		
//		// if partitioningFromcol is not equal with stamp use this field to
//		// calculate the partition_dm value
//		String partitioningFromcolValue = null;
//		
//		// convert the stamp or other value into partiton_dm
//		String partitionDm = null;
//		
//		int errorNum = 0;  // if the file is illegal record which line is error.
//		
//		String errorFolderName = null;  // the errorFolderName get from config.properties.
//		
//		String scrambledCodeFirstValue = null;
//		
//		/**
//		 * Step 1: Validation the every file value and if it is illegal , put
//		 * the switch of flag value is ture and don't deal with it Direct return
//		 * null
//		 */
//		String[] lines = source.split(Constants.NEWLINE);  // split this little file info by "/n"
//		
//		for (String line : lines) {
//				
//				// record which line is error.
//				if (!validation_fail) {
//					errorNum = errorNum + 1;
//				}
//				
//				// if this line is empty,remove this line.
//				if (line.trim().equals("") || line == null ) {
//					continue;
//				}
//				
//				if(scrambledCodeFirstValue==null){
//					scrambledCodeFirstValue=line;
//				}
//				
//				// judge whether every line is contain :
//				if(!line.contains(":")){
//					validation_fail = true;
//					errorFolderName = xmlSchema.getConfigInfo().get("errorFileName").get("ERROR_PerFileDoNotHaveSemicolon");
//					
//					if(scrambledCodeFirstValue!=null){
//						if(scrambledCodeFirstValue.length()>30){
//							stamp = scrambledCodeFirstValue.substring(0,26);
//						}else{
//							stamp = scrambledCodeFirstValue;
//						}
//					}
//					
////					Pattern pattern = Pattern.compile("@^C(.*)^@Ì");
////					Matcher matcher = pattern.matcher(line);
////					while(matcher.find()){
////						stamp = matcher.group(1);
////					}
//					
//					break;
//				}
//				
//				// before frist ":" value is keyword,after frist ":" is value
//				String keyword = line.replaceFirst("^(\\w+):.*", "$1").trim();
//				String value = null;
//				if (xmlSchema.getDetailTablesInfo().containsKey(keyword)) {
//					value = line.replaceFirst("^(\\w+):(.*)", "$2");
//				} else {
//					value = line.replaceFirst("^(\\w+):(.*)", "$2").trim();
//				}
//				
//				/**
//			     * if the front line information illegally don't do those validation
//				 * just put value into list
//				 */
//				if (!validation_fail) {
//					//<1> validation:To determine whether the keyword combination by characters or Numbers,keyword must match "[A-Za-z][A-Za-z_0-9/\\s]*".
//					if (!Pattern.compile("[A-Za-z][A-Za-z_0-9/\\s]*").matcher(keyword).matches()) {
//						validation_fail = true;
//						errorFolderName = xmlSchema.getConfigInfo().get("errorFileName").get("ERROR_IllegalCharacter");
//						continue;
//					}
//					//<2> validation: If masterTable keyword appear twice if the value is not same filter this file.
//					if (!listKeyword.isEmpty() && listKeyword.contains(keyword)
//							&& !xmlSchema.getDetailTablesInfo().keySet().contains(keyword)) {
//						if (!listKeyword.get(listKeyword.indexOf(keyword)).toString().equalsIgnoreCase(value.toString())) {
//							validation_fail = true;
//							errorFolderName = xmlSchema.getConfigInfo().get("errorFileName").get("ERROR_AppearTwice");
//							continue;
//						}
//					}
//					//<3> validation: Judge the keyword 'raw_loader' value is right or not.
//					if (keyword.equalsIgnoreCase("raw_loader")) {
//						if (!(value.equalsIgnoreCase("raw_pipe") || value.equalsIgnoreCase("noraw")
//								|| value.equalsIgnoreCase("raw"))) {
//							validation_fail = true;
//							errorFolderName = xmlSchema.getConfigInfo().get("errorFileName").get("ERROR_ValueOfRawLoaderIncorrect");
//							continue;
//						} else {
//							rawloadervalue = value;
//						}
//					}
//
//				}
//				
//				// if keyword equals with the dataPartitioningFromcol(value from GBConfig.xml generate is 'stamp') save this value for next step.
//				if (xmlSchema.getDataPartitioningFromcol().equalsIgnoreCase(keyword)) {
//					if (keyword.equalsIgnoreCase("stamp")) {
//						stamp = value;
//					} else {
//						partitioningFromcolValue = value;
//					}
//				}
//				//<6> validtion if this file is not Etest file filter this file
//				if (keyword.equalsIgnoreCase("test_type")) {
//					test_type = value.trim();
//				}
//				//put all 'keyword' and 'value' into List for Validtion.
//				listKeyword.add(keyword);
//				listValue.add(value);
//		}
//		
//		//TODO ##########################################
//		
//		
//		//<7> validation : this little file must contain all four keyword 'database','raw_loader','test_type','stamp' if don't filter this little file.Begin
//		if (!validation_fail && !listKeyword.isEmpty() && !(listKeyword.contains("database") && listKeyword.contains("raw_loader")
//				&& listKeyword.contains("test_type") && listKeyword.contains("stamp"))) {
//			errorFolderName = xmlSchema.getConfigInfo().get("errorFileName").get("ERROR_TableNotContentAllFourKeyword");
//			validation_fail = true;
//		}
//		
//		//<7> validation : this little file must contain all four keyword 'database','raw_loader','test_type','stamp' if don't filter this little file.End
//		//According to the situation get the partition_dm values
//		if (!validation_fail) {
//			SimpleDateFormat partitionDmFormat = new SimpleDateFormat(xmlSchema.getDataPartitioningDatepattern());
//			if (xmlSchema.getDataPartitioningCol().equals("")||xmlSchema.getDataPartitioningFromcol().isEmpty()) {
//				// if DataPartitioningCol value is '' partition_dm set current time
//				partitionDm = partitionDmFormat.format(Calendar.getInstance().getTime());
//			} else if (xmlSchema.getDataPartitioningFromcol().equals("stamp")) {
//				// if DataPartitioningCol value is stamp convert stamp to the partition_dm
//				if (stamp != null && stamp.trim() != "") {
//					partitionDm = partitionDmFormat.format(ValidationFunction.ConvertStampToPartitionDm(stamp));
//				} else {
//					validation_fail = true;
//					errorFolderName = xmlSchema.getConfigInfo().get("errorFileName").get("ERROR_StampValueIsNull");
//				}
//			} else {
//				//if the DataPartitioningCol is not null or not equals with "stamp" judege partitioningFromcolValue value.
//				if (partitioningFromcolValue == null) {
//					/** 
//					 * in front of the verification through then do this validation
//					 * If the value of dataPartitioningFromcol in xml is not empty,not
//					 * equals with "stamp",and per file can't find dataPartitioningFromcol
//					 * value, the file is illegal
//					 */
//					errorFolderName = xmlSchema.getConfigInfo().get("errorFileName").get("ERROR_PartitionFromcolValueNotFindInLittleFile");
//					validation_fail = true;
//				}else{
//					// partitioningFromcolValue have value put this values into partitionDm
//					partitionDm = partitioningFromcolValue;
//				}
//			}
//		}
//		
//		//Add validation if this little file , test_type is null.
//		if(test_type==null&&!validation_fail){
//			errorFolderName = xmlSchema.getConfigInfo().get("errorFileName").get("ERROR_TestTypeIsNull");
//			validation_fail = true;
//		}
//		
//		
//		System.out.println(stamp);
//		System.out.println(partitionDm);
//	}

	
}
