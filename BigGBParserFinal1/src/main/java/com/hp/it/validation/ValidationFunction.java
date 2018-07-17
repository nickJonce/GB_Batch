package com.hp.it.validation;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.hp.it.driver.GBParserDriver;
import com.hp.it.utils.db.ColumnMetaData;
import com.hp.it.utils.db.Table;
import com.hp.it.utils.db.XmlSchema;

public class ValidationFunction {
	
	static Logger logger = Logger.getLogger(ValidationFunction.class);	
	
	/**
	 * use to validate partition_column_not_existed
	 * 
	 * @param lcmd
	 * @param tablesInfo
	 * @return Boolean
	 */
//	public static Boolean VerdictPartitionDMIsExit(XmlSchema xmlSchema,Map<String,Table> map,String tableName) {
//		for (ColumnMetaData cmd : map.get(tableName).getColumns()) {
//			if (!cmd.getColumnName().equalsIgnoreCase(xmlSchema.getDataPartitioningCol().trim())) {
//				return false;
//			}
//		}
//		return true;
//	}
	public static Boolean VerdictPartitionDMIsExit(XmlSchema xmlSchema,String tableName) {
		for (ColumnMetaData cmd : xmlSchema.getDetailTablesInfo().get(tableName).getColumns()) {
			if (!cmd.getColumnName().equalsIgnoreCase(xmlSchema.getDataPartitioningCol().trim())) {
				return false;
			}
		}
		return true;
	}
	
	/**
	 * use stamp to convert partition_dm
	 */
	public static Date ConvertStampToPartitionDm(String stampvalue) {
		// set the archive day to the date encoded in the stamp, and grab the partition
		// value in case partitioning is on
		Date stampDateTime=null;
		
		String sMonth = "";
		String sDay = "";
		String sYear = "";
		Integer iYear = null;
		String sDateStr = null;
		int iMonthPosition = stampvalue.indexOf(".") + 1;
		// If this is the old version of a stamp with a ".MDD_YY" format
		if (iMonthPosition > 0) {
			
			sMonth = stampvalue.substring(iMonthPosition, (iMonthPosition + 1));
			sDay = stampvalue.substring((iMonthPosition + 1), (iMonthPosition + 3));
			sYear = stampvalue.substring((iMonthPosition + 4), (iMonthPosition + 6));
			iYear = calculateCentury(sYear);

			// since the month in the stamp is only one digit, we have to change
			// the a, b
			// and c into 10, 11 and 12 here
			if (sMonth.equalsIgnoreCase("a")) {
				sMonth = "10";
			} else {
				if (sMonth.equalsIgnoreCase("b")) {
					sMonth = "11";
				} else {
					if (sMonth.equalsIgnoreCase("c")) {
						sMonth = "12";
					}
				}
			}

			// Make sure we have a two character month for the partiion key
			if (sMonth.length() < 2) {
				sMonth = "0" + sMonth;
			}

			// form a string like "yy-MM-dd:HH:mm:ss",added by
			// hejunping,2009-8-30
			sDateStr = String.valueOf(iYear) + "-" + sMonth + "-" + sDay + " 00:00:00";
			// stampDateTime=sDateStr;
			// get the stamp date time,added by hejunping,2009-8-27
			stampDateTime = getStampDateTime(stampvalue, sDateStr);
		}
		// Otherwise this is the new version of the stamp with the "_YYMMDD_"
		// format
		else {
			int iYearPosition = stampvalue.indexOf("_") + 1;
			sYear = stampvalue.substring(iYearPosition, (iYearPosition + 2));
			sMonth = stampvalue.substring((iYearPosition + 2), (iYearPosition + 4));
			sDay = stampvalue.substring((iYearPosition + 4), (iYearPosition + 6));
			iYear = calculateCentury(sYear);

			// form a string like "yy-MM-dd HH:mm:ss",added by
			// hejunping,2009-8-30
			sDateStr = iYear + "-" + sMonth + "-" + sDay + " 00:00:00";
			// stampDateTime=sDateStr;
			// get the stamp date time,added by hejunping,2009-8-27
			stampDateTime = getStampDateTime(stampvalue, sDateStr);
		}
		return stampDateTime;
	}

	/**
	 * This function figures out whether to put a stamp's two-digit year in the
	 * current or the previous century. It will break in the years 2080 - 2099
	 * if someone tries to put in data that's dated later than the current year
	 */
	public static Integer calculateCentury(String sYear) {
//		logger.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
//		logger.debug("Attention error :"+sYear);
//		logger.debug("$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$");
		Calendar CurrentDate = Calendar.getInstance();
		Integer iPartYear = new Integer(sYear);
		String sCurrentYear = (new Integer(CurrentDate.get(Calendar.YEAR))).toString();
		Integer iCurrentCentury = new Integer(sCurrentYear.substring(0, 2));
		Integer iPreviousCentury = new Integer(iCurrentCentury.intValue() - 1);
		Integer iCurrentYear = new Integer(sCurrentYear.substring(2, 4));

		Integer iYear = null;

		if (iPartYear.equals(iCurrentYear)) {
			iYear = new Integer(iCurrentCentury.toString() + sYear);
		} else {
			if (iPartYear.compareTo(iCurrentYear) < 0) {
				iYear = new Integer(iCurrentCentury.toString() + sYear);
			} else {
				if (iPartYear.intValue() < 80) {
					iYear = new Integer(iCurrentCentury.toString() + sYear);
				} else {
					iYear = new Integer(iPreviousCentury.toString() + sYear);
				}

			}
		}
		return iYear;
	}
	
	// get stamp datetime, added by hejunping,2009-8-30
	@SuppressWarnings("static-access")
	public static Date getStampDateTime(String inStamp, String inSDateStr) {
		Math math = null;
		long lSeconds = 0;
		long lMilliSeconds = 0;
		int iDotPosition = inStamp.indexOf(".");
		String sAscStr = null;
//		String sStampDateTime = null;

		java.util.Date dStampDate = new java.util.Date();
		
		if (iDotPosition > 0) {
			// get the value of "xxx" in "Pi12xxxd.mdd_yy" format or "xxxxx"
			// string in "Pi12xxxxxd.mdd_yy"
			sAscStr = inStamp.substring(4, iDotPosition - 1);
		} else {
			// get the value of "xxx" in "Pi12_yymmdd_xxx" format or "xxxxx"
			// string in "Pi12_yymmdd_xxxxx"
			int iUnderLinePosition = inStamp.lastIndexOf("_");
			sAscStr = inStamp.substring(iUnderLinePosition + 1, inStamp.length());
		}

		// convert the value of "xxx" or "xxxxx" to second
		if (sAscStr.length() == 3) {
			lSeconds = Long.parseLong(sAscStr.substring(0, 1), 36) * (long) (math.pow(36, 2))
					+ Long.parseLong(sAscStr.substring(1, 2), 36) * (long) (math.pow(36, 1))
					+ Long.parseLong(sAscStr.substring(2, 3), 36);
			lSeconds = lSeconds * 2;
			lMilliSeconds = lSeconds * 1000;
		} else if (sAscStr.length() == 5) {
			lMilliSeconds = Long.parseLong(String.valueOf(sAscStr.charAt(0) - 'a')) * (long) (math.pow(26, 4))
					+ Long.parseLong(String.valueOf(sAscStr.charAt(1) - 'a')) * (long) (math.pow(26, 3))
					+ Long.parseLong(String.valueOf(sAscStr.charAt(2) - 'a')) * (long) (math.pow(26, 2))
					+ Long.parseLong(String.valueOf(sAscStr.charAt(3) - 'a')) * (long) (math.pow(26, 1))
					+ Long.parseLong(String.valueOf(sAscStr.charAt(4) - 'a'));
			lMilliSeconds = lMilliSeconds * 10;
		}

		try {
			// get stamp datetime string
			SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
			dStampDate = formatter.parse(inSDateStr);
//			ldateMilliSeconds = dStampDate.getTime() + lMilliSeconds;
//			dStampDate.setTime(ldateMilliSeconds);
//			sStampDateTime = formatter.format(dStampDate);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return dStampDate;
	}

	
	/**
	 * use to validation to validation Invalid_column_length_check
	 */
	public static int CountNumber(String detailvalue, char sign) {
		int count = 0;
		if (sign == '|') {
			for (int i = 0; i < detailvalue.length(); i++) {
				if (detailvalue.charAt(i) == sign) {
					count++;
				}
			}
			return count + 1;
		} else {
			String newDetailValue = detailvalue.substring(1, detailvalue.length());
			for (int i = 0; i < newDetailValue.length(); i++) {
				if (detailvalue.charAt(i) == sign) {
					count++;
				}
			}
			return count + 1;
		}
		
	}
	
	
	/**
	 * To acquire new generated string
	 */
	public static String AcquireNewString(int stampPosition, int partition_dm, String value, List<ColumnMetaData> lcmd,
			String partitionDm, String stampvlaue, String rawloadervalue) {

		if (rawloadervalue.equalsIgnoreCase("raw_pipe")) {

			String joinTogetherValue = stampvlaue + value + partitionDm;

			String[] valueper = joinTogetherValue.split("\\|");

			if ((partition_dm - 1) == lcmd.size()) {
				return joinTogetherValue;
			} else {
				StringBuffer sb = new StringBuffer();
				boolean flag = true;
				for (int i = 0; i < valueper.length; i++) {
					if (flag) {
						if (i != (partition_dm - 1)) {
							sb.append(valueper[i].trim() + "|");
						} else {
							flag = false;
							sb.append(partitionDm.trim() + "|");
						}
					} else {
						if (i < valueper.length - 1) {
							sb.append(valueper[i - 1].trim() + "|");
						} else {
							sb.append(valueper[i - 1].trim());
						}
					}
				}

				return sb.toString();
			}

		} else {
			String joinTogetherValue = stampvlaue + value + partitionDm;
			String[] valueper = joinTogetherValue.split(" ");
			if ((partition_dm - 1) == lcmd.size()) {
				return joinTogetherValue;
			} else {
				StringBuffer sb = new StringBuffer();
				List<String> list = new ArrayList<String>();
				boolean flag = true;
				for (int i = 0; i < valueper.length; i++) {
					if (flag) {
						if (i != (partition_dm - 1)) {
							list.add(i, valueper[i].trim());
						} else {
							flag = false;
							list.add(i, partitionDm.trim());
						}
					} else {
						list.add(i, valueper[i - 1].trim());
					}
				}
				for (int i = 0; i < list.size(); i++) {
					if (i < list.size() - 1) {
						sb.append(list.get(i) + "|");
					} else {
						sb.append(list.get(i));
					}
				}
				return sb.toString();
			}
		}
	}

	/**
	 * remove the 
	 * @param newsource
	 * @return
	 */
	public static String RemoveEmptyLine(String newsource) {
		
		StringBuffer sb = new StringBuffer();
		
		String[] newsourceper = newsource.split("\n");
		for (int i = 0; i < newsourceper.length; i++) {
			if (!(newsourceper[i].isEmpty() || newsourceper[i] == "" || newsourceper[i] == null)) {
				sb.append(newsourceper[i] + "\n");
			}
		}
		return sb.toString();
	}
	
}
