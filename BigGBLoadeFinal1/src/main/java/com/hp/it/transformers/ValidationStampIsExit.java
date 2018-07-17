package com.hp.it.transformers;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.apache.spark.sql.Row;

import com.hp.it.utils.db.DBConnection;
import com.hp.it.utils.db.Table;
import com.hp.it.utils.db.XmlSchema;


public class ValidationStampIsExit {

	static Logger logger = Logger.getLogger(ValidationStampIsExit.class);
	
	static Connection conn=null;
	
	public List<String> masterStampValue;
	
	/**
	 * This function is to get all the stamp from DB and masterDataFrame
	 * @param distinctStampValueList
	 * @param xi
	 * @return
	 */
	public Map<String,String> judgeEveryStampIsExitOrNotInDB (List<Row> distinctStampValueList,XmlSchema xi){
		
		//This Map is this format Map<masterTableName,Map<stamp,Insert_dm>>
		Map<String,Map<String,String>> masterTableStampMap = new HashMap<String,Map<String,String>>();
		
		//below Map is one part of above map values element.
		Map<String,String> stampValuesMap = new HashMap<String,String>();
		
		//First Step:Should put the master table to the masterTableStampMap key.
		xi.getMasterTablesInfo().keySet().forEach(masterTableName -> masterTableStampMap.put(masterTableName,new HashMap<String,String>()));
		
		//Second Step:Because of rowList contain detail table name so should find the master table suit the detail table and put the min insert_dm with samp stamp.
		distinctStampValueList.forEach(row -> putValueIntoMap(row,masterTableStampMap,xi));
		
		try {
			
			conn = DBConnection.getDbConnection(
					xi.getConfigInfo().get("dbConnection").get("dbDriverClass"), 
					xi.getConfigInfo().get("dbConnection").get("dbURL"), 
					xi.getConfigInfo().get("dbConnection").get("dbUser"), 
					xi.getConfigInfo().get("dbConnection").get("dbEncryptedPassword")
					);
			
			List<String> allTheStampExistInDBList = new ArrayList<String>();
			
			/**Third Step:
			   Function one : select all the stamp on DB and get the insert_dm compare with masterTableStampMap insert_dm ,if the DB
			   Insert_dm is min should overwriter the masterTableStampMap 
			   Function two : if stamp is exit in DB should put it into masterStampValue,For master Table Remove Duplicates data.
			   */
			
			masterTableStampMap.keySet().forEach(masterTableName -> 
				stampValuesMap.putAll(updateMapFromDB(conn,masterTableStampMap.get(masterTableName),masterTableName,allTheStampExistInDBList)));
			
			setMasterStampValue(allTheStampExistInDBList);
			
		}catch (Exception e){
			logger.error("when conn the DB appear error,error messsage is:"+e.getMessage());
		}finally{
			if (conn!=null){
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error("when colse the conn appear error :"+e.getMessage());
				}
			}
		}
		
		return stampValuesMap;
		
	}
	
	/**
	 * Step one : Just get the stamp from rowList and put value into map
	 * @param distinctRow
	 * ["stamp","tableName","currentTime"]
	 * @param masterTableStampMap
	 */
	public void putValueIntoMap(Row distinctRow,Map<String,Map<String,String>> masterTableStampMap,XmlSchema xs){
		
		String rowLine = distinctRow.toString().substring(1,distinctRow.toString().length()-1);
		
		String[] arrayRow = rowLine.split(",");
		
		Map<String,Table> mapTableInfo = new HashMap<String,Table>();
		
		mapTableInfo.putAll(xs.getMasterTablesInfo());
		mapTableInfo.putAll(xs.getDetailTablesInfo());
		
		String stamp = arrayRow[0];
	    String tableName = arrayRow[1];
	    String currentTime = arrayRow[2];
	    
	    String everyDetailTestType = mapTableInfo.get(tableName).getTestType();
	    
	    Map<String,String> allTheMasterTestType = new HashMap<String,String>();
	    xs.getMasterTablesInfo().values().forEach(tableInfo -> allTheMasterTestType.put(tableInfo.getTestType(),tableInfo.getTableName()));
	    
	    //Below loggical is to put the min insert_dm with the same stamp;
	    if(masterTableStampMap.keySet().contains(allTheMasterTestType.get(everyDetailTestType))){
	    	
	    	if(masterTableStampMap.get(allTheMasterTestType.get(everyDetailTestType)).isEmpty()){
	    		//First time this map is empty
	    		masterTableStampMap.get(allTheMasterTestType.get(everyDetailTestType)).put(stamp,currentTime);
	    	}else{
	    		if(!masterTableStampMap.get(allTheMasterTestType.get(everyDetailTestType)).keySet().contains(stamp)){//Not found this stamp.
	    			masterTableStampMap.get(allTheMasterTestType.get(everyDetailTestType)).put(stamp,currentTime);
	    		}else{
	    			if(Long.valueOf(masterTableStampMap.get(allTheMasterTestType.get(everyDetailTestType)).get(stamp))>Long.valueOf(currentTime)){//compare the time value
	    				masterTableStampMap.get(allTheMasterTestType.get(everyDetailTestType)).put(stamp,currentTime);
	    			}
	    		}
	    	}
	    	
	    }
	    
	    
	}
	
	/**
	 * 
	 * @param conn
	 * @param stampTimeMap
	 * @param masterTableName
	 * @param masterlist
	 * @return
	 */
	public Map<String,String> updateMapFromDB(Connection conn,Map<String,String> stampTimeMap,String masterTableName,List<String> masterlist){
		
		if(stampTimeMap.size()>0){
		
			StringBuffer sbsql = new StringBuffer();
		
			sbsql.append("select stamp,insert_dm from mfg_pms."+masterTableName+" where stamp in(");
		
			stampTimeMap.keySet().forEach(stamp -> sbsql.append("'"+stamp+"'"+","));
		
			String sql = sbsql.substring(0,sbsql.length()-1);
		
			PreparedStatement pst;
			
			try {
				pst = conn.prepareStatement(sql+")");
				ResultSet rs = pst.executeQuery();
				while(rs.next()){
					String stampValue = rs.getString("stamp");
					Timestamp timeValue = rs.getTimestamp("insert_dm");
					masterlist.add(stampValue);
					stampTimeMap.put(stampValue, String.valueOf(timeValue.getTime()));
				}
			} catch (SQLException e) {
				logger.error("when execute the sql appear error:"+e.getMessage());
			}
		
		}
		
		return stampTimeMap;
	
	}

	public List<String> getMasterStampValue() {
		return masterStampValue;
	}

	public void setMasterStampValue(List<String> masterStampValue) {
		this.masterStampValue = masterStampValue;
	}
	
	
	
}
