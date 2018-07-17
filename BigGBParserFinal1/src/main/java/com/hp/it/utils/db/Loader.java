package com.hp.it.utils.db;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.hp.it.parserxml.ParserConfigResult;

public class Loader {
	
	static Logger logger = Logger.getLogger(Loader.class);	
	
	static ResultSet columnMetaDataRs = null;
	
	Connection conn = null;
	
	public XmlSchema loadData(String loadContent) {
		
		XmlSchema xi = new XmlSchema();
		
		ParserConfigResult parserResult = new ParserConfigResult(loadContent);
		
		xi.setData_source_id_fg(parserResult.isData_source_id_fg());
		xi.setData_source_id(parserResult.getData_source_id());
		xi.setDataPartitioningState(parserResult.isDataPartitioningState());
		xi.setDataPartitioningCol(parserResult.getDataPartitioningCol());
		xi.setDataPartitioningFromcol(parserResult.getDataPartitioningFromcol());
		xi.setDataPartitioningDatepattern(parserResult.getDataPartitioningDatepattern());
		
		xi.setDateformat(parserResult.getDateformat());
		xi.setDatetimeformate(parserResult.getDatetimeformate());
		xi.setTimestampformat(parserResult.getTimestampformat());
		xi.setNumOfRDDLimit(parserResult.getNumOfRDDLimit());
//		xi.setCoalesceGenerateParquetNum(Integer.valueOf(parserResult.getCoalesceGenerateParquetNum()));
		
		Map<String,Map<String,String>> dataRecordMap = parserResult.getDataRecordMap();
		
		Map<String,Table> masterTablesInfo = parserResult.getMasterTables();
		
		Map<String,Table> detailTablesInfo = parserResult.getDetailTables();
		
		xi.setConfigInfo(dataRecordMap);
		
		xi.setDetailTablesInfo(detailTablesInfo);
		
		xi.setMasterTablesInfo(masterTablesInfo);
		
		try {
			
			//get the connection 
			conn = DBConnection.getDbConnection(
					xi.getConfigInfo().get("dbConnection").get("dbDriverClass"), 
					xi.getConfigInfo().get("dbConnection").get("dbURL"), 
					xi.getConfigInfo().get("dbConnection").get("dbUser"), 
					xi.getConfigInfo().get("dbConnection").get("dbEncryptedPassword")
					);
			
			masterTablesInfo.values().forEach(table -> 
					table.setColumns(GetColumnsByTableName(table.getTableName(),conn))
					);
			
			detailTablesInfo.values().forEach(table -> 
					table.setColumns(GetColumnsByTableName(table.getTableName(),conn))
					);
			
		} catch (Exception e) {
			logger.error("when connection Vertica DB appear error:"+e.getMessage());
			throw new RuntimeException(e);
		}finally{
			if(conn!=null){
				try {
					conn.close();
				} catch (SQLException e) {
					logger.error("when close the conn appear error"+e.getMessage());
				}
			}
		}
		
		return xi;
		
	}
	
	public static List<ColumnMetaData> GetColumnsByTableName(String tablename, Connection conn) {
		List<ColumnMetaData> columnsInfo = new ArrayList<ColumnMetaData>();
		try {
			columnMetaDataRs = conn.getMetaData().getColumns(null, null, tablename, null);
			while (columnMetaDataRs.next()) {
				ColumnMetaData columnMetaData = new ColumnMetaData();
				columnMetaData.setColumnName(columnMetaDataRs.getString("COLUMN_NAME").toLowerCase());
				columnMetaData.setDataType(Integer.parseInt(columnMetaDataRs.getString("DATA_TYPE")));
				columnMetaData.setTypeName(columnMetaDataRs.getString("TYPE_NAME"));
				columnMetaData.setColumnSize(Integer.parseInt(columnMetaDataRs.getString("COLUMN_SIZE")));
				columnMetaData.setOrdinalPosition(Integer.parseInt(columnMetaDataRs.getString("ORDINAL_POSITION")));
				columnMetaData.setColumnNullable(Integer.parseInt(columnMetaDataRs.getString("NULLABLE")));
				columnsInfo.add(columnMetaData);
			}
		} catch (SQLException e) {
			logger.error("When get Column Info from db appear error:"+e.getMessage());
		}finally{
			try {
				if(columnMetaDataRs!=null){
					columnMetaDataRs.close();
				}
			} catch (SQLException e) {
				logger.error("When colse columnMetaDataRs appear error "+e.getMessage());
			}
		}
		return columnsInfo;
	}
	
}