package com.hp.it.utils.db;

import java.io.Serializable;
import java.util.Map;


public class XmlSchema implements Serializable{
	
	/**
	 * 
	 */
	private static final long serialVersionUID = 1667133107767098246L;
	
	Boolean Data_source_id_fg;
	String Data_source_id;
	Boolean dataPartitioningState;
	String dataPartitioningCol;
	String dataPartitioningFromcol;
	String dataPartitioningDatepattern;

	String dateformat;
	String datetimeformate;
	String timestampformat;
	String numOfRDDLimit;
//	int coalesceGenerateParquetNum;
	
	Map<String,Map<String,String>> ConfigInfo;
	
	Map<String,Table> masterTablesInfo;
	
	Map<String,Table> detailTablesInfo;
	
	public XmlSchema() {
		super();
	}

	public Boolean getData_source_id_fg() {
		return Data_source_id_fg;
	}

	public void setData_source_id_fg(Boolean data_source_id_fg) {
		Data_source_id_fg = data_source_id_fg;
	}

	public String getData_source_id() {
		return Data_source_id;
	}

	public void setData_source_id(String data_source_id) {
		Data_source_id = data_source_id;
	}

	public Boolean getDataPartitioningState() {
		return dataPartitioningState;
	}

	public void setDataPartitioningState(Boolean dataPartitioningState) {
		this.dataPartitioningState = dataPartitioningState;
	}

	public String getDataPartitioningCol() {
		return dataPartitioningCol;
	}

	public void setDataPartitioningCol(String dataPartitioningCol) {
		this.dataPartitioningCol = dataPartitioningCol;
	}

	public String getDataPartitioningFromcol() {
		return dataPartitioningFromcol;
	}

	public void setDataPartitioningFromcol(String dataPartitioningFromcol) {
		this.dataPartitioningFromcol = dataPartitioningFromcol;
	}

	public String getDataPartitioningDatepattern() {
		return dataPartitioningDatepattern;
	}

	public void setDataPartitioningDatepattern(String dataPartitioningDatepattern) {
		this.dataPartitioningDatepattern = dataPartitioningDatepattern;
	}

	public String getDateformat() {
		return dateformat;
	}

	public void setDateformat(String dateformat) {
		this.dateformat = dateformat;
	}

	public String getDatetimeformate() {
		return datetimeformate;
	}

	public void setDatetimeformate(String datetimeformate) {
		this.datetimeformate = datetimeformate;
	}

	public String getTimestampformat() {
		return timestampformat;
	}

	public void setTimestampformat(String timestampformat) {
		this.timestampformat = timestampformat;
	}

	public Map<String, Map<String, String>> getConfigInfo() {
		return ConfigInfo;
	}

	public void setConfigInfo(Map<String, Map<String, String>> configInfo) {
		ConfigInfo = configInfo;
	}

	public Map<String, Table> getMasterTablesInfo() {
		return masterTablesInfo;
	}

	public void setMasterTablesInfo(Map<String, Table> masterTablesInfo) {
		this.masterTablesInfo = masterTablesInfo;
	}

	public Map<String, Table> getDetailTablesInfo() {
		return detailTablesInfo;
	}

	public void setDetailTablesInfo(Map<String, Table> detailTablesInfo) {
		this.detailTablesInfo = detailTablesInfo;
	}

	public String getNumOfRDDLimit() {
		return numOfRDDLimit;
	}

	public void setNumOfRDDLimit(String numOfRDDLimit) {
		this.numOfRDDLimit = numOfRDDLimit;
	}

//	public int getCoalesceGenerateParquetNum() {
//		return coalesceGenerateParquetNum;
//	}
//
//	public void setCoalesceGenerateParquetNum(int coalesceGenerateParquetNum) {
//		this.coalesceGenerateParquetNum = coalesceGenerateParquetNum;
//	}
	
}
