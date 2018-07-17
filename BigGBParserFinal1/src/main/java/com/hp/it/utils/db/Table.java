package com.hp.it.utils.db;

import java.io.Serializable;
import java.util.List;

public class Table implements Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 5221764226257856971L;
	
	String tableName;
	
	String keyword;
	
	String schema;
	
	String testType;
	
	List<ColumnMetaData> columns;
	
	public String getTableName() {
		return tableName;
	}
	public void setTableName(String tableName) {
		this.tableName = tableName;
	}
	public String getSchema() {
		return schema;
	}
	public void setSchema(String schema) {
		this.schema = schema;
	}
	public List<ColumnMetaData> getColumns() {
		return columns;
	}
	public void setColumns(List<ColumnMetaData> columns) {
		this.columns = columns;
	}
	public String getTestType() {
		return testType;
	}
	public void setTestType(String testType) {
		this.testType = testType;
	}
	public String getKeyword() {
		return keyword;
	}
	public void setKeyword(String keyword) {
		this.keyword = keyword;
	}
	
}
