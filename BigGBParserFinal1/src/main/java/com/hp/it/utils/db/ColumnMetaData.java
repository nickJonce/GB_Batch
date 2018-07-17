package com.hp.it.utils.db;

import java.io.Serializable;

/**
 * Class that describes a column's metadata in the database 
 * @author heng
 */

public class ColumnMetaData implements Comparable<ColumnMetaData>, Serializable {
	
	private static final long serialVersionUID = 1L;

	private String columnName; // COLUMN_NAME
	private int dataType; // DATA_TYPE
	private String typeName; // TYPE_NAME
	private int columnSize; // COLUMN_SIZE
	private int ordinalPosition; // ORDINAL_POSITION
	private int columnNullable; // ColumnNullable
	private String columnValue; //ColumnValue

	public ColumnMetaData() {

	}

	public String getColumnName() {
		return columnName;
	}

	public void setColumnName(String columnName) {
		this.columnName = columnName;
	}

	public int getDataType() {
		return dataType;
	}

	public void setDataType(int dataType) {
		this.dataType = dataType;
	}

	public String getTypeName() {
		return typeName;
	}

	public void setTypeName(String typeName) {
		this.typeName = typeName;
	}

	public int getColumnSize() {
		return columnSize;
	}

	public void setColumnSize(int columnSize) {
		this.columnSize = columnSize;
	}

	public int getOrdinalPosition() {
		return ordinalPosition;
	}

	public void setOrdinalPosition(int ordinalPosition) {
		this.ordinalPosition = ordinalPosition;
	}

	public int getColumnNullable() {
		return columnNullable;
	}

	public void setColumnNullable(int columnNullable) {
		this.columnNullable = columnNullable;
	}

	public String getColumnValue() {
		return columnValue;
	}

	public void setColumnValue(String columnValue) {
		this.columnValue = columnValue;
	}

	public int compareTo(ColumnMetaData o) {
		int pos = this.ordinalPosition;
		int anotherPos = (o == null ? -1 : o.getOrdinalPosition());
		return (pos < anotherPos ? -1 : (pos == anotherPos ? 0 : 1));
	}
}
