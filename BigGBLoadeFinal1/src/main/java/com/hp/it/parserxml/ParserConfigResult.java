package com.hp.it.parserxml;

import java.io.StringReader;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;
import org.jdom2.Element;
import org.jdom2.JDOMException;
import org.jdom2.input.SAXBuilder;

import com.hp.it.utils.db.Table;

public class ParserConfigResult {
	
	static Logger logger = Logger.getLogger(ParserConfigResult.class);	
	
	private Element root;
	
	private Map<String,Table> masterTables;
	
	private Map<String,Table> detailTables;
	
	private Map<String,Map<String,String>> dataRecordMap;
	
	private boolean Data_source_id_fg = false;
	
	private String Data_source_id;
	
	private boolean dataPartitioningState = false;
	
	private String dataPartitioningCol;
	
	private String dataPartitioningFromcol;
	
	private String dataPartitioningDatepattern;
	
	private String dateformat;
	
	private String datetimeformate;
	
	private String timestampformat;
	
	private String coalesceLoaderDBNum;
	
	private String limitOfParquetFileSize;
	
	public ParserConfigResult(String xmlContent) {
		SAXBuilder sb = new SAXBuilder();
		try{
			root = sb.build(new StringReader(xmlContent)).getRootElement();
			dataRecordMap = initResultInMap();
			masterTables = initResultInTableMap("master");
			detailTables = initResultInTableMap("detail");
		}catch(Exception e){
			logger.error("Parser EtlConfigGodzilla.xml have appear error,error message is:"+e.getMessage());
		}
	}
	
	public Map<String,Map<String,String>> initResultInMap(){
		
		Map<String,Map<String,String>> dataMap = new HashMap<String,Map<String,String>>();
		
		if (root.getChildText("Data_source_id_fg").equalsIgnoreCase("true")) {
			Data_source_id_fg = true;
		}
		
		Data_source_id = root.getChildText("Data_source_id");
		
		if (root.getChildText("Data_source_id_fg").equals("true")) {
			dataPartitioningState = true;
		}
		
		dataPartitioningCol = root.getChildText("dataPartitioningCol");
		dataPartitioningFromcol = root.getChildText("dataPartitioningFromcol");
		dataPartitioningDatepattern = root.getChildText("dataPartitioningDatepattern");
		
		dateformat = root.getChildText("dateformat");
		datetimeformate = root.getChildText("datetimeformate");
		timestampformat = root.getChildText("timestampformat");
//		maxLoaderNum=root.getChildText("MaxLoaderNum");
		coalesceLoaderDBNum=root.getChildText("coalesceLoaderDBNum");
		limitOfParquetFileSize = root.getChildText("LimitOfParquetFileSize");
		
		List<Element> dataListDBconn = root.getChildren("dbConnection");
		dataMap.put("dbConnection",getTheConfigNodeInfo(dataListDBconn));
		
		List<Element> dataListAllInputdirPath = root.getChildren("allTheInputdirPath");
		dataMap.put("allTheInputdirPath",getTheConfigNodeInfo(dataListAllInputdirPath));
		
		List<Element> dataListNeedGenerateFile = root.getChildren("needGenerateFile");
		dataMap.put("needGenerateFile",getTheConfigNodeInfo(dataListNeedGenerateFile));
		
		List<Element> dataListErrorFileName = root.getChildren("errorFileName");
		dataMap.put("errorFileName",getTheConfigNodeInfo(dataListErrorFileName));
		
		return dataMap;
	}
	
	
	private Map<String,String> getTheConfigNodeInfo(List<Element> currentList){
		Map<String,String> mapConfigData = new HashMap<String,String>();
		for(Element e:currentList){
			 mapConfigData.putAll(createConfigInfo(e));
		}
		return mapConfigData;
	}
	
	private Map<String,String> createConfigInfo(Element listEL) {
		Map<String,String> mapConfigData = new HashMap<String,String>();
		for (Object element : listEL.getChildren()) {
			Element curEL = (Element) element;
			mapConfigData.put(curEL.getAttributeValue("name"),curEL.getText());
		}
		return mapConfigData;
	}
	
	public Map<String,Table> initResultInTableMap(String typeOfTable) throws JDOMException{
		
		Map<String,Table>  tablesInfo = new HashMap<String,Table>();
		
		List<Element> tableRoot = root.getChildren("testTypes");
		
		for (Element curDataEL : tableRoot) {
			Map<String,Table> tableList = createData(curDataEL,typeOfTable);
			tablesInfo.putAll(tableList);
		}
		
		return tablesInfo;
	}
	
	private Map<String,Table> createData(Element curTableEL,String typeOfTable) throws JDOMException {
		
		Map<String,Table> tablesInfo = new HashMap<String,Table>();
		
		List<Element> oneTypeOfTableList = curTableEL.getChildren("testType");
		
		for(Element curDataEL : oneTypeOfTableList){
			
			String schema = curDataEL.getAttributeValue("schema");
			String testTypeName = curDataEL.getAttributeValue("name");
			
			for(Element curEL : (List<Element>) curDataEL.getChildren("table")){
				
				if(curEL.getAttributeValue("type").equals(typeOfTable)){
					Table table = createTable(curEL,schema,testTypeName);
					tablesInfo.put(table.getTableName(),table);
				}
				
			}
			
		}
		
		return tablesInfo;
		
	}
	
	private Table createTable (Element tableEL,String schema ,String testTypeName){
		
		Table table = new Table();
		for (Object element : tableEL.getChildren()) {
			Element curEL = (Element) element;
			if(curEL.getName().equals("name")){
				table.setTableName(curEL.getText());
			}
		}
		
		table.setSchema(schema);
		table.setTestType(testTypeName);
		return table;
		
	}
	
	public Map<String, Map<String, String>> getDataRecordMap() {
		return dataRecordMap;
	}

	public void setDataRecordMap(Map<String, Map<String, String>> dataRecordMap) {
		this.dataRecordMap = dataRecordMap;
	}

	public boolean isData_source_id_fg() {
		return Data_source_id_fg;
	}

	public void setData_source_id_fg(boolean data_source_id_fg) {
		Data_source_id_fg = data_source_id_fg;
	}

	public String getData_source_id() {
		return Data_source_id;
	}

	public void setData_source_id(String data_source_id) {
		Data_source_id = data_source_id;
	}

	public boolean isDataPartitioningState() {
		return dataPartitioningState;
	}

	public void setDataPartitioningState(boolean dataPartitioningState) {
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

	public Map<String, Table> getMasterTables() {
		return masterTables;
	}
	
	public void setMasterTables(Map<String, Table> masterTables) {
		this.masterTables = masterTables;
	}

	public Map<String, Table> getDetailTables() {
		return detailTables;
	}

	public void setDetailTables(Map<String, Table> detailTables) {
		this.detailTables = detailTables;
	}

	public String getCoalesceLoaderDBNum() {
		return coalesceLoaderDBNum;
	}

	public void setCoalesceLoaderDBNum(String coalesceLoaderDBNum) {
		this.coalesceLoaderDBNum = coalesceLoaderDBNum;
	}

	public String getLimitOfParquetFileSize() {
		return limitOfParquetFileSize;
	}

	public void setLimitOfParquetFileSize(String limitOfParquetFileSize) {
		this.limitOfParquetFileSize = limitOfParquetFileSize;
	}
	
}
