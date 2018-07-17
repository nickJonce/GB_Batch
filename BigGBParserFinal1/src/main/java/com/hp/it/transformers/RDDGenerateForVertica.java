package com.hp.it.transformers;

import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;

public class RDDGenerateForVertica implements Function<String, Row> {

	private static final long serialVersionUID = 1L;
	private String partitionDM;
	private String currentTime;

	public RDDGenerateForVertica(String _partitionDM, String _currentTime) {
		this.partitionDM = _partitionDM;
		this.currentTime = _currentTime;
	}

	@Override
	public Row call(String record) throws Exception {

		String tableName = record.replaceFirst("^(\\w+):.*", "$1").trim();
		String oneRowData = record.replaceFirst("^(\\w+):(.*)", "$2").trim();

		String[] arrayPartiton_Dm = partitionDM.split(Constants.HYPHEN);

		String[] arrayOneRowData = oneRowData.split(Constants.PIPE);

		String[] fields = new String[8];
		fields[0] = arrayPartiton_Dm[0].trim();
		fields[1] = arrayPartiton_Dm[1].trim();
		fields[2] = arrayPartiton_Dm[2].trim();
		fields[3] = tableName;
		fields[4] = oneRowData;
		fields[5] = arrayOneRowData[0].trim();
		fields[6] = tableName;
		fields[7] = currentTime;

		Object[] fields_converted = fields;
		return RowFactory.create(fields_converted);

	}

}
