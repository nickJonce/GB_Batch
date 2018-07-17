package com.hp.it.test;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Properties;

import com.vertica.jdbc.VerticaConnection;
import com.vertica.jdbc.VerticaCopyStream;

public class TestClass {

	public static void main(String[] args) {
		
		String line = "et10oahd.208_17|ETEST|et10|3.3.2.11.6 Test 6T|2017-07-04 05:58:57|2017-07-04 05:58:57str|2017-02-08|2016-10-13";
		
		String line1 = "et10oahd.208_16|ETEST|et10|3.3.2.11.6 Test 6T|2017-07-04 05:58:57|2017-07-04 05:58:57|2017-02-08|2016-10-13";
		
		Connection conn = null;
		
		Properties myProp = new Properties();
		
		myProp.put("user","srvc_mfg_pms_loader_dev");
		myProp.put("password","8ObtOgTL50GC");
		myProp.put("AutoCommit","false"); 
		
		try{
			
			conn =  DriverManager.getConnection(
					"jdbc:vertica://g9t3223.houston.hp.com:5433/shr9_vrt_dev_003",
					myProp
					);
			
//			String copyQueryMaster = "COPY mfg_pms.elecj_set FROM STDIN "
//                    + "  REJECTED DATA AS TABLE mfg_pms.GB_REJECT_INFO";
			
//			String copyQueryMaster = "COPY mfg_pms.elecj_set FROM LOCAL STDIN "
//                    + "DELIMITER '|' DIRECT exceptions '"+"C://YangHeng//test//HPExecption.log"+"' REJECTED DATA '"+"C://YangHeng//test//HPRejected.log"+"'";
			
//			String copyQueryMaster = "COPY mfg_pms.elecj_set FROM LOCAL STDIN "
//                    + "DELIMITER '|' DIRECT";
			
			String copyQueryMaster = "COPY mfg_pms.elecj_set FROM STDIN NO COMMIT "
                    + "DELIMITER '|' DIRECT REJECTED DATA AS TABLE GB_REJECT_INFO";
			
//			COPY cb.table_format2 FROM STDIN ENCLOSED BY '"' delimiter ',' SKIP 1 rejected data as table cb.table_format2_error1
			
			
//			String copyQueryMaster = "COPY mfg_pms.elecj_set FROM STDIN "
//          + "DELIMITER '|' DIRECT";
			
			VerticaCopyStream masterStream = new VerticaCopyStream( (VerticaConnection) conn, copyQueryMaster);
			masterStream.start();
			InputStream in_nocode_master = new ByteArrayInputStream((line+"\n").getBytes());
			InputStream in_nocode_master1 = new ByteArrayInputStream((line1+"\n").getBytes());
			masterStream.addStream(in_nocode_master);
			masterStream.addStream(in_nocode_master1);
			masterStream.execute();
			
			System.out.println(masterStream.getRowCount());
			
			conn.commit();
			
		}catch(Exception e){
			try {
				conn.rollback();
			} catch (SQLException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
			}
			e.printStackTrace();
		}finally{
			try {
				conn.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
		
	}
}
