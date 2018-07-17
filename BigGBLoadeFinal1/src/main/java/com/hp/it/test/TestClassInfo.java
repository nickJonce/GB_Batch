package com.hp.it.test;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import com.hp.it.utils.db.DBConnection;

public class TestClassInfo {

	public static void main(String[] args) {
		
		File file = new File("C://YangHeng//test//Test.txt");
		
		System.out.println(file.toString());
		
//		String path ="/MANF/data/outputdir/Parquet/year=2017/month=07/day=22/table=elecj_detail/xxxxx.parquet";
//		
//		String[] pathElement = path.split("/");
		
//		System.out.println(pathElement[pathElement.length-2].split("=")[1]);
		
//		List<String> listCurrentPartitionTableNames = new ArrayList<String>();
//		
//		if(!listCurrentPartitionTableNames.contains("xx")){
//			System.out.println("contain");
//		}else{
//			listCurrentPartitionTableNames.add("xx");
//			System.out.println("not contain");
//		}
		
//		try{
//			if(2==1){
//				throw new RuntimeException();
//			}
//
//		}catch(Exception e){
//			
//			System.out.println(e.getMessage());
//			throw new RuntimeException(e);
//			
//		}finally{
//			System.out.println("end");
//		}
//		
//		System.out.println("finally");
		
//		File file = new File("C://YangHeng//test//");
//		
//		String[] listFile = file.list();
//		
//		for(int i=0;i<listFile.length;i++){
//			listFile[i]="C://YangHeng//test//"+listFile[i];
//		}
		
		
//		List<String> list = new ArrayList<String>();
//		System.out.println(list.contains("str"));
//		Connection conn = null;
//		PreparedStatement pst;
//		try {
//			 conn = DBConnection.getDbConnection(
//					"com.vertica.jdbc.Driver", 
//					"jdbc:vertica://g9t3223.houston.hp.com:5433/shr9_vrt_dev_003", 
//					"srvc_mfg_pms_loader_dev", 
//					"8ObtOgTL50GC"
//					);
//			
//			StringBuffer sbsql = new StringBuffer();
//			
//			sbsql.append("select stamp,insert_dm from mfg_pms.elecj_set where stamp in(");
//			
//			String stamp1 = "et05ccvd.329_17";
//			
//			String stamp2 = "et06cbvd.329_17";
//			
//			sbsql.append("'"+stamp1+"'").append(",").append("'"+stamp2+"'"+")"+";");
//			
//			System.out.println(sbsql.toString());
//			
//			pst = conn.prepareStatement(sbsql.toString());
//			
//			ResultSet rs = pst.executeQuery();
//			
//			while(rs.next()){
//				String stampValue = rs.getString("stamp");
//				Timestamp timeValue = rs.getTimestamp("insert_dm");
//				System.out.println(stampValue);
//				System.out.println(timeValue.toString());
//			}
//			
//		} catch (Exception e) {
//			// TODO Auto-generated catch block
//			e.printStackTrace();
//		}finally{
//			if(conn!=null){
//				try {
//					conn.close();
//				} catch (SQLException e) {
//					e.printStackTrace();
//				}
//			}
//		}
//		
		
	}
	
}
