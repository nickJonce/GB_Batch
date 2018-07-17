package com.hp.it.utils.db;

import java.sql.Connection;
import java.sql.DriverManager;

public class DBConnection {
	public static Connection getDbConnection(String dbDriver,String dbUrl,String dbUser,String dbPassword) throws Exception {
		Connection con = null;
		try {
			Class.forName(dbDriver).newInstance();
			con = DriverManager.getConnection(dbUrl, dbUser, dbPassword);
		} catch (Exception e) {
			throw e;
		}
		return con;
	}
}
