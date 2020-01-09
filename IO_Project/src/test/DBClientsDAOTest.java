package test;

import static org.junit.Assert.assertTrue;

import java.sql.Connection;
import java.sql.PreparedStatement;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import dao.impl.DBClientsDAOImpl;
import model.Client;
import util.JDBCUtils;

public class DBClientsDAOTest {
	private static DBClientsDAOImpl dao;
	
	@BeforeClass//every test
	public static void initData() {
		System.out.println("1.DBClientsDAOImpl START INITIALIZATION");
		dao = new DBClientsDAOImpl();
		dao.actualDB+="_TEMP";
		//dao.historyOfActualDB+="_TEMP";
		String createTable="CREATE TABLE" + 
				"    "+dao.actualDB+" \r\n" + 
				"    (\r\n" + 
				"        ID INT NOT NULL AUTO_INCREMENT,\r\n" + 
				"        FIRST_NAME VARCHAR(32) NOT NULL,\r\n" + 
				"        LAST_NAME VARCHAR(32) NOT NULL,\r\n" + 
				"        GENDER VARCHAR(16) NOT NULL,\r\n" + 
				"        COUNTRY VARCHAR(32) NOT NULL,\r\n" + 
				"        AGE INT NOT NULL,\r\n" + 
				"        REG_DATE DATE,\r\n" + 
				"        CREATED_TS TIMESTAMP DEFAULT CURRENT_TIMESTAMP NULL,\r\n" + 
				"        UPDATED_TS TIMESTAMP NULL,\r\n" + 
				"        PRIMARY KEY (ID)\r\n" + 
				"    )";
		
		Connection conn = JDBCUtils.createConnection();
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement(createTable);
			pstmt.executeUpdate();
			JDBCUtils.release(conn, null, pstmt);
			
		} catch (Exception e) {

		}
		dao.readFileToBase();
		System.out.println("1.DBClientsDAOImpl END INITIALIZATION");
	}
	
	@AfterClass
	public static void destrData() {
		String dropTable="DROP TABLE "+dao.actualDB+";";
		Connection conn = JDBCUtils.createConnection();
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement(dropTable);
			pstmt.executeUpdate();
			JDBCUtils.release(conn, null, pstmt);
		} catch (Exception e) {

		}
		System.out.println("4.DBClientsDAOImpl FINALIZATION");
	}
	
	@Test
	public void testGetClientById1() {
		System.out.println("2.RUN TEST");
		assertTrue(null==dao.getClientById(new Client(-1)));
		System.out.println("3.VIEW RESULTS");
	}
	
	@Test
	public void testGetClientById2() {
		System.out.println("2.RUN TEST");
		assertTrue(null!=dao.getClientById(new Client(1)));
		System.out.println("3.VIEW RESULTS");
	}
	
	@Test(expected = NullPointerException.class)
	public void testGetClientById3() {
		System.out.println("2.RUN TEST");
		dao.getClientById(null);
		System.out.println("3.VIEW RESULTS");
	}

}
