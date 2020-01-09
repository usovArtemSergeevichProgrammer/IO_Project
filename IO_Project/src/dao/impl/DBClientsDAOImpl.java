package dao.impl;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import dao.ClientsDAO;
import model.Client;
import poi.XLSXReader;
import util.JDBCUtils;


/*
 
  	CREATE TABLE
    clients_history
    (
        CHANGE_ID INT NOT NULL AUTO_INCREMENT,
        ACTION VARCHAR(32) NOT NULL,
        ID INT NOT NULL,
        FIRST_NAME VARCHAR(32),
        LAST_NAME VARCHAR(32),
        COUNTRY VARCHAR(20),
        GENDER VARCHAR(10),
        AGE INT,
        REG_DATE DATE,
        CREATED_TS TIMESTAMP NULL,
        UPDATED_TS TIMESTAMP NULL,
        PRIMARY KEY (CHANGE_ID)
    )
    
    CREATE TABLE
    clients
    (
        ID INT NOT NULL AUTO_INCREMENT,
        FIRST_NAME VARCHAR(32),
        LAST_NAME VARCHAR(32),
        COUNTRY VARCHAR(20),
        GENDER VARCHAR(10),
        AGE INT,
        REG_DATE DATE,
        CREATED_TS TIMESTAMP NULL,
        UPDATED_TS TIMESTAMP NULL,
        PRIMARY KEY (ID)
    )
 */



public class DBClientsDAOImpl implements ClientsDAO {
	
	public String actualDB="CLIENTSDB.CLIENTS";
	public String historyOfActualDB="CLIENTSDB.CLIENTS_HISTORY";

	@Override
	public Client getClientById(Client client) {
		String pStatement = "SELECT * FROM "+actualDB+" WHERE ID=?;";
		Connection conn = JDBCUtils.createConnection();
		PreparedStatement pstmt = null;
		Client result = null;
		try {
			pstmt = conn.prepareStatement(pStatement);
			pstmt.setInt(1, client.getId());
			ResultSet rs = pstmt.executeQuery();
			result = getClient(rs);
			if (result == null) {
				System.out.println("No such client found with ID = " + client.getId());
			}
			JDBCUtils.release(conn, null, pstmt);
		} catch (SQLException e) {

		}

		return result;
	}

	@Override
	public List<Client> getAllClients() {
		String pStatement = "SELECT * FROM "+actualDB+";";
		List<Client> list=null;
		Connection conn = JDBCUtils.createConnection();
		PreparedStatement pstmt = null;
		try {
			pstmt = conn.prepareStatement(pStatement);
			ResultSet rs = pstmt.executeQuery();
			list = getClientList(rs);
			if (list.size() == 0) {
				System.out.println("No such clients");
			}
			JDBCUtils.release(conn, null, pstmt);
		} catch (Exception e) {

		}
		return list;
	}

	@Override
	public boolean insertClient(Client client) {
		String idIndex=client.getId()!=0?"?":"NULL";
		String insertStmt = "INSERT INTO "+actualDB+" (ID, FIRST_NAME, LAST_NAME, GENDER,"
				+ " COUNTRY, AGE, REG_DATE, CREATED_TS) VALUES ";
		insertStmt+=String.format("(%s, ?, ?, ?, ?, ?, ?, CURRENT_TIMESTAMP);",idIndex);
		boolean isInserted = false;
		Connection conn = null;
		PreparedStatement pstmt = null;
		try {
			conn = JDBCUtils.createConnection();
			pstmt = conn.prepareStatement(insertStmt);
			if(idIndex!=null) {
				pstmt.setInt(1, client.getId());
			}
			setClientStatement(pstmt, client, 1);
			isInserted = pstmt.executeUpdate() == 1;
			if (isInserted) {
				JDBCUtils.release(null, null, pstmt);
				addActionToHistory("I", conn, pstmt, client);
				System.out.println(client + " was inserted.");
			}
			JDBCUtils.release(conn, null, pstmt);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return isInserted;
	}

	@Override
	public boolean updateClient(Client client) {
		String updateStmt = "UPDATE "+actualDB+" SET FIRST_NAME = ?, LAST_NAME = ?, GENDER = ?,"
				+ " COUNTRY = ?, AGE = ?,REG_DATE = ?, UPDATED_TS = CURRENT_TIMESTAMP WHERE ID = ?";
		boolean isUpdated = false;
		Connection conn = null;
		PreparedStatement pstmt = null;
		try {
			conn = JDBCUtils.createConnection();
			pstmt = conn.prepareStatement(updateStmt);
			setClientStatement(pstmt, client, 0);
			pstmt.setInt(7, client.getId());
			isUpdated = pstmt.executeUpdate() == 1;
			if (isUpdated) {
				JDBCUtils.release(null, null, pstmt);
				addActionToHistory("U", conn, pstmt, client);
				System.out.println(client + " was updated.");
			}
			JDBCUtils.release(conn, null, pstmt);
		} catch (SQLException e) {

			e.printStackTrace();
		}
		return isUpdated;
	}

	@Override
	public boolean deleteClient(Client client) {
		client = getClientById(client);
		boolean isDeleted = false;
		String updateSQL = "DELETE FROM "+actualDB+" WHERE ID=?;";
		Connection conn = null;
		PreparedStatement pstmt = null;
		try {
			conn = JDBCUtils.createConnection();
			pstmt = conn.prepareStatement(updateSQL);
			pstmt.setInt(1, client.getId());
			isDeleted = pstmt.executeUpdate() == 1;
			if (isDeleted) {
				JDBCUtils.release(null, null, pstmt);
				addActionToHistory("D", conn, pstmt, client);
				System.out.println(client + " was deleted.");
			}
			JDBCUtils.release(conn, null, pstmt);
		} catch (SQLException e) {

			e.printStackTrace();
		}
		return isDeleted;
	}

	@Override
	public void deleateAllClients() {
		List<Client> list = getAllClients();
		for (Client client : list) {
			deleteClient(client);
		}
		System.out.println("All clients was deleted.");
	}

	@Override
	public boolean deleteClientsByCountry(String country) {
		Connection conn = JDBCUtils.createConnection();
		PreparedStatement pstmt = null;
		boolean isDeleted = false;
		try {
			pstmt = conn.prepareStatement("SELECT ID FROM "+actualDB+" WHERE COUNTRY=?;");
			pstmt.setString(1, country);
			ResultSet rs = pstmt.executeQuery();
			while (rs.next()) {
				deleteClient(new Client(rs.getInt("ID")));
			}
			JDBCUtils.release(conn, null, pstmt);
		} catch (Exception e) {

		}
		return isDeleted;
	}

	@Override
	public List<Client> getAllClientsRegistredBefore(java.util.Date date) {
		Connection conn = JDBCUtils.createConnection();
		PreparedStatement pstmt = null;
		List<Client> clients = null;
		try {
			pstmt = conn.prepareStatement("SELECT * FROM "+actualDB+" WHERE REG_DATE<?;");
			pstmt.setDate(1, new Date(date.getTime()));
			ResultSet rs = pstmt.executeQuery();
			clients=getClientList(rs);
			JDBCUtils.release(conn, null, pstmt);
		} catch (Exception e) {

		}
		return clients;
	}

	@Override
	public boolean updateOrInsertClient(Client client) {
		if(updateClient(client)||insertClient(client)) {
			return true;
		}		
		return false;
	}

	private Client getClient(ResultSet rs) throws SQLException {
		Client result = null;
		if (rs.next()) {
			result = new Client();
			result.setId(rs.getInt("ID"));
			result.setFirstName(rs.getString("FIRST_NAME"));
			result.setLastName(rs.getString("LAST_NAME"));
			result.setCountry(rs.getString("COUNTRY"));
			result.setGender(rs.getString("GENDER"));
			result.setAge(rs.getInt("AGE"));
			result.setDate(rs.getDate("REG_DATE"));
			result.setCreatedTs(rs.getTimestamp("CREATED_TS"));
			result.setUpdatedTs(rs.getTimestamp("UPDATED_TS"));
			return result;
		} else {
			return null;
		}
	}

	private List<Client> getClientList(ResultSet rs) throws SQLException {
		List<Client> list = new ArrayList<Client>();
		do {
			list.add(getClient(rs));
		} while (!rs.isLast());
		return list;
	}

	private void addActionToHistory(String action, Connection conn, PreparedStatement pstmt, Client client) throws SQLException {
		String idParameter=null;
		int index=0;
		if (action.equals("I")) {
			idParameter="(SELECT MAX(ID) FROM "+actualDB+")";
		}else {
			idParameter="?";
			index++;
		}
		String statement = "INSERT INTO "+historyOfActualDB+" (ACTION, ID, FIRST_NAME, LAST_NAME, GENDER,"
				+ " COUNTRY, AGE, REG_DATE) VALUES";
		statement += String.format(" ('%s', %s, ?, ?, ?, ?, ?, ?);", action,idParameter);
		pstmt = conn.prepareStatement(statement);
		if(index==1) {
			pstmt.setInt(index, client.getId());
		}
		pstmt.setString(++index, client.getFirstName());
		pstmt.setString(++index, client.getLastName());
		pstmt.setString(++index, client.getGender());
		pstmt.setString(++index, client.getCountry());
		pstmt.setInt(++index, client.getAge());
		pstmt.setDate(++index, new Date(client.getDate().getTime()));
		pstmt.executeUpdate();

	}

    public void readFileToBase () {
	List<Client> clients = null;
	XLSXReader reader = new XLSXReader("resourses/file_example.xlsx");
	try {
		clients = reader.getClients();
	} catch (Exception e) {
		e.printStackTrace();
	}
	for (Client client : clients) {
		insertClient(client);
	}
}

    private void setClientStatement(PreparedStatement pstmt,Client client,int index) throws SQLException {
    	pstmt.setString(++index, client.getFirstName());
		pstmt.setString(++index, client.getLastName());
		pstmt.setString(++index, client.getGender());
		pstmt.setString(++index, client.getCountry());
		pstmt.setInt(++index, client.getAge());
		pstmt.setDate(++index, new Date(client.getDate().getTime()));
    }
    
    public static void main(String[] args) {
		DBClientsDAOImpl dao = new DBClientsDAOImpl();
//		Client key = new Client(1002);
//		Client dbClient = dao.getClientById(key);
//		dbClient.setAge(dbClient.getAge()+1);
//		boolean isUpdated=dao.updateClient(dbClient);
//		System.out.println(isUpdated);

		
		
		
		dao.readFileToBase();
		
//		dao.deleateAllClients();
//		for (Client client:dao.getAllClientsRegistredBefore(new java.util.Date("JUN 1 2016"))) {
//			System.out.println(client);
//		}
		
		
//		Client client=dao.getClientById(new Client(2));
//		Client client2=dao.getClientById(new Client(2));
//		client2.setAge(22);
//		dao.updateOrInsertClient(client2);
//		dao.deleteClient(client);
//		dao.updateOrInsertClient(client);
		

//		
//		try {
//			clients=reader.getClients();
//		} catch (Exception e) {
//			e.printStackTrace();
//		}
//		
//		for (Client client : clients) {
//			dao.deleteClient(client);
//		}
////		try {
//			clients = dao.getAllClients();
//		} catch (Exception e) {
//			
//			e.printStackTrace();
//		}
////System.out.println(dao.insertClient(new Client("A","b","MALE","BLR",25,new java.util.Date(),new Timestamp(0))));
//		for (Client client : clients) {
//			System.out.println(client);
//		}
//		System.out.println(dao.deleteClient(dbClient));
	
	}
    
}
