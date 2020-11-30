package kr.ac.sejong.da.project;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

public class DatabaseMgr {
	
	// db 관련 변수
	Connection m_connection = null;
	Statement m_stmt = null;
	PreparedStatement m_pStmt = null;
	
	// getter 
	public Connection getConnection() { return m_connection;}		// 채수화
	public Statement getStatement() { return m_stmt; }				// 채수화
	public PreparedStatement getPreStatement() { return m_pStmt; }	// 채수화
	
	
	// setter
	
	// 초기화 함수, DB 서버 연결
	public void initialize(String port, String pswd) throws SQLException {	// 채수화
		m_connection = 
				DriverManager.getConnection("jdbc:mariadb://localhost:" + port, "root", pswd);
		m_stmt = m_connection.createStatement();
		m_pStmt = m_connection.prepareStatement("");
		
		//m_connection.setAutoCommit(false); // DB 자동 반영 끔
		
		// DB(그래프) 및 테이블(엣지,버텍스) 생성
		//m_pStmt.executeUpdate("CREATE OR REPLACE DATABASE Graph_Team3;");
		m_pStmt.executeUpdate("USE Graph_Team3;");
		//m_pStmt.executeUpdate("CREATE OR REPLACE TABLE Vertices (ID INTEGER PRIMARY KEY, Properties JSON);");
		//m_pStmt.executeUpdate("CREATE OR REPLACE TABLE Edges (OutV INTEGER NOT NULL, InV INTEGER NOT NULL,	Label VARCHAR(50) NULL, Properties JSON, PRIMARY KEY (OutV, Inv), FOREIGN KEY (OutV) REFERENCES Vertices(ID), FOREIGN KEY (InV) REFERENCES Vertices(ID));");
		
	}
	
	// 종료 함수, DB 서버 연결 종료
	public void release() throws SQLException {	// 채수화
		if(m_stmt != null) m_stmt.close();
		if(m_connection != null) m_connection.close();
	}
	
	// 싱글톤 
	private static DatabaseMgr m_instance;
	
	private DatabaseMgr() {};
	
	public static DatabaseMgr getInstance() {	// 채수화
		if(m_instance == null) 
			m_instance = new DatabaseMgr();
			
		return m_instance;
	}
}
