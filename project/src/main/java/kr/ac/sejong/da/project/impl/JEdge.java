package kr.ac.sejong.da.project.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.json.JSONObject;

import kr.ac.sejong.da.project.DatabaseMgr;
import kr.ac.sejong.da.project.Direction;
import kr.ac.sejong.da.project.Edge;
import kr.ac.sejong.da.project.Vertex;

public class JEdge implements Edge {

	// OutV,inV,label 전역변수 설정
	public int out, in;
	public String lab;

	public Connection m_connection = null;
	private Statement m_stmt = null;

	JEdge() {
		if (m_connection == null)
			m_connection = DatabaseMgr.getInstance().getConnection();
		if (m_stmt == null)
			m_stmt = DatabaseMgr.getInstance().getStatement();
	}

	// setter
	public void setStatement(Statement stmt) {
		m_stmt = stmt;
	}

	public void setID(String outV, String inV, String label) { // OutV, inV,label 값 설정
		out = Integer.parseInt(outV);
		in = Integer.parseInt(inV);
		lab = label;

	}

	@Override
	// Vertices테이블에서 하나의 vertex를 가져오는거 같은데 direction을 어떻게 쿼리문을 가져올지 몰라서 일단은 select *
	// from vertices; 를씀
	public Vertex getVertex(Direction direction) throws SQLException { // Vertex가져와서 반환하는 메소드
		// 없으면 NULL
		Vertex vertex = null;

		String sql = "SELECT * FROM Vertices WHERE ID = ";
		if (direction == Direction.IN)
			sql += in;
		else if (direction == Direction.OUT)
			sql += out;

		ResultSet rs = m_stmt.executeQuery(sql);

	      while(rs.next()) {
	          String ID = rs.getString("ID");         
	          vertex = new JVertex();
	          ((JVertex) vertex).setID(ID);
	          return vertex;
	       }
	       return null;
	}

	@Override
	public String getLabel() throws SQLException { // db에서 Label을 select label from edges; 가져오는 테이블
		ResultSet rs = m_stmt.executeQuery("SELECT Label FROM Edges;");
		String label = rs.getString("Label");

		if (label == null)
			return null;

		return label;
	}

	@Override // property key의 value 값 가져오기
	public Object getProperty(String key) throws SQLException {
		// 현재 OutV와 InV에 맞는 Properties select하는 쿼리
		String sql = "SELECT Properties FROM edges WHERE OutV = " + out + " AND InV = " + in
				+ " AND NOT Properties IS NULL;"; // 없다면 NULL
		ResultSet rs = m_stmt.executeQuery(sql);

		// JSON 형태를 이용하기 위해 사용
		JSONObject jObj = null;
		if (!rs.next())
			jObj = new JSONObject();
		else
			jObj = new JSONObject(rs.getString(1));

		// Properties 내용 반복문으로 가져오기
		Iterator<String> iter = jObj.keys();
		while (iter.hasNext()) {
			if (iter.next() == key) {
				jObj.remove(key);
				break;
			}
		}
		return jObj.getInt(key);
	}

	@Override
	public Set<String> getPropertyKeys() throws SQLException { // property의 key값 모두 가져오기
		Set<String> set = new HashSet<>();
		// 현재 OutV와 InV에 맞는 Properties select하는 쿼리
		String sql = "SELECT Properties FROM edges WHERE OutV = " + out + " AND InV = " + in
				+ " AND NOT Properties IS NULL;"; // 없다면 NULL
		ResultSet rs = m_stmt.executeQuery(sql);

		// JSON 형태를 이용하기 위해 사용
		JSONObject jObj = null;
		if (!rs.next())
			jObj = new JSONObject();
		else
			jObj = new JSONObject(rs.getString(1));

		// Properties의 key값을 반복문으로 가져오기
		Iterator<String> iter = jObj.keys();
		while (iter.hasNext()) {
			String str = iter.next();
			
			if (!set.contains(str)) {
				set.add(str);
			}
		}
		
		return set;
	}

	@Override // insert Properties 쿼리
	public void setProperty(String key, Object value) throws SQLException {

		// 현재 OutV와 InV에 맞는 Properties select하는 쿼리
		String sql = "SELECT Properties FROM edges WHERE OutV = " + out + " AND InV = " + in
				+ " AND NOT Properties IS NULL;"; // 없다면 NULL
		ResultSet rs = m_stmt.executeQuery(sql);

		// JSON 형태를 이용하기 위해 사용
		JSONObject jObj = null;
		if (!rs.next())
			jObj = new JSONObject();
		else
			jObj = new JSONObject(rs.getString(1));

		// Properties 내용 반복문으로 가져오기
		Iterator<String> iter = jObj.keys();
		while (iter.hasNext()) {
			if (iter.next() == key) {
				jObj.remove(key);
				break;
			}
		}

		jObj.put(key, value);
		String strJson = jObj.toString();

		m_stmt.executeUpdate("INSERT INTO Edges VALUES (" + out + "," + in + "," + lab + ",'" + strJson
				+ "') ON DUPLICATE KEY UPDATE Properties = '" + strJson + "';"); // OutV, InV, Label 삽입쿼리

	}

	@Override
	public Object getId() {
		return out + " " + in + " " + lab;
	}
}
