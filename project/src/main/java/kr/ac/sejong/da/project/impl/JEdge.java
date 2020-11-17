package kr.ac.sejong.da.project.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Set;

import kr.ac.sejong.da.project.DatabaseMgr;
import kr.ac.sejong.da.project.Direction;
import kr.ac.sejong.da.project.Edge;
import kr.ac.sejong.da.project.Vertex;

public class JEdge implements Edge {

	public String id; // outID|label|inID 형태의 고유 아이디

	// 쿼리문 사용하기 위해 가져옴
	public Connection m_connection = null;
	private Statement m_stmt = null;
	
	JEdge() {
		m_connection = DatabaseMgr.getInstance().getConnection();
	}
	
	// setter
	public void setStatement(Statement stmt) { m_stmt = stmt; }
	
	public void setID(String outV, String inV, String label) {
		id = outV + "|" + label + "|" + inV;
	}

	@Override // Vertices테이블에서 하나의 vertex를 가져오는거 같은데 direction을 어떻게 쿼리문을 가져올지 몰라서 일단은 select *
	// from vertices; 를씀
	public Vertex getVertex(Direction direction) throws SQLException { // Vertex가져와서 반환하는 메소드
		
		Vertex vertex = null;	// 없으면 NULL
		ResultSet rs = m_stmt.executeQuery("SELECT * FROM Vertices;");
		// Vertices테이블에서 하나의 vertex를 가져오는거 같은데 direction을 어떻게 쿼리문을 가져올지 몰라서 일단은 select *
		// from vertices; 를씀

		Object ID = rs.getObject("ID");
		Object Properties = rs.getObject("Properties");

		if (ID == null)
			return null;
		vertex.setProperty("ID", ID);
		vertex.setProperty("Properties", Properties);

		return vertex;
	}

	@Override
	public String getLabel() throws SQLException { // db에서 Label을 select label from edges; 가져오는 테이블
		ResultSet rs = m_stmt.executeQuery("SELECT Label FROM Edges;"); // edges테이블에서 label을 반환하는데 label이 여러개가 있는데 왜
																		// string배열을 안쓰고 그냥 string으로 반환하냐? 의문점..
		String label = rs.getString("Label");

		if (label == null)
			return null;

		return label;
	}

	@Override // property가 테이블에 다 존재 하는데 일단은 뭔지 몰라서 edges에만 넣어둠
	public Object getProperty(String key) throws SQLException { // select key from graph where key=?
		PreparedStatement pstmt = m_connection.prepareStatement("SELECT ? FROM Edges;");
		pstmt.setString(1, key);
		
		ResultSet rs = pstmt.executeQuery();
		Object result = rs.getObject(key);

		if (result == null)
			return null;

		return result;
	}

	@Override
	public Set<String> getPropertyKeys() throws SQLException { // 이해못했는데 컬럼명을 가져오는 거 같음... 확실치 않아요
		ResultSet rs = m_stmt
				.executeQuery("SELECT column_name FROM information_schema.columns WHERE table_schema = 'DB이름넣기';");
		Set<String> result = new HashSet<>();
		while (rs.next()) {
			result.add(rs.getString(1));
		}

		return result;
	}

	@Override // property가 테이블에 다 존재 하는데 일단은 뭔지 몰라서 edges에만 넣어둠
	public void setProperty(String key, Object value) throws SQLException { // select key from edges
		PreparedStatement pstmt = m_connection
				.prepareStatement("INSERT INTO Edges (?) VALUES (?) ON DUPLICATE KEY UPDATE ?=?"); // 존재하는 값이 있으면
																									// update하고 없으면
																									// insert
		pstmt.setString(1, key);
		pstmt.setObject(2, value);
		pstmt.setString(3, key);
		pstmt.setObject(4, value);
		pstmt.executeUpdate();

	}

	@Override // 뭔지 몰라서 this.id로 반환
	public Object getId() {
		return this.id;
	}
}
