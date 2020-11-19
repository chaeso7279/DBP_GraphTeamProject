package kr.ac.sejong.da.project.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import kr.ac.sejong.da.project.DatabaseMgr;
import kr.ac.sejong.da.project.Direction;
import kr.ac.sejong.da.project.Edge;
import kr.ac.sejong.da.project.Vertex;

public class JVertex implements Vertex {

    //예: string 형태의 고유 아이디, '|' 사용 금지
    private String m_id; // DB에서는 INT형
    
    // 쿼리문 사용하기 위해 가져옴
	private Statement m_stmt = null;
    
	// setter
    public void setStatement(Statement stmt) { m_stmt = stmt; }
    public void setID(String id) { m_id = id; }
    
    // getter
    @Override
    public Object getID() { return m_id; }
    
    @Override	// 현재 버텍스가 인자 방향(Out,In)에 해당하는 Edges 모두 가져옴
    public Iterable<Edge> getEdges(Direction direction, String... labels) throws SQLException {
    	//int id = Integer.parseInt(m_id);
    	if(m_stmt == null) // Statement Null 이면 DBMgr에서 가져옴
    		m_stmt = DatabaseMgr.getInstance().getStatement();
    	
    	// SQL 구문 실행
    	String sql = "SELECT * FROM Edges WHERE ";
    	if(direction == Direction.IN)
    		sql += "InV = " + m_id +";";
    	else if(direction == Direction.OUT)
    		sql += "OutV = " + m_id +";";
    	else // BOTH
    		sql += "OutV = " + m_id + "OR InV = " + m_id +";";
    	ResultSet rs = m_stmt.executeQuery(sql);
    	
    	// 결과 담을 리스트 생성
    	List<Edge> result = new ArrayList<Edge>();
    	
    	while(rs.next()) {
    		// 가져온 결과가 null 일 경우 처리 해줘야함(인덱싱?)
    		String outV = rs.getString(1);
    		String inV = rs.getString(2);
    		String label = rs.getString(3);
    		//String prop = rs.getString(4);
    		
    		Edge eTemp = new JEdge();
    		((JEdge) eTemp).setID(outV, inV, label);
    		//eTemp.setProperty(); // JSON 라이브러리 사용 여부
    		
    		result.add(eTemp);
    	}
    	
    	if(result.isEmpty()) // 결과가 없을 경우, null 반환
    		return null;
    	
        return result;
    }

    @Override	// 현재 버텍스와 해당 방향(Out,In) 으로 연결된 Vertex 모두 가져옴
    public Iterable<Vertex> getVertices(Direction direction, String... labels) throws SQLException {
    	if(m_stmt == null) // Statement Null 이면 DBMgr에서 가져옴
    		m_stmt = DatabaseMgr.getInstance().getStatement();
    	
    	String sql = "";
    	if(direction == Direction.IN) 
    		sql = "SELECT OutV FROM Edges WHERE InV " + m_id + ";";
    	else if(direction == Direction.OUT)
    		sql = "SELECT InV FROM Edges WHERE OutV " + m_id + ";";
		/*
		 * else // BOTH // BOTH 일 경우 생각해보기..
		 */    	
		
    	ResultSet rs = m_stmt.executeQuery(sql);
    	
    	// 결과 담을 리스트 생성
    	List<Vertex> result = new ArrayList<Vertex>();
    	
    	while(rs.next()) {
    		// 가져온 결과가 null 일 경우 처리 해줘야함(인덱싱?)
    		String id = rs.getString(1);
    		//String prop = rs.getString(2);
    		
    		Vertex vTemp = new JVertex();
    		((JVertex) vTemp).setID(id);
    		
    		//vTemp.setProperty(); // JSON 라이브러리 사용 여부
    		
    		result.add(vTemp);
    	}
    	
    	if(result.isEmpty()) // 결과가 없을 경우, null 반환
    		return null;
    	
    	return result;
    }

    @Override	// 현재 버텍스가 OutV, 인자가 InV에 해당하는 Edge 추가
    public Edge addEdge(String label, Vertex inVertex) throws SQLException {
    	if(m_stmt == null) // Statement Null 이면 DBMgr에서 가져옴
    		m_stmt = DatabaseMgr.getInstance().getStatement();
    	
    	int OutID = Integer.parseInt(m_id);
    	int InID = Integer.parseInt((String) inVertex.getId());
    	
    	// DB 삽입
    	String sql = "INSERT INTO Edges (OutV, InV, Label) VALUES (" + OutID + "," + InID + "," + label + ");";
    	m_stmt.executeUpdate(sql);
    	// 만약 삽입 오류 발생(중복 삽입 등) 시 상황 추가 해줘야함!!
    	
    	
    	// Edge 객체 생성
    	Edge eTemp = new JEdge();
    	((JEdge) eTemp).setID(m_id, (String) inVertex.getId(), label);
    	
        return eTemp;
    }

    @Override
    public Object getProperty(String key) {
        return null;
    }

    @Override
    public Set<String> getPropertyKeys() {
        return null;
    }

    @Override	// 기존에 있던 Property에 추가하는 형태? 초기화하고 Set하는 형태?
    public void setProperty(String key, Object value) throws SQLException {
    	// UPDATE 구문 사용
    	// UPDATE Vertices SET Properties = "" WHERE ID = "";
    	if(m_stmt == null) // Statement Null 이면 DBMgr에서 가져옴
    		m_stmt = DatabaseMgr.getInstance().getStatement();
    	
    	int numID = Integer.parseInt(m_id);
    	String strJson = "{ \"" + key + "\" : " + value + "\"}"; 
    	
    	m_stmt.executeUpdate("UPDATE Vertices SET Properties ('"+ strJson + "') "
    			+ "WHERE ID = " + numID + ";");
    }
}
