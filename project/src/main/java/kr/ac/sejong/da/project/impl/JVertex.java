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
    
    public void setStatement(Statement stmt) { m_stmt = stmt; }
    
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
    		String outV = rs.getString(1);
    		String inV = rs.getString(2);
    		String label = rs.getString(3);
    		//String prop = rs.getString(4);
    		
    		Edge eTemp = new JEdge();
    		((JEdge) eTemp).setID(outV, inV, label);
    		//eTemp.setProperty(); // JSON 라이브러리 사용 여부
    		
    		result.add(eTemp);
    	}
    	
        return result;
    }

    @Override	// 해당 방향(Out,In) 으로 연결된 Vertex 모두 가져옴
    public Iterable<Vertex> getVertices(Direction direction, String... labels) { 
        return null;
    }

    @Override	// 현재 버텍스가 OutV, 인자가 InV에 해당하는 Edge 추가
    public Edge addEdge(String label, Vertex inVertex) {
        return null;
    }

    @Override
    public Object getProperty(String key) {
        return null;
    }

    @Override
    public Set<String> getPropertyKeys() {
        return null;
    }

    @Override
    public void setProperty(String key, Object value) {
    }

    @Override
    public Object getId() {
    	return null;
    }
}
