package kr.ac.sejong.da.project.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.json.JSONObject;

import kr.ac.sejong.da.project.DatabaseMgr;
import kr.ac.sejong.da.project.Direction;
import kr.ac.sejong.da.project.Edge;
import kr.ac.sejong.da.project.Vertex;

public class JVertex implements Vertex {

    //예: string 형태의 고유 아이디, '|' 사용 금지
    private String m_id; // DB에서는 INT형
    
    // 쿼리문 사용하기 위해 가져옴
	private Statement m_stmt = null;
    
	JVertex() {
		super();
		if(m_stmt == null)	// Statement Null 이면 DBMgr에서 가져옴
			m_stmt = DatabaseMgr.getInstance().getStatement();
	}
	
	// setter
    public void setStatement(Statement stmt) { m_stmt = stmt; }
    public void setID(String id) { m_id = id; }	// 채수화
    
    // getter
    @Override
    public Object getId() { return m_id; }	// 채수화
    
    @Override	// 현재 버텍스가 인자 방향(Out,In)에 해당하는 Edges 모두 가져옴
    public Iterable<Edge> getEdges(Direction direction, String... labels) throws SQLException {	// 채수화
    	// SQL 구문 실행
    	String sql = "";
    	if(direction == Direction.IN)
    		sql = "SELECT * FROM Edges WHERE InV = " + m_id;
    	else if(direction == Direction.OUT)
    		sql = "SELECT * FROM Edges WHERE OutV = " + m_id;
    	/*
		 * else // BOTH // BOTH 일 경우 생각해보기..
		 */  
    	
    	// labels 개수: 0 ~ 여러개 
    	if(labels.length <= 0) 		// labels 인자가 들어오지 않음
    		sql += ";";
    	else {						// labels 인자 한 개 이상
    		sql += " AND Label = \"" + labels[0] + "\"";
    		for(int i = 1; i < labels.length; ++i) 
    			sql += " OR Label = \"" + labels[i] + "\"";
    		sql += ";";
    	}
  
    	ResultSet rs = m_stmt.executeQuery(sql);
    	
    	// 결과 담을 리스트 생성
    	List<Edge> result = new ArrayList<Edge>();
    	
    	while(rs.next()) {
    		// 가져온 결과가 null 일 경우 처리 해줘야함(인덱싱?)
    		String outV = rs.getString(1);
    		String inV = rs.getString(2);
    		String label = rs.getString(3);
    		String prop = rs.getString(4);
    		
    		Edge eTemp = new JEdge();
    		((JEdge) eTemp).setID(outV, inV, label);
    		
    		if(prop != null) {	// property가 null 아닐 때
    			JSONObject jObj = new JSONObject(prop);	// 라이브러리 사용
    			Iterator<String> iter = jObj.keys();
    			
    			while(iter.hasNext()) {
    				String key = (String)iter.next();
    				eTemp.setProperty(key, jObj.get(key));
    			}	
    		}
    		
    		result.add(eTemp);
    	}
    	
    	if(result.isEmpty()) // 결과가 없을 경우, null 반환
    		return null;
    	
        return result;
    }

    @Override	// 현재 버텍스와 해당 방향(Out,In) 으로 연결된 Vertex 모두 가져옴
    public Iterable<Vertex> getVertices(Direction direction, String... labels) throws SQLException {	// 채수화
    	String sql = "";
    	if(direction == Direction.IN)
    		sql = "SELECT OutV FROM Edges WHERE InV = " + m_id;
    	else if(direction == Direction.OUT)
    		sql = "SELECT InV FROM Edges WHERE OutV = " + m_id;
    	/*
		 * else // BOTH // BOTH 일 경우 생각해보기..
		 */  
    	
    	// labels 개수: 0 ~ 여러개 
    	if(labels.length <= 0) 		// labels 인자가 들어오지 않음
    		sql += ";";
    	else {						// labels 인자 한 개 이상
    		sql += " AND Label = \"" + labels[0] + "\"";
    		for(int i = 1; i < labels.length; ++i) 
    			sql += " OR Label = \"" + labels[i] + "\"";
    		sql += ";";
    	}
 
    	ResultSet rs = m_stmt.executeQuery(sql);
    	
    	// 결과 담을 리스트 생성
    	List<Vertex> result = new ArrayList<Vertex>();
    	
    	while(rs.next()) {
    		// 가져온 결과가 null 일 경우 처리 해줘야함(인덱싱?)
    		String id = rs.getString(1);
    		String prop = rs.getString(2);
    		
    		Vertex vTemp = new JVertex();
    		((JVertex) vTemp).setID(id);
    		
    		if(prop != null) {	// property가 null 아닐 때
    			JSONObject jObj = new JSONObject(prop);	// 라이브러리 사용
    			Iterator<String> iter = jObj.keys();
    			
    			while(iter.hasNext()) {
    				String key = (String)iter.next();
    				vTemp.setProperty(key, jObj.get(key));
    			}	
    		}
    	
    		result.add(vTemp);
    	}
    	
    	if(result.isEmpty()) // 결과가 없을 경우, null 반환
    		return null;
    	
    	return result;
    }

    @Override	// 현재 버텍스가 OutV, 인자가 InV에 해당하는 Edge 추가
    public Edge addEdge(String label, Vertex inVertex) throws SQLException {	// 채수화
    	int outID = Integer.parseInt(m_id);
    	int inID = Integer.parseInt((String) inVertex.getId());
    	
    	// DB 삽입
    	String sql = "INSERT INTO Edges SET OutV = " + outID + ", InV = " + inID + ", Label = \"" + label + "\";"; 
    	if(0 == m_stmt.executeUpdate(sql)) // 삽입 오류 발생 시(중복 등),
    		return null; 					// null 반환
    	
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

    @Override	// 기존 property 유지하면서 추가, key 중복 시 value 업데이트
    public void setProperty(String key, Object value) throws SQLException {	// 채수화
    	// 1. 기존 property 가져오기
    	// 2. 라이브러리 이용, jsonObject로 파싱하기
    	// 3. put 함수 이용해 key, value 추가 또는 value 값 수정
    	// 4. String 으로 변환하기
    	// 5. SQL 구문으로 DB에 넣기
    	
    	
    	int numID = Integer.parseInt(m_id);
    	String strJson = "{ \"" + key + "\" : " + value + "\"}"; 
    	
    	m_stmt.executeUpdate("UPDATE Vertices SET Properties ('"+ strJson + "') "
    			+ "WHERE ID = " + numID + ";");
    }
}
