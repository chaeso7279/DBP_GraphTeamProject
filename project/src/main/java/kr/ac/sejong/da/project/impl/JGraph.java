package kr.ac.sejong.da.project.impl;

import kr.ac.sejong.da.project.DatabaseMgr;
import kr.ac.sejong.da.project.Edge;
import kr.ac.sejong.da.project.Graph;
import kr.ac.sejong.da.project.Vertex;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;


public class JGraph implements Graph {
	// 쿼리문 사용하기 위해 가져옴
    private Statement m_stmt = null;
    public void setStatement(Statement stmt) { m_stmt = stmt; }
    
	public JGraph() {
		super();
    	if(m_stmt == null) { // Statement Null 이면 DBMgr에서 가져옴
    		m_stmt = DatabaseMgr.getInstance().getStatement();
    	}
	}

	@Override //db에 인자 id를 가진 vertex 추가 후 객체 반환
    public Vertex addVertex(String id) { //손혜원
        try {
        	int intID = Integer.parseInt(id);
        	m_stmt.executeUpdate("INSERT INTO vertices SET ID=" + intID + ";");
        	
        	JVertex v = new JVertex();
        	v.setID(id);
        	
          	return v;
        } catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
    }

    @Override //db에서 인자 id를 가진 vertex를 찾아 객체 반환
    public Vertex getVertex(String id) { //손혜원
		try {
			int intID = Integer.parseInt(id);
			ResultSet rs = m_stmt.executeQuery("SELECT * FROM vertices WHERE ID=" + intID);
			
			JVertex v = new JVertex();
			while(rs.next()) {
				Object line = rs.getObject(2); 
				JSONArray arr = new JSONArray(line);
				
				String key = arr.getString(1); //한 쌍일때를 상정->보완 필요
				Object value = arr.get(3);
				
				v.setID(id);
				v.setProperty(key, value);
	          }
			
			return v;
		} catch (SQLException e) {
			e.printStackTrace();
		}
    	
    	return null;
    }

    @Override //현재 db에 존재하는 모든 vertex를 반환
    public Iterable<Vertex> getVertices() { //손혜원
    	try {
    		ResultSet rs = m_stmt.executeQuery("SELECT * FROM vertices;");
    		List<Vertex> vertexData = new ArrayList<Vertex>();
  
			while(rs.next()) {
				JVertex v = new JVertex();
				String id = rs.getString(1);
				Object line = rs.getObject(2); 
				JSONArray arr = new JSONArray(line);
				
				String key = arr.getString(1); //한 쌍일때를 상정->보완 필요
				Object value = arr.get(3);
				
				v.setID(id);
				v.setProperty(key, value);	
				vertexData.add(v);
				System.out.println(v.getId());
			}
			return vertexData;
		} catch (SQLException e) {
			e.printStackTrace();
		}
    	
        return null;
    }

    @Override //현재 db에 존재하는 vertex중, 특정 key와 value를 가지는 vertex 반환
    public Iterable<Vertex> getVertices(String key, Object value) { //손혜원
    	
    	try {
    		ResultSet rs = m_stmt.executeQuery("SELECT ID, JSON_VALUE(Properties,'$."+key+"'),  "
    				+ " JSON_VALUE(Properties,'$." + value + "')" + "FROM vertices;");
    		List<Vertex> vertexData = new ArrayList<Vertex>();
    		
			while(rs.next()) {
				JVertex v = new JVertex();
				String id = rs.getString(1);
				String k = rs.getString(2); //자료형이 나눠져 반환되므로
				Object val = rs.getObject(3);
				
				v.setID(id);
				v.setProperty(k, val);
				vertexData.add(v);
				System.out.println(v.getId());
			}
			return vertexData;
		} catch (SQLException e) {
			e.printStackTrace();
		}
        return null;
    }

    @Override
    public Edge addEdge(Vertex outVertex, Vertex inVertex, String label) {
    	// insert into edges value ();
    	Object outID = outVertex.getId();
    	Object inID = inVertex.getId();
    	Edge edge = null;
    	
    	try {
    		m_stmt.executeUpdate("INSERT INTO edges (OutV, InV, Label) "
					+ "VALUE(" + outID + inID + label + ");");
			//e.setProperty 해서 반환
			return edge;
		} catch (SQLException e) {
			e.printStackTrace();
		}
    	
        return null;
    }

    @Override
    public Edge getEdge(Vertex outVertex, Vertex inVertex, String label) {
    	ResultSet rs;
    	Object outID = outVertex.getId();
    	Object inID = inVertex.getId();
    	Edge edge = null;
    	
		try {
			rs = m_stmt.executeQuery("SELECT * FROM edges "
					+ "WHERE OutV=" + outID + " and " + "InV=" + inID + " and " + "label=" + label);
			
			//edge.setProperty 해서 반환
			return edge;
		} catch (SQLException e) {
			e.printStackTrace();
		}
        return null;
    }

    @Override
    // 그래프내의 모든 엣지를 반환하는 메소드
    public Iterable<Edge> getEdges() {
    	// List 컬렉션에 모든 엣지들을 추가해서 컬렉션자체를 리턴 (메인에서 iterator 생성)
    	List<Edge> edgeData = new ArrayList<Edge>();
    	ResultSet rs;
    	
    	try {
			rs = m_stmt.executeQuery("SELECT * FROM edges;");
			while(rs.next()) {
				// JEdge객체를 생성하고, 고유ID를 setting 후에 컬렉션에 붙이기
				// 질문 : property 처리는 어떻게 해야하나요..
				JEdge e = new JEdge();
				e.setID(rs.getString(1), rs.getString(2), rs.getString(3));
				edgeData.add(e);
			}
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
        return null;
    }

    @Override
    public Iterable<Edge> getEdges(String key, Object value) {
    	List<Edge> edgeData = new ArrayList<Edge>();
    	edgeData=null;
    	try {
    		// 제이슨 라이브러리 사용가능?
			ResultSet rs = m_stmt.executeQuery("select json_value(properties,'$."+key+"') from edges;");
			while(rs.next()) {
				String str = rs.getString(1);
				if(value.equals(str)) {
					JEdge e = new JEdge();
					//edge setting?
					edgeData.add(e);
				}
			}
			return edgeData;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
        return null;
    }
}
