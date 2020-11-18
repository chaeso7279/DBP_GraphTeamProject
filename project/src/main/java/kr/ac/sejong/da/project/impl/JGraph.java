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
    public Vertex addVertex(String id) {
        try {
        	int intID = Integer.parseInt(id);
        	m_stmt.executeUpdate("INSERT INTO vertices (ID) VALUE(" + intID + ");");
        	
        	Vertex v = new JVertex();
        	v.setProperty(id, "property"); //property는 json으로, 추후 논의 필요
          	
          	return v;
        } catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
    }

    @Override
    public Vertex getVertex(String id) {
		try {
			int intID = Integer.parseInt(id);
			ResultSet rs = m_stmt.executeQuery("SELECT * FROM vertices WHERE ID=" + intID);
			
			Vertex v = new JVertex();
			while(rs.next()) {
				String ID = rs.getString(1); // id를 받아옴(인자 id 그대로 사용가능)
				String prop = rs.getString(2); // {key:value} 쌍을 받아옴
				
				v.setProperty(ID, prop); 
			}
			
			return v;
		} catch (SQLException e) {
			e.printStackTrace();
		}
    	
    	return null;
    }

    @Override //현재 db에 존재하는 모든 vertex를 반환
    public Iterable<Vertex> getVertices() {
    	try {
    		ResultSet rs = m_stmt.executeQuery("SELECT * FROM vertices;");
    		List<Vertex> vertexData = new ArrayList<Vertex>();
  
			while(rs.next()) {
				Vertex v = new JVertex();
				
				String ID = rs.getString(1); // id를 받아옴(인자 id 그대로 사용가능)
				String prop = rs.getString(2); // {key:value} 쌍을 받아옴
				v.setProperty(ID, prop); 
				
				vertexData.add(v);
			}
			return vertexData;
		} catch (SQLException e) {
			e.printStackTrace();
		}
    	
        return null;
    }

    @Override //현재 db에 존재하는 vertex중, 특정 key와 value를 가지는 vertex 반환
    public Iterable<Vertex> getVertices(String key, Object value) {
    	try {
    		ResultSet rs = m_stmt.executeQuery("SELECT * FROM vertices WHERE ID="
    				+ key + "AND Properties=" + value + ";");
    		
    		List<Vertex> vertexData = new ArrayList<Vertex>();
  
			while(rs.next()) {
				Vertex v = new JVertex();
				
				String id = (String) rs.getObject(1);
				Object prop = rs.getObject(2);
				v.setProperty(id, prop);

				vertexData.add(v);
				//System.out.print(rs.getObject(1)); //확인용 코드
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
    public Iterable<Edge> getEdges() {
        return null;
    }

    @Override
    public Iterable<Edge> getEdges(String key, Object value) {
        return null;
    }
}
