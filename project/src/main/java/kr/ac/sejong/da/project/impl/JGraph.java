package kr.ac.sejong.da.project.impl;

import kr.ac.sejong.da.project.Edge;
import kr.ac.sejong.da.project.Graph;
import kr.ac.sejong.da.project.Vertex;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

//null point에 대해 처리할것인지? 그런 상황은 없다고 가정하는지 궁금합니당

public class JGraph implements Graph {
    
	// 쿼리문 사용하기 위해 가져옴
    private Statement m_stmt = null;
    
    public void setStatement(Statement stmt) { m_stmt = stmt; }
    
	public JGraph() {
		super();
	}

	@Override
    public Vertex addVertex(String id) {
		int intID = Integer.parseInt(id);
		Vertex v = null;
				
        try {
        	m_stmt.executeUpdate("INSERT INTO vertices (ID) VALUE(" + intID + ");");
          	//!!!!!vertex 변수 하나 선언해서 거기에 추가하고 return하는것인지..?-> 디자인 추후 논의!!!!!
          	//!!!!!vertex class에 v.setID 메소드가 없는게 맞나요? (아래 모두 마찬가지)
          	return v;
        } catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
    }

    @Override
    public Vertex getVertex(String id) {
    	ResultSet rs;
    	int intID = Integer.parseInt(id);
    	Vertex v = null;
    	
		try {
			rs = m_stmt.executeQuery("SELECT * FROM vertices WHERE ID=" + intID);
			//확인용으로  출력했던 코드
			/*  while(rs.next()) {
		    	Object vetID = rs.getObject(1);
				System.out.println(vetID);
			}*/
			
			//v 세팅 후 반환
			return v;
		} catch (SQLException e) {
			e.printStackTrace();
		}
    	
    	return null;
    }

    @Override
    public Iterable<Vertex> getVertices() {
    	//vertex 리스트를 생성하여 그 리스트를 가리키는 이터레이터 반환..?
    	//!!!!!리스트를 메모리에 유지하지 말라 하셨는데 그 부분 논의
    	List<Vertex> vertexData = new ArrayList<Vertex>();
    	Iterator<Vertex> iter;
    	ResultSet rs;
    	Vertex v = null;
   
    	try {
    		rs = m_stmt.executeQuery("SELECT * FROM vertices;");
			while(rs.next()) {
				//v 세팅 후 추가?
				//v.setID(rs.getObject(1)); v.setProperty(rs.getObject(2));
				vertexData.add(v);
				//System.out.print(rs.getObject(1)); //확인용 코드
			}
			iter = vertexData.iterator();
			
			return (Iterable<Vertex>) iter;
		} catch (SQLException e) {
			e.printStackTrace();
		}
    	
        return null;
    }

    @Override
    public Iterable<Vertex> getVertices(String key, Object value) {
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
			//e.setID 해서 반환
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
			
			//edge 세팅 후 반환 -> !!!!!vertex와 같은 이슈 존재
			//edge.setID();
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
