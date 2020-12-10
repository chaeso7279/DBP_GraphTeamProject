package kr.ac.sejong.da.project.impl;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;

import org.json.JSONObject;

import kr.ac.sejong.da.project.DatabaseMgr;
import kr.ac.sejong.da.project.Edge;
import kr.ac.sejong.da.project.Graph;
import kr.ac.sejong.da.project.Vertex;

public class JGraph implements Graph {
	// 쿼리문 사용하기 위해 가져옴
	private Statement m_stmt = null;
	
	public void setStatement(Statement stmt) {
		m_stmt = stmt;
	}

	public JGraph() {
		super();
	
		// DB 연결
		DatabaseMgr dbMgr = DatabaseMgr.getInstance();
		try {
			//dbMgr.initialize("3306", "0000");
			dbMgr.initialize("3307", "1111");
		} catch (SQLException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		} 
		
		if (m_stmt == null) { // Statement Null 이면 DBMgr에서 가져옴
			m_stmt = DatabaseMgr.getInstance().getStatement();
		}
	}
	
	public void finalize() {
		try {
			DatabaseMgr.getInstance().release();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	@Override //db에 인자 id를 가진 vertex 추가 후 객체 반환
    public Vertex addVertex(String id) { //손혜원
        try {
        	int intID = Integer.parseInt(id);
        	m_stmt.executeUpdate("INSERT INTO vertices SET ID=" + intID + ";");
        	
        	JVertex v = new JVertex();
        	v.setID(id); //id만 세팅
        	
          	return v;
        } catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override // db에서 인자 id를 가진 vertex를 찾아 객체 반환
	public Vertex getVertex(String id) { // 손혜원
		try {
			int intID = Integer.parseInt(id);
			ResultSet rs = m_stmt.executeQuery("SELECT * FROM vertices WHERE ID=" + intID);

			if( !rs.next() ) {
				return null;
			}
			
			JVertex v = new JVertex();
			v.setID(id);
			
			while (rs.next()) {
				String line = rs.getString(2); //vertex의 property를 받아옴
				
				if(line!=null) { //NullPointerException 오류 처리
					JSONObject arr = new JSONObject(line);

					//v의 key와 property를 db에서 받아온 vertex와 동일하게 세팅
					Iterator<String> iter = arr.keys();
					
					while(iter.hasNext()) {
						String key = (String)(iter.next());
	    				v.setProperty(key, arr.get(key));
	    			}
				}
			}

			return v;
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return null;
	}

	@Override // 현재 db에 존재하는 모든 vertex를 반환
	public Iterable<Vertex> getVertices() { // 손혜원
		try {
			ResultSet rs = m_stmt.executeQuery("SELECT * FROM vertices;");
			List<Vertex> vertexData = new ArrayList<Vertex>();

			while (rs.next()) {
				JVertex v = new JVertex();
				String id = rs.getString(1);
				String line = rs.getString(2); //vertex의 property를 받아옴
				
				v.setID(id);
				
				if(line!=null) { //NullPointerException 오류 처리
					JSONObject arr = new JSONObject(line);

					//v의 key와 property를 db에서 받아온 vertex와 동일하게 세팅
					Iterator<String> iter = arr.keys();
					
					while(iter.hasNext()) {
						String key = (String)(iter.next());
	    				v.setProperty(key, arr.get(key));
	    			}
				}
				vertexData.add(v);
			}
			return vertexData;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
        return null;
    }

	@Override // 현재 db에 존재하는 vertex중, 특정 key와 value를 가지는 vertex 반환
	public Iterable<Vertex> getVertices(String key, Object value) { // 손혜원
		try {
			ResultSet rs = m_stmt.executeQuery("SELECT ID, JSON_VALUE(Properties,'$." + key + "')"
					+  "FROM vertices WHERE Properties IS NOT NULL;");
			
			List<Vertex> vertexData = new ArrayList<Vertex>();

			while (rs.next()) {
				JVertex v = new JVertex();
				String id = rs.getString(1);
				String line = rs.getString(2);
				
				v.setID(id); 

				if(line!=null && line.equals(value.toString())) { //NullPointerException 오류 처리
					v = (JVertex) this.getVertex(id);
					vertexData.add(v);
				}
			}
			return vertexData;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	// 버텍스객체 두개와 Label을 값으로 지니는 edge 생성->(DB)삽입->반환
	public Edge addEdge(Vertex outVertex, Vertex inVertex, String label) { // 김현모
		Object outID = outVertex.getId();
		Object inID = inVertex.getId();
		JEdge edge = new JEdge();

		try {
			m_stmt.executeUpdate("INSERT INTO edges SET OutV=" + outID + ", InV=" + inID + ",Label='" + label + "';");
			edge.setID((String) outID, (String) inID, label);
			return edge;
		} catch (SQLException e) {
			e.printStackTrace();
		}

		return null;
	}

	@Override
	// 버텍스객체 두개를 잇고 label값을 갖는 edge 반환
	public Edge getEdge(Vertex outVertex, Vertex inVertex, String label) { // 김현모
		ResultSet rs;
		Object outID = outVertex.getId();
		Object inID = inVertex.getId();
		JEdge edge = new JEdge();

		try {
			rs = m_stmt.executeQuery("SELECT * FROM edges " + "WHERE OutV=" + outID + " and " + "InV=" + inID + " and "
					+ "label='" + label + "';");
			rs.next();
			edge.setID((String) outID, (String) inID, label);
			return edge;
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}

	@Override
	// 모든 edge 반환
	public Iterable<Edge> getEdges() { // 김현모
		// List 컬렉션에 모든 엣지들을 추가해서 컬렉션자체를 리턴 (메인에서 iterator 생성)
		List<Edge> edgeData = new ArrayList<Edge>();
		ResultSet rs;

		try {
			rs = m_stmt.executeQuery("SELECT * FROM edges;");
			while (rs.next()) {
				// JEdge객체를 생성하고, 고유ID를 setting 후에 컬렉션에 붙이기
				String outID = rs.getString(1);
				String inID = rs.getString(2);
				String label = rs.getString(3);
				JEdge e = new JEdge();
				e.setID(outID, inID, label);
				edgeData.add(e);
			}
			return edgeData;
		} catch (SQLException e1) {
			e1.printStackTrace();
		}
		return null;
	}

	@Override
	// key와 value의 property를 가진 edge를 반환
	public Iterable<Edge> getEdges(String key, Object value) { // 김현모
		List<Edge> edgeData = new ArrayList<Edge>();
		try {
			ResultSet rs = m_stmt.executeQuery("select OutV,InV,Label from edges where JSON_VALUE(properties,'$.\""+key+"\"')='"+value+"';");
			while (rs.next()) {
				String outID = rs.getString(1);
				String inID = rs.getString(2);
				String label = rs.getString(3);
				JEdge e = new JEdge();
				e.setID(outID, inID, label);
				edgeData.add(e);
			}
			return edgeData;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public void getReachableVertices() throws SQLException, IOException {

		BufferedWriter w = new BufferedWriter(new FileWriter("d:\\rv(reduced).txt"));
		
		String sql = "WITH RECURSIVE CTE AS ( SELECT InV, OutV FROM Edges WHERE OutV = (?) UNION SELECT a.InV, a.OutV FROM Edges a INNER JOIN CTE b ON a.OutV = b.InV) SELECT COUNT(DISTINCT InV) FROM CTE;";
		PreparedStatement pstmt = DatabaseMgr.getInstance().getConnection().prepareStatement(sql);
		
		Iterator<Vertex> iter = getVertices().iterator();
		while (iter.hasNext()) {
			// 각 vertex에 대해 
			Vertex v = iter.next();
	
			pstmt.setInt(1, Integer.parseInt((String) v.getId()));
			
			ResultSet rs = pstmt.executeQuery();
			if(rs.next()) {
				System.out.println(v.toString() + " : " + rs.getInt(1));
				w.write(v.toString() + " : " + rs.getInt(1) + "\n");
			}		
		}
		w.close();
	}
}
