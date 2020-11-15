package kr.ac.sejong.da.project.impl;

import java.sql.SQLException;

import kr.ac.sejong.da.project.DatabaseMgr;
import kr.ac.sejong.da.project.Vertex;

public class App {
    public static void main(String[] args) throws SQLException{
    	
    	// DB 서버 연결
    	DatabaseMgr dbMgr = DatabaseMgr.getInstance();
    	dbMgr.getInstance().initialize("3307", "1111"); // 각자 사용하는 포트, 비번 작성
    	
    	// 예를 들어 버텍스에서 쿼리문 사용
    	JVertex v = new JVertex();
    	// dbMgr에서 이미 db서버와 연동된 Statement 사용함
    	v.setStatement(dbMgr.getInstance().getStatement());
    	
    	
		/*
		 * Graph g = new JGraph(); Vertex v1 = g.addVertex("1"); Vertex v2 =
		 * g.addVertex("2"); Vertex v3 = g.addVertex("3");
		 * 
		 * Edge e1 = g.addEdge(v2, v1, "label"); Edge e2 = g.addEdge(v2, v3, "label");
		 * 
		 * Iterator<Edge> ei1 = v2.getEdges(Direction.OUT, "label").iterator(); //결과는
		 * v1, v3를 포함한 iterator while(ei1.hasNext()){ Edge e =ei1.next();
		 * System.out.println(e); } Iterator<Edge> ei2 = v2.getEdges(Direction.IN,
		 * "label").iterator(); //결과는 null while(ei2.hasNext()){ Edge e = ei2.next();
		 * System.out.println(e); }
		 * 
		 * System.out.println(e1.getVertex(Direction.OUT)); //결과는 v2
		 * System.out.println(e1.getVertex(Direction.IN)); //결과는 v1
		 * 
		 * v1.setProperty("x", 300); v1.setProperty("y", 300);
		 */

    }
}
