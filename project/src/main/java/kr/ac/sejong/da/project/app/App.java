package kr.ac.sejong.da.project.app;

import java.sql.SQLException;
import java.util.Iterator;

import kr.ac.sejong.da.project.DatabaseMgr;
import kr.ac.sejong.da.project.Direction;
import kr.ac.sejong.da.project.Edge;
import kr.ac.sejong.da.project.Graph;
import kr.ac.sejong.da.project.Vertex;
import kr.ac.sejong.da.project.impl.JGraph;
import kr.ac.sejong.da.project.impl.JVertex;

public class App {
	public static void main(String[] args) throws SQLException {
		// DB 연결 매니저
		//DatabaseMgr dbMgr = DatabaseMgr.getInstance();
		//dbMgr.initialize("3306", "0000"); 
		
		//Vertex v = new JVertex();
		//v.setStatement(dbMgr.getStatement());
		
		Graph g = new JGraph();
        Vertex v1 = g.addVertex("1");
        Vertex v2 = g.addVertex("2");
        Vertex v3 = g.addVertex("3");
        
        Edge e1 = g.addEdge(v2, v1, "label");
        Edge e2 = g.addEdge(v2, v3, "label");

        Iterator<Edge> ei1 = v2.getEdges(Direction.OUT).iterator();
        while(ei1.hasNext()){
            Edge e =ei1.next();
            System.out.println(e);
        }
        
//        Iterator<Edge> ei2 = v2.getEdges(Direction.IN, "label").iterator();
//        while(ei2.hasNext()){
//            Edge e = ei2.next();
//            System.out.println(e);
//        }

        // 11.23 확인 결과 터짐
        // System.out.println(e1.getVertex(Direction.OUT)); 
        // System.out.println(e1.getVertex(Direction.IN)); 

        v1.setProperty("x", 300);
        v1.setProperty("y", 300);
        v1.setProperty("y", 200);
        
        // edge Property 값 설정
		e1.setProperty("x", 600);
		e1.setProperty("y", 700);
		e2.setProperty("v", 400);
		e2.setProperty("w", 500);
		
		// autoCommit 꺼져(false)있으면, get 하기전에 DB 적용시키는 방법
		//dbMgr.getConnection().commit();  
		
		e1.getProperty("x"); // edge Property값 가져오기
		e2.getPropertyKeys(); //edge Property의 key 값 가져오기
		
		DatabaseMgr.getInstance().release();
	}
}
