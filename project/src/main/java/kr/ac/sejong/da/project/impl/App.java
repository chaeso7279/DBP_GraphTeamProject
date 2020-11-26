package kr.ac.sejong.da.project.impl;

import java.sql.SQLException;
import java.util.Iterator;

import kr.ac.sejong.da.project.DatabaseMgr;
import kr.ac.sejong.da.project.Direction;
import kr.ac.sejong.da.project.Edge;
import kr.ac.sejong.da.project.Graph;
import kr.ac.sejong.da.project.Vertex;

public class App {
	public static void main(String[] args) throws SQLException {
		// DB �꽌踰� �뿰寃�
		DatabaseMgr dbMgr = DatabaseMgr.getInstance();
		dbMgr.initialize("3306", "0000"); // 媛곸옄 �궗�슜�븯�뒗 �룷�듃, 鍮꾨쾲 �옉�꽦

		// �삁瑜� �뱾�뼱 踰꾪뀓�뒪�뿉�꽌 荑쇰━臾� �궗�슜
		JVertex v = new JVertex();
		// dbMgr�뿉�꽌 �씠誘� db�꽌踰꾩� �뿰�룞�맂 Statement �궗�슜�븿
		v.setStatement(dbMgr.getStatement());
		
		Graph g = new JGraph();
        Vertex v1 = g.addVertex("1");
        Vertex v2 = g.addVertex("2");
        Vertex v3 = g.addVertex("3");
        
        Edge e1 = g.addEdge(v2, v1, "label");
        Edge e2 = g.addEdge(v2, v3, "label");

        Iterator<Edge> ei1 = v2.getEdges(Direction.OUT).iterator();    //寃곌낵�뒗 v1, v3瑜� �룷�븿�븳 iterator
        while(ei1.hasNext()){
            Edge e =ei1.next();
            System.out.println(e);
        }
        
//        Iterator<Edge> ei2 = v2.getEdges(Direction.IN, "label").iterator();     //寃곌낵�뒗 null
//        while(ei2.hasNext()){
//            Edge e = ei2.next();
//            System.out.println(e);
//        }

        // 11.23 �솗�씤 寃곌낵 �꽣吏� 
        // System.out.println(e1.getVertex(Direction.OUT));    //寃곌낵�뒗   v2
        // System.out.println(e1.getVertex(Direction.IN));    //寃곌낵�뒗   v1

        v1.setProperty("x", 300);
        v1.setProperty("y", 300);
        v1.setProperty("y", 200);
		e1.setProperty("x", 400);
		e1.setProperty("y", 500);
		e2.setProperty("x", 400);
		e2.setProperty("y", 500);
		// DB �꽌踰� 醫낅즺, 荑쇰━臾� �옉�뾽 紐⑤몢 留덉튇 �썑, 留덉�留됱뿉 �샇異쒗븷 寃�
		dbMgr.release();
	}
}
