package kr.ac.sejong.da.project.app;

import java.sql.SQLException;

import kr.ac.sejong.da.project.Team3DBMgr;
import kr.ac.sejong.da.project.Direction;
import kr.ac.sejong.da.project.Edge;
import kr.ac.sejong.da.project.Graph;
import kr.ac.sejong.da.project.Vertex;
import kr.ac.sejong.da.project.impl.Team3JGraph;

public class UnitTest {

	public static void doTest(Graph g) throws SQLException {

		
		
		// 그래프 생성
		for (int i = 0; i < 13; i++) {
			Vertex v = g.addVertex(String.valueOf(i));
			v.setProperty("key1", String.valueOf(i));
		}

		// 제공할때는 null 빼고 제공
		Edge e01 = g.addEdge(g.getVertex("0"), g.getVertex("1"), "label");
		e01.setProperty("key1", 0 + 1);
		
		Edge e02 = g.addEdge(g.getVertex("0"), g.getVertex("2"), "label");
		e02.setProperty("key1", 0 + 2);
		
		Edge e13 = g.addEdge(g.getVertex("1"), g.getVertex("3"), "label");
		e13.setProperty("key1", 1 + 3);
		
		Edge e14 = g.addEdge(g.getVertex("1"), g.getVertex("4"), "label");
		e14.setProperty("key1", 1 + 4);
		
		Edge e25 = g.addEdge(g.getVertex("2"), g.getVertex("5"), "label");
		e25.setProperty("key1", 2 + 5);
		
		Edge e26 = g.addEdge(g.getVertex("2"), g.getVertex("6"), "label");
		e26.setProperty("key1", 2 + 6);
		
		Edge e70 = g.addEdge(g.getVertex("7"), g.getVertex("0"), "label");
		e70.setProperty("key1", 7 + 0);
		
		Edge e80 = g.addEdge(g.getVertex("8"), g.getVertex("0"), "label");
		e80.setProperty("key1", 8 + 0);
		
		Edge e97 = g.addEdge(g.getVertex("9"), g.getVertex("7"), "label");
		e97.setProperty("key1", 9 + 7);
		
		Edge e107 = g.addEdge(g.getVertex("10"), g.getVertex("7"), "label");
		e107.setProperty("key1", 10 + 7);
		
		Edge e118 = g.addEdge(g.getVertex("11"), g.getVertex("8"), "label");
		e118.setProperty("key1", 11 + 8);
		
		Edge e128 = g.addEdge(g.getVertex("12"), g.getVertex("8"), "label");
		e128.setProperty("key1", 12 + 8);
		
		System.out.println(g.getVertices());
		System.out.println(g.getVertices("key1", "3").iterator().next());
		System.out.println(g.getVertices("key1", "5").iterator().next());
		
		System.out.println(g.getEdges());
		System.out.println(g.getEdges("key1", 8).iterator().next());
		
		System.out.println(g.getVertex("0").getEdges(Direction.OUT, "label"));
		System.out.println(g.getVertex("1").getEdges(Direction.OUT, "label"));
		System.out.println(g.getVertex("0").getEdges(Direction.IN, "label"));
		System.out.println(g.getVertex("7").getEdges(Direction.IN, "label"));
		
		System.out.println(e128.getVertex(Direction.OUT));
		System.out.println(e128.getVertex(Direction.IN));
	}

	public static void main(String[] args) {
	
		// 이름 교체하세요
		Graph g = new Team3JGraph();
		try {
			doTest(g);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}

