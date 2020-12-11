package kr.ac.sejong.da.project.app;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.SQLException;
import java.util.HashSet;
import java.util.Iterator;

import kr.ac.sejong.da.project.Graph;
import kr.ac.sejong.da.project.Vertex;
import kr.ac.sejong.da.project.impl.Team3JGraph;

public class TraversalTest {

	public static void createGraph(Graph g) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader("d:\\data.txt"));
		int cnt = 0;
		while (true) {
			String line = br.readLine();
			if (line == null)
				break;
			if (line.startsWith("#"))
				continue;
			
			String[] arr = line.split("\t");

			Vertex vl = g.getVertex(arr[0]);
			if (vl == null) {
				vl = g.addVertex(arr[0]);
			}
			Vertex vr = g.getVertex(arr[1]);
			if (vr == null) {
				vr = g.addVertex(arr[1]);
			}

			g.addEdge(vl, vr, "label");
			System.out.println(cnt);
		}
		br.close();
	}

	/**
	 * 아래의 코드는 아마 동작할 것임 하지만 수정가능 (물론 같은 결과를 출력해야 함)
	 * 각 팀은 getReachableVertices를 효율적으로 구현해야 하며 [아래의 테이블 부분 수정                        가능], 
 	 * 필요에 따라 여러분 그래프에 새로운 메소드를 추가하여 호출 할 수 있음
	 * 그 메소드는 데이터를 로드해 두고 메모리 상에서 연산하는 것을 불허함.
            * 과제5를 하면서 동작방법에 대한 컨펌을 받으시길 바람. 불이익을 받을 수 있음
	 * 
	 * 여러분의 결과에 따라, 퍼포먼스 경연때는 두 번내가 아닌 세 번내, 네 번내로 조정가능
             * 혹은 source vertices 를 특정할 수 있음 
	 * @throws SQLException 
	 */
	
	public static void getReachableVertices(Graph g) throws IOException, SQLException {
		BufferedWriter w = new BufferedWriter(new FileWriter("d:\\rv.txt"));
		Iterator<Vertex> iter = g.getVertices().iterator();
		while(iter.hasNext()) {
			// 각 vertex에 대해
			Vertex v = iter.next();
			HashSet<String> rv = new HashSet<String>();
			// 수정 가능한 부분 ~ 
			((Team3JGraph)g).getReachableVertices(v, rv);
			// ~ 수정 가능한 부분
			
			System.out.println(v.toString() + " : " + rv.size());
			w.write(v.toString() + " : " + rv.size() + "\n");

		}
		w.close();
	} 

	public static void main(String[] args) throws IOException, SQLException {
		Graph g = new Team3JGraph();

		long p = System.currentTimeMillis();
		createGraph(g);
		System.out.println("Graph Creation (ms.): " + (System.currentTimeMillis() - p));

		p = System.currentTimeMillis();
		getReachableVertices(g);
		System.out.println("RV (ms.): " + (System.currentTimeMillis() - p));
		//RV (ms.): 93792
	}
}

