package kr.ac.sejong.da.project.impl;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import kr.ac.sejong.da.project.DatabaseMgr;
import kr.ac.sejong.da.project.Direction;
import kr.ac.sejong.da.project.Edge;
import kr.ac.sejong.da.project.Vertex;

public class JVertex implements Vertex {

	// 예: string 형태의 고유 아이디, '|' 사용 금지
	private String m_id; // DB에서는 INT형

	// 쿼리문 사용하기 위해 가져옴
	private Statement m_stmt = null;

	JVertex() {
		super();
		if (m_stmt == null) // Statement Null 이면 DBMgr에서 가져옴
			m_stmt = DatabaseMgr.getInstance().getStatement();
	}

	// setter
	public void setStatement(Statement stmt) {
		m_stmt = stmt;
	}

	public void setID(String id) {
		m_id = id;
	} // 채수화

	// getter
	@Override
	public Object getId() {
		return m_id;
	} // 채수화

	@Override // 현재 버텍스가 인자 방향(Out,In)에 해당하는 Edges 모두 가져옴
	public Iterable<Edge> getEdges(Direction direction, String... labels) { // 채수화
		// SQL 구문 실행
		String sql = "";
		
		if(direction == Direction.IN) 
			sql = "SELECT * FROM Edges WHERE InV = " + m_id; 
		else 
			sql = "SELECT * FROM Edges WHERE OutV = " + m_id;
		
		// labels 개수: 0 ~ 여러개
		if(labels.length <= 0) // labels 인자가 들어오지 않음
			sql += ";";
		else { // labels 인자 한 개 이상
			sql += " AND Label = \"" + labels[0] + "\"";
			for(int i = 1; i < labels.length; ++i)
				sql += " OR Label = \"" + labels[i] + "\"";
			sql += ";";
		}
		
		ResultSet rs = null;
		try {
			rs = m_stmt.executeQuery(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		// prepared statement 버전 (향상 효과 없었음)
		/*
			 * if (direction == Direction.IN) sql =
			 * "SELECT * FROM Edges WHERE InV = (?) AND Label = (?);"; else sql =
			 * "SELECT * FROM Edges WHERE OutV = (?) AND Label = (?);";
			 * 
			 * PreparedStatement pStmt =
			 * DatabaseMgr.getInstance().getConnection().prepareStatement(sql);
			 * pStmt.setInt(1, Integer.parseInt(m_id)); pStmt.setString(2, labels[0]);
			 * 
			 * ResultSet rs = pStmt.executeQuery();
			 */

		
		
		// 결과 담을 리스트 생성
		List<Edge> result = new ArrayList<Edge>();

		try {
			while (rs.next()) {
				// 가져온 결과가 null 일 경우 처리 해줘야함(인덱싱?)
				String outV = rs.getString(1);
				String inV = rs.getString(2);
				String label = rs.getString(3);
				String prop = rs.getString(4);

				Edge eTemp = new JEdge();
				((JEdge) eTemp).setID(outV, inV, label);

				if (prop != null) { // property가 null 아닐 때
					JSONObject jObj = new JSONObject(prop); // 라이브러리 사용
					Iterator<String> iter = jObj.keys();

					while (iter.hasNext()) {
						String key = (String) iter.next();
						eTemp.setProperty(key, jObj.get(key));
					}
				}
				result.add(eTemp);
			}
		} catch (JSONException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (result.isEmpty()) // 결과가 없을 경우, null 반환
			return null;

		return result;
	}

	@Override // 현재 버텍스와 해당 방향(Out,In) 으로 연결된 Vertex 모두 가져옴
	public Iterable<Vertex> getVertices(Direction direction, String... labels) { // 채수화
		String sql = "";
		
		if(direction == Direction.IN) 
			sql =	"SELECT OutV, Properties FROM Edges WHERE InV = " + m_id;
		else if(direction == Direction.OUT) 
			sql = "SELECT InV, Properties FROM Edges WHERE OutV = " + m_id;

		// labels 개수: 0 ~ 여러개
		if(labels.length <= 0) // labels 인자가 들어오지 않음 
			sql += ";"; 
		else { // labels 인자 한 개 이상
			sql += " AND Label = \"" + labels[0] + "\""; 
			for(int i = 1; i < labels.length; ++i) 
				sql += " OR Label = \"" + labels[i] + "\""; 
			sql += ";"; 
		}
	 

		ResultSet rs = null;
		try {
			rs = m_stmt.executeQuery(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// prepared statement 버전 (향상 효과 없었음)
//		if (direction == Direction.IN)
//			sql = "SELECT OutV, Properties FROM Edges WHERE InV = (?) AND Label = (?);";
//		else
//			sql = "SELECT InV, Properties FROM Edges WHERE OutV = (?) AND Label = (?);";
//
//		PreparedStatement pStmt = DatabaseMgr.getInstance().getConnection().prepareStatement(sql);
//		pStmt.setInt(1, Integer.parseInt(m_id));
//		pStmt.setString(2, labels[0]);
//
//		ResultSet rs = pStmt.executeQuery();
		
		// 결과 담을 리스트 생성
		List<Vertex> result = new ArrayList<Vertex>();

		try {
			while (rs.next()) {
				// 가져온 결과가 null 일 경우 처리 해줘야함(인덱싱?)
				String id = rs.getString(1);
				String prop = rs.getString(2);

				Vertex vTemp = new JVertex();
				((JVertex) vTemp).setID(id);

				if (prop != null) { // property가 null 아닐 때
					JSONObject jObj = new JSONObject(prop); // 라이브러리 사용
					Iterator<String> iter = jObj.keys();

					while (iter.hasNext()) {
						String key = (String) iter.next();
						vTemp.setProperty(key, jObj.get(key));
					}
				}
				result.add(vTemp);
			}
		} catch (JSONException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (result.isEmpty()) // 결과가 없을 경우, null 반환
			return null;

		return result;
	}

	@Override // 현재 버텍스가 OutV, 인자가 InV에 해당하는 Edge 추가
	public Edge addEdge(String label, Vertex inVertex) { // 채수화
		int outID = Integer.parseInt(m_id);
		int inID = Integer.parseInt((String) inVertex.getId());
		System.out.println("dd : " + inID);

		// DB 삽입
		String sql = "INSERT INTO Edges SET OutV = " + outID + ", InV = " + inID + ", Label = \"" + label + "\";";
		try {
			if (0 == m_stmt.executeUpdate(sql)) // 삽입 오류 발생 시(중복 등),
				return null;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} // null 반환

		// Edge 객체 생성
		Edge eTemp = new JEdge();
		((JEdge) eTemp).setID(m_id, (String) inVertex.getId(), label);

		return eTemp;
	}

	@Override
	public Object getProperty(String key) { // 채수화

		String sql = "SELECT JSON_VALUE(Properties, '$." + key + "') FROM Vertices WHERE ID = " + m_id
				+ " AND NOT Properties IS NULL;";
		ResultSet rs = null;
		try {
			rs = m_stmt.executeQuery(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			if (!rs.next()) // 기존 Property 없음(처음 작성) => null 반환
				return null;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}

		try {
			return rs.getString(1);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
	}

	@Override
	public Set<String> getPropertyKeys() { // 채수화

		String sql = "SELECT JSON_KEYS(Properties) FROM Vertices WHERE ID = " + m_id + " AND NOT Properties IS NULL;";
		ResultSet rs = null;
		try {
			rs = m_stmt.executeQuery(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		try {
			if (!rs.next()) // 기존 Property 없음(처음 작성) => keyX, null 반환
				return null;
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}

		JSONArray arrKeys = null;
		try {
			arrKeys = new JSONArray(rs.getString(1));
		} catch (JSONException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return null;
		}
		
		HashSet<String> setKeys = new HashSet<String>();

		for (int i = 0; i < arrKeys.length(); ++i)
			setKeys.add(arrKeys.getString(i));

		return setKeys;
	}

	@Override // 기존 property 유지하면서 추가, key 중복 시 value 업데이트
	public void setProperty(String key, Object value) { // 채수화

		// 1. 기존 property 가져오기
		String sql = "SELECT Properties FROM Vertices WHERE ID = " + m_id + " AND NOT Properties IS NULL;"; // NULL 제외
		ResultSet rs = null;
		try {
			rs = m_stmt.executeQuery(sql);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// 2. 라이브러리 이용 => jsonObject로 파싱하기
		JSONObject jObj = null;
		try {
			if (!rs.next()) // 기존 Property 없음(처음 작성)
				jObj = new JSONObject();
			else // 기존 Property를 JSON Object로 만듦
				jObj = new JSONObject(rs.getString(1));
		} catch (JSONException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// 3. jsonObject의 put 함수 이용해 key, value 추가 또는 value 값 수정
		Iterator<String> iter = jObj.keys(); // key 중복 검사
		while (iter.hasNext()) {
			if (iter.next() == key) { // 기존 property에 추가하려는 key가 이미 존재할 때,
				jObj.remove(key); // 삭제하고, 다시 추가해줌
				break; // 단, 이 경우 property 내 순서 변경발생 -> 삭제하고 뒤에 추가되면서
			}
		}

		jObj.put(key, value); // 인자로 들어온 key, value 추가

		// 4. 라이브러리 이용 => String 으로 변환하기
		String strJson = jObj.toString();

		// 5. SQL 구문으로 DB에 넣기
		try {
			m_stmt.executeUpdate("UPDATE Vertices SET Properties = '" + strJson + "'" + "WHERE ID = " + m_id + ";");
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public String toString() {
		String line = "v[" + m_id + "]";
		return line;
	}
}
