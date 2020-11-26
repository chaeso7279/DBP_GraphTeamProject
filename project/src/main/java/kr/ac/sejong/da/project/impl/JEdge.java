package kr.ac.sejong.da.project.impl;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.json.JSONObject;

import kr.ac.sejong.da.project.DatabaseMgr;
import kr.ac.sejong.da.project.Direction;
import kr.ac.sejong.da.project.Edge;
import kr.ac.sejong.da.project.Vertex;

public class JEdge implements Edge {

	public int out, in; // outID|label|inID �삎�깭�쓽 怨좎쑀 �븘�씠�뵒
	public String lab;

	// 荑쇰━臾� �궗�슜�븯湲� �쐞�빐 媛��졇�샂
	public Connection m_connection = null;
	private Statement m_stmt = null;

	JEdge() {
		if (m_connection == null)
			m_connection = DatabaseMgr.getInstance().getConnection();
		if (m_stmt == null)
			m_stmt = DatabaseMgr.getInstance().getStatement();
	}

	// setter
	public void setStatement(Statement stmt) {
		m_stmt = stmt;
	}

	public void setID(String outV, String inV, String label) { // 梨꾩닔�솕
		out = Integer.parseInt(outV);
		in = Integer.parseInt(inV);
		lab = label;

	}

	@Override // Vertices�뀒�씠釉붿뿉�꽌 �븯�굹�쓽 vertex瑜� 媛��졇�삤�뒗嫄� 媛숈��뜲 direction�쓣 �뼱�뼸寃� 荑쇰━臾몄쓣
				// 媛��졇�삱吏� 紐곕씪�꽌 �씪�떒�� select *
	// from vertices; 瑜쇱�
	public Vertex getVertex(Direction direction) throws SQLException { // Vertex媛��졇���꽌 諛섑솚�븯�뒗 硫붿냼�뱶
		Vertex vertex = null; // �뾾�쑝硫� NULL

		String sql = "SELECT * FROM Vertices WHERE ID = ";
		if (direction == Direction.IN)
			sql += in;
		else if (direction == Direction.OUT)
			sql += out;

		ResultSet rs = m_stmt.executeQuery(sql);

		String ID = rs.getString("ID");
		// Object Properties = rs.getObject("Properties");

		if (ID == null)
			return null;

		vertex = new JVertex();
		((JVertex) vertex).setID(ID);
		// vertex.setProperty("Properties", Properties);

		return vertex;
	}

	@Override
	public String getLabel() throws SQLException {
		return lab;
	}

	@Override // property媛� �뀒�씠釉붿뿉 �떎 議댁옱 �븯�뒗�뜲 �씪�떒�� 萸붿� 紐곕씪�꽌 edges�뿉留� �꽔�뼱�몺
	public Object getProperty(String key) throws SQLException { // select key from graph where key=?
		PreparedStatement pstmt = m_connection.prepareStatement("SELECT ? FROM Edges;");
		pstmt.setString(1, key);

		ResultSet rs = pstmt.executeQuery();
		Object result = rs.getObject(key);

		if (result == null)
			return null;

		return result;
	}

	@Override
	public Set<String> getPropertyKeys() throws SQLException { // �씠�빐紐삵뻽�뒗�뜲 而щ읆紐낆쓣 媛��졇�삤�뒗 嫄� 媛숈쓬... �솗�떎移� �븡�븘�슂
		ResultSet rs = m_stmt
				.executeQuery("SELECT column_name FROM information_schema.columns WHERE table_schema = 'DB�씠由꾨꽔湲�';");
		Set<String> result = new HashSet<>();
		while (rs.next()) {
			result.add(rs.getString(1));
		}

		return result;
	}

	@Override // property媛� �뀒�씠釉붿뿉 �떎 議댁옱 �븯�뒗�뜲 �씪�떒�� 萸붿� 紐곕씪�꽌 edges�뿉留� �꽔�뼱�몺
	public void setProperty(String key, Object value) throws SQLException { // select key from edges

		// 1. 湲곗〈 property 媛��졇�삤湲�
    	String sql = "SELECT Properties FROM edges WHERE OutV = " + out +" AND InV = " + in + " AND NOT Properties IS NULL;"; // NULL �젣�쇅
    	ResultSet rs = m_stmt.executeQuery(sql);
    	
    	// 2. �씪�씠釉뚮윭由� �씠�슜 => jsonObject濡� �뙆�떛�븯湲� 
    	JSONObject jObj = null;
    	if(!rs.next())		 			// 湲곗〈 Property �뾾�쓬(泥섏쓬 �옉�꽦)
    		jObj = new JSONObject();
    	else							// 湲곗〈 Property瑜� JSON Object濡� 留뚮벀
    		jObj = new JSONObject(rs.getString(1));
    	
    	// 3. jsonObject�쓽 put �븿�닔 �씠�슜�빐 key, value 異붽� �삉�뒗 value 媛� �닔�젙
    	Iterator<String> iter = jObj.keys(); // key 以묐났 寃��궗
		while(iter.hasNext()) {
			if(iter.next() == key) {	// 湲곗〈 property�뿉 異붽��븯�젮�뒗 key媛� �씠誘� 議댁옱�븷 �븣,
				jObj.remove(key);		// �궘�젣�븯怨�, �떎�떆 異붽��빐以� 
				break;					// �떒, �씠 寃쎌슦 property �궡 �닚�꽌 蹂�寃쎈컻�깮 -> �궘�젣�븯怨� �뮘�뿉 異붽��릺硫댁꽌	
			}							
		}

		jObj.put(key, value);
		String strJson = jObj.toString();

		m_stmt.executeUpdate("INSERT INTO Edges VALUES (" + out + "," + in + "," + lab + ",'" + strJson
				+ "') ON DUPLICATE KEY UPDATE Properties = '" + strJson + "';"); // update�븯怨�

	}

	@Override // this.id濡� 諛섑솚
	public Object getId() {
		return out+" "+in+" "+lab;
	}
}
