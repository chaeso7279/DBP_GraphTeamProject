package kr.ac.sejong.da.project.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.json.JSONObject;

import kr.ac.sejong.da.project.DatabaseMgr;
import kr.ac.sejong.da.project.Direction;
import kr.ac.sejong.da.project.Edge;
import kr.ac.sejong.da.project.Vertex;

public class JVertex implements Vertex {

    //�삁: string �삎�깭�쓽 怨좎쑀 �븘�씠�뵒, '|' �궗�슜 湲덉�
    private String m_id; // DB�뿉�꽌�뒗 INT�삎
    
    // 荑쇰━臾� �궗�슜�븯湲� �쐞�빐 媛��졇�샂
	private Statement m_stmt = null;
    
	JVertex() {
		super();
		if(m_stmt == null)	// Statement Null �씠硫� DBMgr�뿉�꽌 媛��졇�샂
			m_stmt = DatabaseMgr.getInstance().getStatement();
	}
	
	// setter
    public void setStatement(Statement stmt) { m_stmt = stmt; }
    public void setID(String id) { m_id = id; }	// 梨꾩닔�솕
    
    // getter
    @Override
    public Object getId() { return m_id; }	// 梨꾩닔�솕
    
    @Override	// �쁽�옱 踰꾪뀓�뒪媛� �씤�옄 諛⑺뼢(Out,In)�뿉 �빐�떦�븯�뒗 Edges 紐⑤몢 媛��졇�샂
    public Iterable<Edge> getEdges(Direction direction, String... labels) throws SQLException {	// 梨꾩닔�솕
    	// SQL 援щЦ �떎�뻾
    	String sql = "";
    	if(direction == Direction.IN)
    		sql = "SELECT * FROM Edges WHERE InV = " + m_id;
    	else if(direction == Direction.OUT)
    		sql = "SELECT * FROM Edges WHERE OutV = " + m_id;
    	/*
		 * else // BOTH // BOTH �씪 寃쎌슦 �깮媛곹빐蹂닿린..
		 */  
    	
    	// labels 媛쒖닔: 0 ~ �뿬�윭媛� 
    	if(labels.length <= 0) 		// labels �씤�옄媛� �뱾�뼱�삤吏� �븡�쓬
    		sql += ";";
    	else {						// labels �씤�옄 �븳 媛� �씠�긽
    		sql += " AND Label = \"" + labels[0] + "\"";
    		for(int i = 1; i < labels.length; ++i) 
    			sql += " OR Label = \"" + labels[i] + "\"";
    		sql += ";";
    	}
  
    	ResultSet rs = m_stmt.executeQuery(sql);
    	
    	// 寃곌낵 �떞�쓣 由ъ뒪�듃 �깮�꽦
    	List<Edge> result = new ArrayList<Edge>();
    	
    	while(rs.next()) {
    		// 媛��졇�삩 寃곌낵媛� null �씪 寃쎌슦 泥섎━ �빐以섏빞�븿(�씤�뜳�떛?)
    		String outV = rs.getString(1);
    		String inV = rs.getString(2);
    		String label = rs.getString(3);
    		String prop = rs.getString(4);
    		
    		Edge eTemp = new JEdge();
    		((JEdge) eTemp).setID(outV, inV, label);
    		
    		if(prop != null) {	// property媛� null �븘�땺 �븣
    			JSONObject jObj = new JSONObject(prop);	// �씪�씠釉뚮윭由� �궗�슜
    			Iterator<String> iter = jObj.keys();
    			
    			while(iter.hasNext()) {
    				String key = (String)iter.next();
    				eTemp.setProperty(key, jObj.get(key));
    			}	
    		}
    		result.add(eTemp);
    	}
    	
    	if(result.isEmpty()) // 寃곌낵媛� �뾾�쓣 寃쎌슦, null 諛섑솚
    		return null;
    	
        return result;
    }

    @Override	// �쁽�옱 踰꾪뀓�뒪�� �빐�떦 諛⑺뼢(Out,In) �쑝濡� �뿰寃곕맂 Vertex 紐⑤몢 媛��졇�샂
    public Iterable<Vertex> getVertices(Direction direction, String... labels) throws SQLException {	// 梨꾩닔�솕
    	String sql = "";
    	if(direction == Direction.IN)
    		sql = "SELECT OutV FROM Edges WHERE InV = " + m_id;
    	else if(direction == Direction.OUT)
    		sql = "SELECT InV FROM Edges WHERE OutV = " + m_id;
    	/*
		 * else // BOTH // BOTH �씪 寃쎌슦 �깮媛곹빐蹂닿린..
		 */  
    	
    	// labels 媛쒖닔: 0 ~ �뿬�윭媛� 
    	if(labels.length <= 0) 		// labels �씤�옄媛� �뱾�뼱�삤吏� �븡�쓬
    		sql += ";";
    	else {						// labels �씤�옄 �븳 媛� �씠�긽
    		sql += " AND Label = \"" + labels[0] + "\"";
    		for(int i = 1; i < labels.length; ++i) 
    			sql += " OR Label = \"" + labels[i] + "\"";
    		sql += ";";
    	}
 
    	ResultSet rs = m_stmt.executeQuery(sql);
    	
    	// 寃곌낵 �떞�쓣 由ъ뒪�듃 �깮�꽦
    	List<Vertex> result = new ArrayList<Vertex>();
    	
    	while(rs.next()) {
    		// 媛��졇�삩 寃곌낵媛� null �씪 寃쎌슦 泥섎━ �빐以섏빞�븿(�씤�뜳�떛?)
    		String id = rs.getString(1);
    		String prop = rs.getString(2);
    		
    		Vertex vTemp = new JVertex();
    		((JVertex) vTemp).setID(id);
    		
    		if(prop != null) {	// property媛� null �븘�땺 �븣
    			JSONObject jObj = new JSONObject(prop);	// �씪�씠釉뚮윭由� �궗�슜
    			Iterator<String> iter = jObj.keys();
    			
    			while(iter.hasNext()) {
    				String key = (String)iter.next();
    				vTemp.setProperty(key, jObj.get(key));
    			}	
    		}
    		result.add(vTemp);
    	}
    	
    	if(result.isEmpty()) // 寃곌낵媛� �뾾�쓣 寃쎌슦, null 諛섑솚
    		return null;
    	
    	return result;
    }

    @Override	// �쁽�옱 踰꾪뀓�뒪媛� OutV, �씤�옄媛� InV�뿉 �빐�떦�븯�뒗 Edge 異붽�
    public Edge addEdge(String label, Vertex inVertex) throws SQLException {	// 梨꾩닔�솕
    	int outID = Integer.parseInt(m_id);
    	int inID = Integer.parseInt((String) inVertex.getId());
    	System.out.println("dd : "+inID);
    	
    	// DB �궫�엯
    	String sql = "INSERT INTO Edges SET OutV = " + outID + ", InV = " + inID + ", Label = \"" + label + "\";"; 
    	if(0 == m_stmt.executeUpdate(sql)) // �궫�엯 �삤瑜� 諛쒖깮 �떆(以묐났 �벑),
    		return null; 					// null 諛섑솚
    	
    	// Edge 媛앹껜 �깮�꽦
    	Edge eTemp = new JEdge();
    	((JEdge) eTemp).setID(m_id, (String) inVertex.getId(), label);
    	
        return eTemp;
    }

    @Override
    public Object getProperty(String key) {
        return null;
    }

    @Override
    public Set<String> getPropertyKeys() {
        return null;
    }

    @Override	// 湲곗〈 property �쑀吏��븯硫댁꽌 異붽�, key 以묐났 �떆 value �뾽�뜲�씠�듃
    public void setProperty(String key, Object value) throws SQLException {	// 梨꾩닔�솕

    	// 1. 湲곗〈 property 媛��졇�삤湲�
    	String sql = "SELECT Properties FROM Vertices WHERE ID = " + m_id + " AND NOT Properties IS NULL;"; // NULL �젣�쇅
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
		
		jObj.put(key, value);	// �씤�옄濡� �뱾�뼱�삩 key, value 異붽�
    	
		// 4. �씪�씠釉뚮윭由� �씠�슜 => String �쑝濡� 蹂��솚�븯湲�
    	String strJson = jObj.toString(); 
    	
    	// 5. SQL 援щЦ�쑝濡� DB�뿉 �꽔湲�
    	m_stmt.executeUpdate("UPDATE Vertices SET Properties = '"+ strJson + "'"
    			+ "WHERE ID = " + m_id + ";");
    }
}
