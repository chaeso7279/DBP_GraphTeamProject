package kr.ac.sejong.da.project;

import java.sql.SQLException;

public interface Edge extends Element {
    /**
     * Return the tail/out or head/in vertex.
     *
     * @param direction whether to return the tail/out or head/in vertex
     * @return the tail/out or head/in vertex
     * @throws IllegalArgumentException is thrown if a direction of both is provided
     * @throws SQLException 
     */
    public Vertex getVertex(Direction direction) throws IllegalArgumentException, SQLException;

    /**
     * Return the label associated with the edge.
     *
     * @return the edge label
     * @throws SQLException 
     */
    public String getLabel() throws SQLException;
}
