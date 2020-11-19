package kr.ac.sejong.da.project;

import java.sql.SQLException;

public interface Vertex extends Element {
	/**
	 * Return the edges incident to the vertex according to the provided direction
	 * and edge labels.
	 *
	 * @param direction the direction of the edges to retrieve
	 * @param labels    the labels of the edges to retrieve
	 * @return an iterable of incident edges
	 * @throws SQLException 
	 */
	public Iterable<Edge> getEdges(Direction direction, String... labels) throws SQLException;

	/**
	 * Return the vertices adjacent to the vertex according to the provided
	 * direction and edge labels. This method does not remove duplicate vertices
	 * (i.e. those vertices that are connected by more than one edge).
	 *
	 * @param direction the direction of the edges of the adjacent vertices
	 * @param labels    the labels of the edges of the adjacent vertices
	 * @return an iterable of adjacent vertices
	 */
	public Iterable<Vertex> getVertices(Direction direction, String... labels) throws SQLException;;

	/**
	 * Add a new outgoing edge from this vertex to the parameter vertex with
	 * provided edge label.
	 *
	 * @param label    the label of the edge
	 * @param inVertex the vertex to connect to with an incoming edge
	 * @return the newly created edge
	 * @throws SQLException 
	 */
	public Edge addEdge(String label, Vertex inVertex) throws SQLException;
}
