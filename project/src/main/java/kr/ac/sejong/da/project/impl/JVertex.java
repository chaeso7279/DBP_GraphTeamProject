package kr.ac.sejong.da.project.impl;

import java.util.Set;

import kr.ac.sejong.da.project.Direction;
import kr.ac.sejong.da.project.Edge;
import kr.ac.sejong.da.project.Vertex;

public class JVertex implements Vertex {

    //예: string 형태의 고유 아이디, '|' 사용 금지
    private String id;

    @Override
    public Iterable<Edge> getEdges(Direction direction, String... labels) {
        return null;
    }

    @Override
    public Iterable<Vertex> getVertices(Direction direction, String... labels) {
        return null;
    }

    @Override
    public Edge addEdge(String label, Vertex inVertex) {
        return null;
    }

    @Override
    public Object getProperty(String key) {
        return null;
    }

    @Override
    public Set<String> getPropertyKeys() {
        return null;
    }

    @Override
    public void setProperty(String key, Object value) {

    }

    @Override
    public Object getId() {
        return null;
    }
}
