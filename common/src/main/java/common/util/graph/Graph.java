package common.util.graph;

import java.util.ArrayList;
import java.util.List;

public class Graph {
    private int nodeSize;
    private List<Edge> edges = new ArrayList<>();
    private int[] nodeWeights;
    private int[][] adjMatrix;
    public Graph(int nodeSize) {
        this.nodeSize = nodeSize;
        this.nodeWeights = new int[nodeSize];
    }

    public List<Edge> getEdges() {
        return edges;
    }

    public void addNode(int id, int weight) {
        nodeWeights[id] = weight;
    }

    public void addEdge(Edge edge) {
        edges.add(edge);
    }
    public void addEdge(int from, int to, int weight) {
        edges.add(new Edge(from, to, weight));
    }

    public int[] getNodeWeights() {
        return nodeWeights;
    }

    public int getNodeSize() {
        return nodeSize;
    }
}
