package common.util.graph;

import java.util.List;
import java.util.Vector;

public class Graph {
    private int nodeSize;
    private Vector<Edge> edges = new Vector<>();
    private int[] nodeWeights;
    private GraphPartitioner graphPartitioner;
    public Graph(int nodeSize, int partitionCount) {
        this.nodeSize = nodeSize;
        this.nodeWeights = new int[nodeSize];
        this.graphPartitioner = new GraphPartitioner(nodeSize, nodeWeights, edges, partitionCount);
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
    public void clean() {
        edges.clear();
    }
    public void partition() {
        graphPartitioner.run();
    }
    public List<List<Integer>> getPartitions() {
        return graphPartitioner.getPartitions();
    }
}
