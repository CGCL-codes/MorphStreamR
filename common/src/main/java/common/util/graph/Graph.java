package common.util.graph;

import java.util.ArrayList;
import java.util.List;

public class Graph {
    private List<Node> nodes = new ArrayList<>();
    private List<Integer> nodeId = new ArrayList<>();
    private List<Edge> edges = new ArrayList<>();
    private int[] nodeWeights;
    private int[][] adjMatrix;
    public Graph(int nodeSize) {
        adjMatrix = new int[nodeSize][nodeSize];
        nodeWeights = new int[nodeSize];
    }

    public List<Node> getNodes() {
        return nodes;
    }

    public List<Edge> getEdges() {
        return edges;
    }

    public void addNode(Node node) {
        nodes.add(node);
        nodeWeights[node.id] = node.weight;
        nodeId.add(node.id);
    }

    public void removeNode(Node node) {
        nodes.remove(node);
        for (Edge edge : edges) {
            if (edge.getFrom() == node.id || edge.getTo() == node.id) {
                edges.remove(edge);
            }
        }
    }

    public void addEdge(Edge edge) {
        edges.add(edge);
    }
    public void addEdge(int from, int to, int weight) {
        edges.add(new Edge(from, to, weight));
        adjMatrix[from][to] = weight;
        adjMatrix[to][from] = weight;
    }

    public int[][] getAdjMatrix() {
        return adjMatrix;
    }

    public int[] getNodeWeights() {
        return nodeWeights;
    }

    public List<Integer> getNodeId() {
        return nodeId;
    }

    public static void graphPartitioner(int[][] graph, int[] nodeWeights, int k) {
        List<Integer>[] partitions = new ArrayList[k];
        int[] partitionWeights = new int[k];
        int[][] partitionEdges = new int[k][k];
        int averageWeight = 0;
        int n = graph.length;
        for (int i = 0; i < k; i++) {
            partitions[i] = new ArrayList<>();
        }
        for (int i = 0; i < n; i++) {
            partitions[i % k].add(i);
            partitionWeights[i % k] += nodeWeights[i];
            averageWeight += nodeWeights[i];
        }
        averageWeight /= k;
        for (int i = 0; i < k; i++) {
            for (int j = 0; j < k; j++) {
                if (i != j) {
                    for (int x : partitions[i]) {
                        for (int y : partitions[j]) {
                            partitionEdges[i][j] += graph[x][y];
                        }
                    }
                }
            }
        }
        //Partition G into a set of sub-graphs {G1,G2,. . . ,Gn} with about the same weight according to the key range
        while (true) {
            boolean balanced = true;
            for (int i = 0; i < k; i++) {
                if (partitionWeights[i] > averageWeight) {
                    balanced = false;
                    int j = (i + 1) % k;
                    while (partitionWeights[j] <= averageWeight) {
                        j = (j + 1) % k;
                    }
                    int node = partitions[i].remove(partitions[i].size() - 1);
                    partitions[j].add(node);
                    partitionWeights[i] -= nodeWeights[node];
                    partitionWeights[j] += nodeWeights[node];
                    break;
                }
            }
            if (balanced) {
                break;
            }
        }
        //Move the nodes between the sub-graphs to minimize the total edge weight
        boolean changed = true;
        while (changed) {
            changed = false;
            for (int i = 0; i < k; i++) {
                for (int j = 0; j < partitions[i].size(); j++) {
                    int node = partitions[i].get(j);
                    int minWeight = Integer.MAX_VALUE;
                    int minPartition = -1;
                    for (int p = 0; p < k; p++) {
                        if (p != i) {
                            int weight = partitionEdges[i][p] - partitionEdges[p][i] + partitionWeights[p];
                            if (weight < minWeight) {
                                minWeight = weight;
                                minPartition = p;
                            }
                        }
                    }
                    if (minPartition != -1) {
                        partitions[i].remove(j);
                        partitions[minPartition].add(node);
                        partitionWeights[i] -= nodeWeights[node];
                        partitionWeights[minPartition] += nodeWeights[node];
                        partitionEdges[i][minPartition] += nodeWeights[node];
                        partitionEdges[minPartition][i] += nodeWeights[node];
                        changed = true;
                        break;
                    }
                }
            }
        }

        List<List<Integer>> result = new ArrayList<>();
        for (int i = 0; i < k; i++) {
            result.add(partitions[i]);
        }
        System.out.println(result);
    }
}
