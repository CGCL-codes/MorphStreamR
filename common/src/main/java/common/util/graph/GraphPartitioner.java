package common.util.graph;

import java.util.*;

// Greedy algorithm
// The time complexity is O(n log n), where n is the number of nodes
public class GraphPartitioner {
    private List<Integer> nodes;  // Node
    private List<Edge> edges;  // Edge
    int[] nodeWeights;  // Node weight
    private final int[][] partitionEdgesWeight;// Edge weight between partitions
    private int[] partitionWeights;  // Partition weight
    private int[][] benefits;  // the benefit to move node_i to partition_j int[i][j] Vi -> Pj
    List<List<Integer>> partitions = new ArrayList<>();// Partition
    private int partitionCount;  // Partition count
    private int max_itr = 1000;  // Max iteration count
    public GraphPartitioner(List<Integer> vertices, int[] nodeWeights,List<Edge> edges, int partitionCount) {
        this.nodes = vertices;
        this.edges = edges;
        this.nodeWeights = nodeWeights;
        this.partitionCount = partitionCount;
        this.partitionEdgesWeight = new int[partitionCount][partitionCount];
        this.partitionWeights = new int[partitionCount];
        for (int i = 0; i < partitionCount; i++) {
            partitions.add(new ArrayList<>());
        }
        benefits = new int[nodes.size()][partitionCount];
    }

    public List<List<Integer>> run() {
        initPartitions();
        while (max_itr-- > 0) {
            int[] selectedBenefit = this.findMaxBenefit();
            int fromPartition = findPartition(selectedBenefit[0]);
            int targetPartition = selectedBenefit[1];
            if (benefits[fromPartition][targetPartition] > 0) {
                moveNode(fromPartition, targetPartition, selectedBenefit[0]);
                calcBenefit();
            } else {
                break;
            }
        }
        return partitions;
    }
    private void initPartitions() {
        //Partition G into a set of sub-graphs {G1,G2,. . . ,Gn} with about the same weight according to the key range
        int averageWeight = 0;
        for (int i = 0; i < nodes.size(); i++) {
            partitions.get(i % partitionCount).add(i);
            partitionWeights[i % partitionCount] += nodeWeights[i];
            averageWeight += nodeWeights[i];
        }
        averageWeight /= partitionCount;
        while (true) {
            boolean balanced = true;
            for (int i = 0; i < partitionCount; i++) {
                if (partitionWeights[i] > averageWeight) {
                    balanced = false;
                    int j = (i + 1) % partitionCount;
                    while (partitionWeights[j] <= averageWeight) {
                        j = (j + 1) % partitionCount;
                    }
                    int node = partitions.get(i).remove(partitions.get(i).size() - 1);
                    partitions.get(j).add(node);
                    partitionWeights[i] -= nodeWeights[node];
                    partitionWeights[j] += nodeWeights[node];
                    break;
                }
            }
            if (balanced) {
                break;
            }
        }
        //calcWeight();
        calcBenefit();
    }
    /**
     * Compute the edge weight between partitions
     */
    private void calcWeight() {
        for (Edge edge : this.edges) {
            int v1 = edge.getFrom();
            int v2 = edge.getTo();
            int partition1 = findPartition(v1);
            int partition2 = findPartition(v2);
            if (partition1 != partition2) {
                partitionEdgesWeight[partition1][partition2] += edge.getWeight();
                partitionEdgesWeight[partition2][partition1] += edge.getWeight();
            }
        }
    }
    private void initBenefit() {
        for (int i = 0; i < benefits.length; i++) {
            for (int j = 0; j < benefits[i].length; j++) {
                benefits[i][j] = 0;
            }
        }
    }
    /**
     * Compute the benefit to move node_i to partition_j
     */
    private void calcBenefit() {
        initBenefit();
        for (Edge edge : this.edges) {
            int v1 = edge.getFrom();
            int v2 = edge.getTo();
            int partition1 = findPartition(v1);
            int partition2 = findPartition(v2);
            for (int i = 0; i < partitionCount; i++) {
                //Move V1 to Pi
                if (i != partition1 && i == partition2) {
                    benefits[v1][i] += edge.getWeight();
                }
                if (i != partition1 && i != partition2) {
                    benefits[v1][i] -= edge.getWeight();
                }
                //Move V2 to Pi
                if (i != partition2 && i == partition1) {
                    benefits[v2][i] += edge.getWeight();
                }
                if (i != partition2 && i != partition1) {
                    benefits[v2][i] -= edge.getWeight();
                }
            }
        }
    }
    /**
     * Find the maximum benefit to move a node from one partition to another
     */
    public int[] findMaxBenefit() {
        int[] maxIndex = new int[2];
        int max = Integer.MIN_VALUE;
        for (int i = 0; i < this.benefits.length; i++) {
            for (int j = 0; j < this.benefits[i].length; j++) {
                if (this.benefits[i][j] > max) {
                    max = this.benefits[i][j];
                    maxIndex[0] = i;
                    maxIndex[1] = j;
                }
            }
        }
        return maxIndex;
    }
    /**
     * Move a node from one partition to another
     */
    private void moveNode(int fromPartition, int targetPartition, int nodeId) {
        this.partitions.get(fromPartition).remove((Object) nodeId);
        this.partitions.get(targetPartition).add(nodeId);
    }
    /**
     * Find the partition that a node belongs to
     */
    private int findPartition(int vertex) {
        for (int i = 0; i < partitionCount; i++) {
            if (this.partitions.get(i).contains(vertex)) {
                return i;
            }
        }
        return -1;
    }

}
