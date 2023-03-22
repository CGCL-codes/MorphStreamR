package common.util.graph;

import java.util.*;

// Greedy algorithm
// The time complexity is O(n log n), where n is the number of nodes
public class GraphPartitioner {
    private List<Integer> nodes;  // 顶点集合
    private List<Edge> edges;  // 边集合
    int[] nodeWeights;  // 顶点权重
    private final int[][] partitionEdgesWeight;
    private int[] partitionWeights;  // 分区权重
    private int[][] benefits;  // 移动顶点带来的收益 int[i][j] Vi -> Pj
    List<List<Integer>> partitions = new ArrayList<>();
    private int partitionCount;  // 分区数量
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
        int[] selectedBenefit = this.findMaxBenefit();
        int fromPartition = findPartition(selectedBenefit[0]);
        int targetPartition = selectedBenefit[1];
        if (benefits[fromPartition][targetPartition] > 0) {
            moveNode(fromPartition, targetPartition, selectedBenefit[0]);
            //calcWeight();
            calcBenefit();
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
    // 计算分区间边权重
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
                //Move V2 to P2
                if (i != partition2 && i == partition1) {
                    benefits[v2][i] += edge.getWeight();
                }
                if (i != partition2 && i != partition1) {
                    benefits[v2][i] -= edge.getWeight();
                }
            }
        }
    }
    //找到benefit的最大值
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

    // 移动顶点
    private void moveNode(int fromPartition, int targetPartition, int nodeId) {
        this.partitions.get(fromPartition).remove((Object) nodeId);
        this.partitions.get(targetPartition).add(nodeId);
    }
    // 查找顶点所在的分区
    private int findPartition(int vertex) {
        for (int i = 0; i < partitionCount; i++) {
            if (this.partitions.get(i).contains(vertex)) {
                return i;
            }
        }
        return -1;
    }

}
