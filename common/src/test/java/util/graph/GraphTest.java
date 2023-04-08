package util.graph;

import common.util.graph.Graph;
import common.util.graph.GraphPartitioner;
import common.util.graph.Node;
import org.junit.Test;

import java.util.List;

public class GraphTest {
    public Node[] nodes;
    @Test
    public void testPartition() {
        this.nodes = new Node[4];
        Graph graph = new Graph(this.nodes.length, 2);
        for (int i = 0; i < this.nodes.length; i++) {
            graph.addNode(i, 4);
        }
        graph.addEdge(0, 1, 1);
        graph.addEdge(1, 2, 4);
        graph.addEdge(2, 3, 1);
        System.out.println("start partitioning");
        GraphPartitioner graphPartitioner = new GraphPartitioner(graph.getNodeSize(), graph.getNodeWeights(), graph.getEdges(),2);
        List<List<Integer>> partitions = graphPartitioner.run();
        System.out.println(partitions);
        System.out.println("partitioning finished");
        int[] nodeWeights = new int[4];
        for (int i = 0; i < 4; i++) {
            nodeWeights[i] = 4;
        }
        int[] ints = nodeWeights;
        for (int i = 0; i < 4; i++) {
            System.out.println(ints[i]);
        }
        for (int i = 0; i < 4; i++) {
            nodeWeights[i] = 5;
        }
        for (int i = 0; i < 4; i++) {
            System.out.println(ints[i]);
        }
    }
}
