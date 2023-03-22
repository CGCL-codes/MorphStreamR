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
        Graph graph = new Graph(this.nodes.length);
        for (int i = 0; i < this.nodes.length; i++) {
            nodes[i] = new Node(i, 4);
            graph.addNode(nodes[i]);
        }
        graph.addEdge(0, 1, 1);
        graph.addEdge(1, 2, 4);
        graph.addEdge(2, 3, 1);
        System.out.println("start partitioning");
        GraphPartitioner graphPartitioner = new GraphPartitioner(graph.getNodeId(), graph.getNodeWeights(), graph.getEdges(),2);
        List<List<Integer>> partitions = graphPartitioner.run();
        System.out.println(partitions);
        System.out.println("partitioning finished");
    }
}
