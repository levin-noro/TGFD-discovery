package infra;

import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;

import java.util.HashMap;

public class VF2DataGraph {

    private Graph<vertex, relationshipEdge> graph = new DefaultDirectedGraph<>(relationshipEdge.class);

    private HashMap<Integer,vertex> nodeMap;

    public VF2DataGraph()
    {
        nodeMap= new HashMap<>();
    }

    public Graph<vertex, relationshipEdge> getGraph() {
        return graph;
    }

    public void addVertex(dataVertex v)
    {
        if(!nodeMap.containsKey(v.getHashValue()))
        {
            graph.addVertex(v);
            nodeMap.put(v.getHashValue(),v);
        }
        else
        {
            System.out.println("Vertex URI: " + v.getVertexURI());
        }
    }

    public vertex getNode(int nodeID)
    {
        return nodeMap.getOrDefault(nodeID, null);
    }

    public void addEdge(dataVertex v1, dataVertex v2, relationshipEdge edge)
    {
        graph.addEdge(v1,v2,edge);
    }

    public int getSize()
    {
        return nodeMap.size();
    }
}
