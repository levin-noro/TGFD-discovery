package Infra;

import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;

import java.io.Serializable;
import java.util.*;

public class VF2DataGraph implements Serializable {

    private Graph<Vertex, RelationshipEdge> graph = new DefaultDirectedGraph<>(RelationshipEdge.class);

    private HashMap<String, Vertex> nodeMap;

    public VF2DataGraph()
    {
        nodeMap= new HashMap<>();
    }

    public VF2DataGraph(Graph <Vertex, RelationshipEdge> graph)
    {
        nodeMap= new HashMap<>();
        this.graph = graph;
        for (Vertex v:graph.vertexSet()) {
            DataVertex dataV=(DataVertex) v;
            if(!nodeMap.containsKey(dataV.getVertexURI())) {
                nodeMap.put(dataV.getVertexURI(), dataV);
            }
        }
    }

    public Graph<Vertex, RelationshipEdge> getGraph() {
        return graph;
    }

    public boolean deleteVertex(DataVertex vertex)
    {
        if(!nodeMap.containsKey(vertex.getVertexURI()))
            return false;

        DataVertex v = (DataVertex) this.getNode(vertex.getVertexURI());
        if (v == null)
            return false;
        List<RelationshipEdge> edgesToDelete = new ArrayList<>(graph.edgesOf(v));
        for (RelationshipEdge e : edgesToDelete) {
            graph.removeEdge(e);
        }
        boolean deleteVertex = graph.removeVertex(v);
        nodeMap.remove(v.getVertexURI());

        return deleteVertex;
    }

    public boolean addVertex(DataVertex v)
    {
        if(nodeMap.containsKey(v.getVertexURI()))
            return false;

        boolean addedVertex = graph.addVertex(v);
        nodeMap.put(v.getVertexURI(),v);
        return addedVertex;
    }

    public Vertex getNode(String vertexURI)
    {
        return nodeMap.getOrDefault(vertexURI.toLowerCase(), null);
    }

    public boolean addEdge(DataVertex v1, DataVertex v2, RelationshipEdge edge)
    {
        boolean addedEdge = graph.addEdge(v1, v2, edge);
        if (!addedEdge) { // if multiple edges exist between v1 and v2, keep edge that is alphabetically first
            for (RelationshipEdge e: graph.getAllEdges(v1,v2)) { // In VF2, there can only be 1 edge between v1 and v2
                if (e.getLabel().compareTo(edge.getLabel()) > 0) {
                    if (removeEdge(v1, v2, e))
                        addedEdge = graph.addEdge(v1, v2, edge);
                    return addedEdge;
                }
            }
        }
        return addedEdge;
    }

    public boolean removeEdge(DataVertex v1, DataVertex v2, RelationshipEdge edge)
    {
        for (RelationshipEdge e:graph.outgoingEdgesOf(v1)) {
            DataVertex target=(DataVertex) e.getTarget();
            if(target.getVertexURI().equals(v2.getVertexURI()) && edge.getLabel().equals(e.getLabel()))
            {
                boolean removedEdge = this.graph.removeEdge(e);
                return removedEdge;
            }
        }
        return false;
    }

    public int getSize()
    {
        return nodeMap.size();
    }

    public HashMap<String, Vertex> getNodeMap() {
        return nodeMap;
    }

    // TO-DO: Merge with other getSubGraphWithinDiameter method?
    public Graph<Vertex, RelationshipEdge> getSubGraphWithinDiameter(DataVertex center, int diameter, Set<String> edgeLabels, Set<RelationshipEdge> edgeSet, Set<String> uris)
    {
        Graph<Vertex, RelationshipEdge> subgraph = new DefaultDirectedGraph<>(RelationshipEdge.class);

        List<Vertex> withinDiameter=new ArrayList<>();

        // Define a HashMap to store visited vertices
        HashMap<String,Integer> visited=new HashMap<>();

        // Create a queue for BFS
        LinkedList<DataVertex> queue = new LinkedList<>();

        // Mark the current node as visited with distance 0 and then enqueue it
        visited.put(center.getVertexURI(),0);
        queue.add(center);
        // Store the center as the node within the diameter
        withinDiameter.add(center);
        //temp variables
        DataVertex v,w;

        while (queue.size() != 0)
        {
            // Dequeue a vertex from queue and get its distance
            v = queue.poll();
            int distance=visited.get(v.getVertexURI());

            // Outgoing edges
            for (RelationshipEdge edge : graph.outgoingEdgesOf(v)) {

                if (!edgeLabels.contains(edge.getLabel())) continue;

                boolean skip = true;
                if (uris.contains(((DataVertex)(edge.getSource())).getVertexURI()) || uris.contains(((DataVertex)(edge.getTarget())).getVertexURI())) {
                    skip = false;
                } else {
                    for (RelationshipEdge patternEdge : edgeSet) {
                        if (edge.getLabel().equals(patternEdge.getLabel())
                                && edge.getSource().getTypes().contains(patternEdge.getSource().getTypes().iterator().next())
                                && edge.getTarget().getTypes().contains(patternEdge.getTarget().getTypes().iterator().next())) {
                            skip = false;
                            break;
                        } else if (uris.contains(((DataVertex) (edge.getSource())).getVertexURI()) || uris.contains(((DataVertex) (edge.getTarget())).getVertexURI())) {
                            skip = false;
                            break;
                        }
                    }
                }
                if (skip) continue;

                w = (DataVertex) edge.getTarget();

                // Check if the vertex is not visited
                if (!visited.containsKey(w.getVertexURI())) {

                    // Check if the vertex is within the diameter
                    if (distance + 1 <= diameter) {

                        //Enqueue the vertex and add it to the visited set
                        visited.put(w.getVertexURI(), distance + 1);
                        queue.add(w);
                        withinDiameter.add(w);
                    }

                }
            }
            // Incoming edges
            for (RelationshipEdge edge : graph.incomingEdgesOf(v)) {

                if (!edgeLabels.contains(edge.getLabel())) continue;

                boolean skip = true;
                if (uris.contains(((DataVertex)(edge.getSource())).getVertexURI()) || uris.contains(((DataVertex)(edge.getTarget())).getVertexURI())) {
                    skip = false;
                } else {
                    for (RelationshipEdge patternEdge : edgeSet) {
                        if (edge.getLabel().equals(patternEdge.getLabel())
                                && edge.getSource().getTypes().contains(patternEdge.getSource().getTypes().iterator().next())
                                && edge.getTarget().getTypes().contains(patternEdge.getTarget().getTypes().iterator().next())) {
                            skip = false;
                            break;
                        }
                    }
                }
                if (skip) continue;

                w = (DataVertex) edge.getSource();

                // Check if the vertex is not visited
                if (!visited.containsKey(w.getVertexURI())) {

                    // Check if the vertex is within the diameter
                    if (distance + 1 <= diameter) {

                        //Enqueue the vertex and add it to the visited set
                        visited.put(w.getVertexURI(), distance + 1);
                        queue.add(w);
                        withinDiameter.add(w);
                    }

                }
            }
        }
        for (Vertex vertex:withinDiameter) {
            subgraph.addVertex(vertex);
        }
        for (Vertex source:withinDiameter) {
            for (RelationshipEdge e:graph.outgoingEdgesOf(source)) {
                if (!edgeLabels.contains(e.getLabel())) continue;
                boolean skip = true;
                if (uris.contains(((DataVertex)(e.getSource())).getVertexURI()) || uris.contains(((DataVertex)(e.getTarget())).getVertexURI())) {
                    skip = false;
                } else {
                    for (RelationshipEdge patternEdge : edgeSet) {
                        if (e.getLabel().equals(patternEdge.getLabel())
                                && e.getSource().getTypes().contains(patternEdge.getSource().getTypes().iterator().next())
                                && e.getTarget().getTypes().contains(patternEdge.getTarget().getTypes().iterator().next())) {
                            skip = false;
                            break;
                        }
                    }
                }
                if (skip) continue;
                DataVertex target=(DataVertex)e.getTarget();
                if(visited.containsKey(target.getVertexURI()))
                    subgraph.addEdge(e.getSource(),e.getTarget(),e);
            }
        }
        return subgraph;
    }

    public int getSubGraphSize(DataVertex center, int diameter)
    {
        int size=0;

        List<Vertex> withinDiameter=new ArrayList<>();

        // Define a HashMap to store visited vertices
        HashMap<String,Integer> visited=new HashMap<>();

        // Create a queue for BFS
        LinkedList<DataVertex> queue = new LinkedList<>();
        // Mark the current node as visited with distance 0 and then enqueue it
        visited.put(center.getVertexURI(),0);
        queue.add(center);
        // Store the center as the node within the diameter
        withinDiameter.add(center);
        //temp variables
        DataVertex v,w;
        while (queue.size() != 0)
        {
            // Dequeue a vertex from queue and get its distance
            v = queue.poll();
            int distance=visited.get(v.getVertexURI());
            // Outgoing edges
            for (RelationshipEdge edge : graph.outgoingEdgesOf(v)) {
                w = (DataVertex) edge.getTarget();
                // Check if the vertex is not visited
                if (!visited.containsKey(w.getVertexURI())) {
                    // Check if the vertex is within the diameter
                    if (distance + 1 <= diameter) {
                        //Enqueue the vertex and add it to the visited set
                        visited.put(w.getVertexURI(), distance + 1);
                        queue.add(w);
                        withinDiameter.add(w);
                    }
                }
            }
            // Incoming edges
            for (RelationshipEdge edge : graph.incomingEdgesOf(v)) {
                w = (DataVertex) edge.getSource();
                // Check if the vertex is not visited
                if (!visited.containsKey(w.getVertexURI())) {
                    // Check if the vertex is within the diameter
                    if (distance + 1 <= diameter) {
                        //Enqueue the vertex and add it to the visited set
                        visited.put(w.getVertexURI(), distance + 1);
                        queue.add(w);
                        withinDiameter.add(w);
                    }

                }
            }
        }
        for (Vertex source : withinDiameter) {
            for (RelationshipEdge e : graph.outgoingEdgesOf(source)) {
                DataVertex target = (DataVertex) e.getTarget();
                if (visited.containsKey(target.getVertexURI()))
                    size++;
            }
        }
        return size;
    }

    public List<Vertex> getVerticesWithinDiameter(DataVertex center, int diameter)
    {
        List<Vertex> withinDiameter=new ArrayList<>();

        // Define a HashMap to store visited vertices
        HashMap<String,Integer> visited=new HashMap<>();

        // Create a queue for BFS
        LinkedList<DataVertex> queue = new LinkedList<>();

        // Mark the current node as visited with distance 0 and then enqueue it
        visited.put(center.getVertexURI(),0);
        queue.add(center);
        // Store the center as the node within the diameter
        withinDiameter.add(center);
        //temp variables
        DataVertex v,w;

        while (queue.size() != 0)
        {
            // Dequeue a vertex from queue and get its distance
            v = queue.poll();
            int distance=visited.get(v.getVertexURI());

            // Outgoing edges
            for (RelationshipEdge edge : graph.outgoingEdgesOf(v)) {
                w = (DataVertex) edge.getTarget();

                // Check if the vertex is not visited
                if (!visited.containsKey(w.getVertexURI())) {

                    // Check if the vertex is within the diameter
                    if (distance + 1 <= diameter) {

                        //Enqueue the vertex and add it to the visited set
                        visited.put(w.getVertexURI(), distance + 1);
                        queue.add(w);
                        withinDiameter.add(w);
                    }

                }
            }
            // Incoming edges
            for (RelationshipEdge edge : graph.incomingEdgesOf(v)) {
                w = (DataVertex) edge.getSource();

                // Check if the vertex is not visited
                if (!visited.containsKey(w.getVertexURI())) {

                    // Check if the vertex is within the diameter
                    if (distance + 1 <= diameter) {

                        //Enqueue the vertex and add it to the visited set
                        visited.put(w.getVertexURI(), distance + 1);
                        queue.add(w);
                        withinDiameter.add(w);
                    }

                }
            }
        }
        return withinDiameter;
    }

    public Graph<Vertex, RelationshipEdge> getFragmentedGraph(List<FocusNode> focusNodes)
    {
        Graph<Vertex, RelationshipEdge> fragmentedGraph = new DefaultDirectedGraph<>(RelationshipEdge.class);

        HashSet<String> allVisitedVertices=new HashSet <>();

        for (FocusNode focusNode:focusNodes) {
            DataVertex centerNode= (DataVertex) this.nodeMap.get(focusNode.getNodeURI());
            if(centerNode==null)
                continue;

            List<Vertex> withinDiameter=new ArrayList<>();

            // Define a HashMap to store visited vertices
            HashMap<String,Integer> visited=new HashMap<>();

            // Create a queue for BFS
            LinkedList<DataVertex> queue = new LinkedList<>();

            // Mark the current node as visited with distance 0 and then enqueue it
            visited.put(centerNode.getVertexURI(),0);
            queue.add(centerNode);
            // Store the center as the node within the diameter
            withinDiameter.add(centerNode);
            //temp variables
            DataVertex v,w;

            while (queue.size() != 0)
            {
                // Dequeue a vertex from queue and get its distance
                v = queue.poll();
                int distance=visited.get(v.getVertexURI());

                // Outgoing edges
                for (RelationshipEdge edge : graph.outgoingEdgesOf(v)) {
                    w = (DataVertex) edge.getTarget();

                    // Check if the vertex is not visited
                    if (!visited.containsKey(w.getVertexURI())) {

                        // Check if the vertex is within the diameter
                        if (distance + 1 <= focusNode.getDiameter()) {

                            //Enqueue the vertex and add it to the visited set
                            visited.put(w.getVertexURI(), distance + 1);
                            queue.add(w);
                            withinDiameter.add(w);
                        }

                    }
                }
                // Incoming edges
                for (RelationshipEdge edge : graph.incomingEdgesOf(v)) {
                    w = (DataVertex) edge.getSource();

                    // Check if the vertex is not visited
                    if (!visited.containsKey(w.getVertexURI())) {

                        // Check if the vertex is within the diameter
                        if (distance + 1 <= focusNode.getDiameter()) {

                            //Enqueue the vertex and add it to the visited set
                            visited.put(w.getVertexURI(), distance + 1);
                            queue.add(w);
                            withinDiameter.add(w);
                        }

                    }
                }
            }
            for (Vertex vertex:withinDiameter) {
                DataVertex dataV=(DataVertex) vertex;
                if(!allVisitedVertices.contains(dataV.getVertexURI()))
                {
                    allVisitedVertices.add(dataV.getVertexURI());
                    fragmentedGraph.addVertex(vertex);
                }
            }
            for (Vertex source:withinDiameter) {
                for (RelationshipEdge e:graph.outgoingEdgesOf(source)) {
                    DataVertex target=(DataVertex)e.getTarget();
                    if(visited.containsKey(target.getVertexURI()))
                    {
                        //We need to check if that edge is already added to the fragmented graph
                        boolean exist=false;
                        for (RelationshipEdge e_f:fragmentedGraph.outgoingEdgesOf(source)) {
                            if(e_f.equals(e))
                            {
                                exist=true;
                                break;
                            }
                        }
                        if(!exist)
                            fragmentedGraph.addEdge(e.getSource(),e.getTarget(),e);
                    }
                }
            }
        }
        return fragmentedGraph;
    }

    public Graph<Vertex, RelationshipEdge> getSubGraphWithinDiameter(DataVertex center, int diameter, Set<String> edgeLabels, Set<RelationshipEdge> edgeSet) {
        Graph<Vertex, RelationshipEdge> subgraph = new DefaultDirectedGraph<>(RelationshipEdge.class);

        List<Vertex> withinDiameter=new ArrayList<>();

        // Define a HashMap to store visited vertices
        HashMap<String,Integer> visited=new HashMap<>();

        // Create a queue for BFS
        LinkedList<DataVertex> queue = new LinkedList<>();

        // Mark the current node as visited with distance 0 and then enqueue it
        visited.put(center.getVertexURI(),0);
        queue.add(center);
        // Store the center as the node within the diameter
        withinDiameter.add(center);
        //temp variables
        DataVertex v,w;

        while (queue.size() != 0)
        {
            // Dequeue a vertex from queue and get its distance
            v = queue.poll();
            int distance=visited.get(v.getVertexURI());

            // Outgoing edges
            for (RelationshipEdge edge : graph.outgoingEdgesOf(v)) {

                if (!edgeLabels.contains(edge.getLabel())) continue;

                boolean skip = true;
                for (RelationshipEdge patternEdge: edgeSet) {
                    if (edge.getLabel().equals(patternEdge.getLabel())
                            && edge.getSource().getTypes().contains(patternEdge.getSource().getTypes().iterator().next())
                            && edge.getTarget().getTypes().contains(patternEdge.getTarget().getTypes().iterator().next())) {
                        skip = false;
                        break;
                    }
                }
                if (skip) continue;

                w = (DataVertex) edge.getTarget();

                // Check if the vertex is not visited
                if (!visited.containsKey(w.getVertexURI())) {

                    // Check if the vertex is within the diameter
                    if (distance + 1 <= diameter) {

                        //Enqueue the vertex and add it to the visited set
                        visited.put(w.getVertexURI(), distance + 1);
                        queue.add(w);
                        withinDiameter.add(w);
                    }

                }
            }
            // Incoming edges
            for (RelationshipEdge edge : graph.incomingEdgesOf(v)) {

                if (!edgeLabels.contains(edge.getLabel())) continue;

                boolean skip = true;
                for (RelationshipEdge patternEdge: edgeSet) {
                    if (edge.getLabel().equals(patternEdge.getLabel())
                            && edge.getSource().getTypes().contains(patternEdge.getSource().getTypes().iterator().next())
                            && edge.getTarget().getTypes().contains(patternEdge.getTarget().getTypes().iterator().next())) {
                        skip = false;
                        break;
                    }
                }
                if (skip) continue;


                w = (DataVertex) edge.getSource();

                // Check if the vertex is not visited
                if (!visited.containsKey(w.getVertexURI())) {

                    // Check if the vertex is within the diameter
                    if (distance + 1 <= diameter) {

                        //Enqueue the vertex and add it to the visited set
                        visited.put(w.getVertexURI(), distance + 1);
                        queue.add(w);
                        withinDiameter.add(w);
                    }

                }
            }
        }
        for (Vertex vertex:withinDiameter) {
            subgraph.addVertex(vertex);
        }
        for (Vertex source:withinDiameter) {
            for (RelationshipEdge e:graph.outgoingEdgesOf(source)) {
                if (!edgeLabels.contains(e.getLabel())) continue;
                boolean skip = true;
                for (RelationshipEdge patternEdge: edgeSet) {
                    if (e.getLabel().equals(patternEdge.getLabel())
                            && e.getSource().getTypes().contains(patternEdge.getSource().getTypes().iterator().next())
                            && e.getTarget().getTypes().contains(patternEdge.getTarget().getTypes().iterator().next())) {
                        skip = false;
                        break;
                    }
                }
                if (skip) continue;
                DataVertex target=(DataVertex)e.getTarget();
                if(visited.containsKey(target.getVertexURI()))
                    subgraph.addEdge(e.getSource(),e.getTarget(),e);
            }
        }
        return subgraph;
    }
}
