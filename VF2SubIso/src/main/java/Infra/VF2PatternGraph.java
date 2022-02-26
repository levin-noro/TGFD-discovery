package Infra;

import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;

public class VF2PatternGraph {

    private Graph<Vertex, RelationshipEdge> pattern;
    private HashMap<Vertex, Integer> vertexToRadius = new HashMap<>();

    private int diameter;

    public String centerVertexType="";
    private int radius;

    public VF2PatternGraph(int diameter)
    {
        pattern = new DefaultDirectedGraph<>(RelationshipEdge.class);
        this.diameter=diameter;
    }

    public VF2PatternGraph(int diameter,String centerVertexType)
    {
        pattern = new DefaultDirectedGraph<>(RelationshipEdge.class);
        this.diameter=diameter;
        this.centerVertexType=centerVertexType;
    }

    public VF2PatternGraph()
    {
        pattern = new DefaultDirectedGraph<>(RelationshipEdge.class);
    }

    public Graph<Vertex, RelationshipEdge> getPattern() {
        return pattern;
    }

    public void setDiameter(int diameter) {
        this.diameter = diameter;
    }

    public int getDiameter() {
        return diameter;
    }

    public void addVertex(PatternVertex v)
    {
        pattern.addVertex(v);
    }

    public void addEdge(PatternVertex v1, PatternVertex v2, RelationshipEdge edge)
    {
        pattern.addEdge(v1,v2,edge);
    }

    public String getCenterVertexType()
    {
        if(centerVertexType.equals("")) {
            findCenterNode();
        }
        return centerVertexType;
    }

    public void setCenterVertexType(String centerVertexType) {
        this.centerVertexType = centerVertexType;
    }

    public int getSize()
    {
        return this.pattern.edgeSet().size();
    }

    private void findCenterNode()
    {
        if (this.pattern.vertexSet().size() == 1) {
            this.centerVertexType = this.pattern.vertexSet().stream().iterator().next().getTypes().stream().iterator().next();
            this.diameter = 0;
            return;
        }
        int patternDiameter=0;
        int patternRadius = this.pattern.vertexSet().size();
        Vertex centerNode=null;
        for (Vertex v:this.pattern.vertexSet()) {
            int d = calculateRadiusForGivenVertex(v);
            if(d>patternDiameter) {
                patternDiameter=d;
            }
            if (d < patternRadius) {
                patternRadius = d;
                centerNode = v;
            }
        }
        if(!centerNode.getTypes().isEmpty())
            this.centerVertexType= centerNode.getTypes().iterator().next();
        else
            this.centerVertexType="NoType";
        this.diameter=patternDiameter;
        this.setRadius(patternRadius);
    }

    public int calculateRadiusForGivenVertex(Vertex v) {
        // store results of earlier queries to calculate radius
        if (vertexToRadius.containsKey(v)) {
            return vertexToRadius.get(v);
        }

        // Define a HashMap to store visited vertices
        HashMap <Vertex,Integer> visited=new HashMap<>();

        // Create a queue for BFS
        LinkedList <Vertex> queue = new LinkedList<>();
        int d=Integer.MAX_VALUE;
        // Mark the current node as visited with distance 0 and then enqueue it
        visited.put(v,0);
        queue.add(v);

        //temp variables
        Vertex x,w;
        while (queue.size() != 0)
        {
            // Dequeue a vertex from queue and get its distance
            x = queue.poll();
            int distance = visited.get(x);
            for (RelationshipEdge edge : pattern.edgesOf(x)) {
                w = edge.getSource();
                if (w.equals(x)) w = edge.getTarget();
                // Check if the vertex is not visited
                if (!visited.containsKey(w)) {
                    // Check if the vertex is within the diameter
                    d = distance + 1;
                    //Enqueue the vertex and add it to the visited set
                    visited.put(w, distance + 1);
                    queue.add(w);
                }
            }
        }
        vertexToRadius.putIfAbsent(v, d);
        return d;
    }

    public VF2PatternGraph copy() {
        VF2PatternGraph newPattern = new VF2PatternGraph();
        for (Vertex v : pattern.vertexSet()) {
            PatternVertex newV = ((PatternVertex) v).copy();
            newPattern.addVertex(newV);
        }
        for (RelationshipEdge e : pattern.edgeSet()) {
            PatternVertex source = null;
            for (Vertex vertex : newPattern.getPattern().vertexSet()) {
                if (vertex.getTypes().contains(new ArrayList<>(((PatternVertex) e.getSource()).getTypes()).get(0))) {
                    source = (PatternVertex) vertex;
                }
            }
            PatternVertex target = null;
            for (Vertex vertex : newPattern.getPattern().vertexSet()) {
                if (vertex.getTypes().contains(new ArrayList<>(((PatternVertex) e.getTarget()).getTypes()).get(0))) {
                    target = (PatternVertex) vertex;
                }
            }
            newPattern.addEdge(source, target, new RelationshipEdge(e.getLabel()));
        }

        return newPattern;
    }

    @Override
    public String toString() {
        StringBuilder res= new StringBuilder("VF2PatternGraph{");
        if (pattern.edgeSet().size() > 0) {
            for (RelationshipEdge edge : pattern.edgeSet()) {
                res.append(edge.toString());
            }
        } else {
            for (Vertex v : pattern.vertexSet()) {
                res.append(v.toString());
            }
        }
        res.append('}');
        return res.toString();
    }

    public int getRadius() {
        return radius;
    }

    public void setRadius(int radius) {
        this.radius = radius;
    }
}