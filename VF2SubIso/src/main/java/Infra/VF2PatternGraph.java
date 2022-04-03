package Infra;

import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;

import java.util.*;
import java.util.stream.Collectors;

public class VF2PatternGraph {

    private Graph<Vertex, RelationshipEdge> pattern;
    private HashMap<Vertex, Integer> vertexToRadius = new HashMap<>();

    private int diameter;

    public String centerVertexType = "";
    private int radius;
    private Vertex centerVertex;
    private Vertex firstNode;
    private PatternType patternType = null;

    public VF2PatternGraph(int diameter) {
        pattern = new DefaultDirectedGraph<>(RelationshipEdge.class);
        this.diameter = diameter;
    }

    public VF2PatternGraph(int diameter, String centerVertexType) {
        pattern = new DefaultDirectedGraph<>(RelationshipEdge.class);
        this.diameter = diameter;
        this.centerVertexType = centerVertexType;
    }

    public VF2PatternGraph() {
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

    public void addVertex(PatternVertex v) {
        pattern.addVertex(v);
    }

    public void addEdge(PatternVertex v1, PatternVertex v2, RelationshipEdge edge) {
        pattern.addEdge(v1, v2, edge);
    }

    public String getCenterVertexType() {
        if (centerVertexType.equals("")) {
            findCenterNode();
        }
        return centerVertexType;
    }

    public void assignOptimalCenterVertex(Map<String, Double> vertexTypesToAvgInDegreeMap, boolean isFastMatching) {
        if (this.getPattern().edgeSet().size() == 1) {
            RelationshipEdge e = this.getPattern().edgeSet().iterator().next();
            Vertex source = e.getSource();
            String sourceType = source.getTypes().iterator().next();
            Vertex target = e.getTarget();
            String targetType = target.getTypes().iterator().next();
            Vertex centerVertex = vertexTypesToAvgInDegreeMap.get(sourceType) > vertexTypesToAvgInDegreeMap.get(targetType) ? source : target;
            this.setCenterVertex(centerVertex);
            this.setRadius(1);
            this.setDiameter(1);
        } else {
//            boolean considerAlternativeParents = true;
            this.getCenterVertexType();
            if (isFastMatching && this.getPattern().edgeSet().size() > 2) {
                if (this.getPatternType() == PatternType.Line) {
                    this.setCenterVertex(this.getFirstNode());
//                    considerAlternativeParents = false;
                }
            } else {
                int minRadius = this.getPattern().vertexSet().size();
                for (Vertex newV : this.getPattern().vertexSet()) {
                    minRadius = Math.min(minRadius, this.calculateRadiusForGivenVertex(newV));
                }
                Map<Vertex, Double> maxDegreeTypes = new HashMap<>();
                for (Vertex newV : this.getPattern().vertexSet()) {
                    if (minRadius == this.calculateRadiusForGivenVertex(newV)) {
                        String type = newV.getTypes().iterator().next();
                        maxDegreeTypes.put(newV, vertexTypesToAvgInDegreeMap.get(type));
                    }
                }
                if (maxDegreeTypes.size() <= 0)
                    throw new IllegalArgumentException("maxDegreeTypes.size() <= 0");
                List<Map.Entry<Vertex, Double>> entries = new ArrayList<>(maxDegreeTypes.entrySet());
                entries.sort(new Comparator<Map.Entry<Vertex, Double>>() {
                    @Override
                    public int compare(Map.Entry<Vertex, Double> o1, Map.Entry<Vertex, Double> o2) {
                        return o2.getValue().compareTo(o1.getValue());
                    }
                });
                Vertex centerVertex = entries.get(0).getKey();
                this.setCenterVertex(centerVertex);
            }
        }
    }

    private void setCenterVertexType(String centerVertexType) {
        this.centerVertexType = centerVertexType;
    }

    public void setCenterVertex(Vertex centerVertex) {
        this.centerVertex = centerVertex;
        this.setCenterVertexType(centerVertex.getTypes().iterator().next());
    }

    public int getSize() {
        return this.pattern.edgeSet().size();
    }

    private void findCenterNode() {
        if (this.pattern.vertexSet().size() == 1) {
            this.centerVertexType = this.pattern.vertexSet().stream().iterator().next().getTypes().stream().iterator().next();
            this.diameter = 0;
            return;
        }
        int patternDiameter = 0;
        int patternRadius = this.pattern.vertexSet().size();
        Vertex centerNode = null;
        Vertex firstNode = null;
        for (Vertex v : this.pattern.vertexSet()) {
            int d = calculateRadiusForGivenVertex(v);
            if (d > patternDiameter) {
                patternDiameter = d;
                firstNode = v;
            }
            if (d < patternRadius) {
                patternRadius = d;
                centerNode = v;
            }
        }
        if (centerNode != null && !centerNode.getTypes().isEmpty()) {
            this.setCenterVertex(centerNode);
            this.firstNode = firstNode;
            this.centerVertexType = this.centerVertex.getTypes().iterator().next();
        } else {
            this.centerVertexType = "NoType";
        }
        this.diameter = patternDiameter;
        this.setRadius(patternRadius);
    }

    public int calculateRadiusForGivenVertex(Vertex v) {
        // store results of earlier queries to calculate radius
        if (vertexToRadius.containsKey(v)) {
            return vertexToRadius.get(v);
        }

        // Define a HashMap to store visited vertices
        HashMap<Vertex, Integer> visited = new HashMap<>();

        // Create a queue for BFS
        LinkedList<Vertex> queue = new LinkedList<>();
        int d = Integer.MAX_VALUE;
        // Mark the current node as visited with distance 0 and then enqueue it
        visited.put(v, 0);
        queue.add(v);

        //temp variables
        Vertex x, w;
        while (queue.size() != 0) {
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
        StringBuilder res = new StringBuilder("VF2PatternGraph{");
        if (pattern.edgeSet().size() > 0) {
            for (RelationshipEdge edge : pattern.edgeSet()) {
                res.append("\n\t");
                res.append(edge.toString());
            }
        } else {
            for (Vertex v : pattern.vertexSet()) {
                res.append("\n\t");
                res.append(v.toString());
            }
        }
        res.append("\n");
        res.append('}');
        return res.toString();
    }

    public int getRadius() {
        return radius;
    }

    public void setRadius(int radius) {
        this.radius = radius;
    }

    public Vertex getCenterVertex() {
        if (this.centerVertex == null) {
            findCenterNode();
        }
        return centerVertex;
    }

    public Vertex getFirstNode() {
        return firstNode;
    }

    public PatternType getPatternType() {
        if (patternType == null) {
            assignPatternType();
        }
        return patternType;
    }

    public void assignPatternType() {
        int patternSize = this.getSize();
        if (patternSize < 1)
            this.setPatternType(PatternType.SingleNode);
        else if (patternSize == 1)
            this.setPatternType(PatternType.SingleEdge);
        else {
            if (patternSize == 2)
                this.setPatternType(PatternType.DoubleEdge);
            else { // > 2
                if (this.getPattern().edgesOf(this.getCenterVertex()).size() == patternSize)
                    this.setPatternType(PatternType.Star);
                else if (isLinePattern())
                    this.setPatternType(PatternType.Line);
                else if (isCirclePattern())
                    this.setPatternType(PatternType.Circle);
                else
                    this.setPatternType(PatternType.Complex);
            }
        }
        System.out.println("PatternType: " + this.getPatternType().name());
    }

    private boolean isLinePattern() {
        List<Integer> degrees = this.getPattern().vertexSet().stream().map(vertex -> this.getPattern().edgesOf(vertex).size()).collect(Collectors.toList());
        return degrees.stream().filter(degree -> degree == 1).count() == 2 && degrees.stream().filter(degree -> degree == 2).count() == this.getPattern().vertexSet().size() - 2;
    }

    private boolean isCirclePattern() {
        return this.getPattern().vertexSet().stream().allMatch(vertex -> this.getPattern().edgesOf(vertex).size() == 2);
    }

    private void setPatternType(PatternType patternType) {
        this.patternType = patternType;
    }
}