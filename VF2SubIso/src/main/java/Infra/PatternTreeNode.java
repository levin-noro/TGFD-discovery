package Infra;

import org.jgrapht.Graph;

import java.util.*;
import java.util.stream.Collectors;

public class PatternTreeNode {
    private VF2PatternGraph pattern;
    private Double patternSupport = null;
    private final PatternTreeNode parentNode;
    private ArrayList<PatternTreeNode> subgraphParents = new ArrayList<>();
    private PatternTreeNode centerVertexParent = null;
    private final String edgeString;
    private boolean isPruned = false;
    private ArrayList<AttributeDependency> lowSupportDependencies = new ArrayList<>();
    private ArrayList<AttributeDependency> minimalDependencies = new ArrayList<>();
    private ArrayList<AttributeDependency> minimalConstantDependencies = new ArrayList<>();
    private ArrayList<ArrayList<DataVertex>> listOfCenterVertices = null;
    private Map<String, List<Integer>> entityURIs = null;
    private boolean isAcyclic = true;
    private ArrayList<AttributeDependency> zeroEntityDependencies = new ArrayList<>();
//    private PatternType patternType;

    public PatternTreeNode(VF2PatternGraph pattern, PatternTreeNode parentNode, String edgeString) {
        this.setPattern(pattern);
        this.parentNode = parentNode;
        this.edgeString = edgeString;
    }

    public PatternTreeNode(VF2PatternGraph pattern) {
        this.setPattern(pattern);
        this.parentNode = null;
        this.edgeString = null;
    }

    public Graph<Vertex, RelationshipEdge> getGraph() {
        return pattern.getPattern();
    }

    private void setPattern(VF2PatternGraph pattern) {
        this.pattern = pattern;
        this.pattern.assignPatternType();
//        this.checkIfCyclesExist();
    }

//    private void assignPatternType() {
//        int patternSize = this.getPattern().getSize();
//        if (patternSize < 1)
//            this.setPatternType(PatternType.SingleNode);
//        else if (patternSize == 1)
//            this.setPatternType(PatternType.SingleEdge);
//        else {
//            if (patternSize == 2)
//                this.setPatternType(PatternType.DoubleEdge);
//            else { // > 2
//                if (this.getGraph().edgesOf(this.getPattern().getCenterVertex()).size() == patternSize) // TODO: getCenterVertex can return null
//                    this.setPatternType(PatternType.Star);
//                else if (isLinePattern())
//                    this.setPatternType(PatternType.Line);
//                else if (isCirclePattern())
//                    this.setPatternType(PatternType.Circle);
//                else
//                    this.setPatternType(PatternType.Complex);
//            }
//        }
//        System.out.println("PatternType: "+ this.getPatternType().name());
//    }

    private boolean isLinePattern() {
        List<Integer> degrees = this.getGraph().vertexSet().stream().map(vertex -> this.getGraph().edgesOf(vertex).size()).collect(Collectors.toList());
        return degrees.stream().filter(degree -> degree == 1).count() == 2 && degrees.stream().filter(degree -> degree == 2).count() == this.getGraph().vertexSet().size() - 2;
    }

    private boolean isCirclePattern() {
        return this.getGraph().vertexSet().stream().allMatch(vertex -> this.getGraph().edgesOf(vertex).size() == 2);
    }

    private void checkIfCyclesExist() {
        for (Vertex currentVertex: this.getGraph().vertexSet()) {
            Map<Vertex, Vertex> visited = new HashMap<>();
            visited.put(currentVertex, null);
            LinkedList<Vertex> queue = new LinkedList<>();
            queue.add(currentVertex);
            Vertex x,w;
            while (queue.size() != 0) {
                x = queue.poll();
                for (RelationshipEdge edge : this.getGraph().edgesOf(x)) {
                    w = edge.getSource();
                    if (w.equals(x)) w = edge.getTarget();
                    if (visited.get(x) != null && visited.get(x).equals(w)) continue;
                    if (visited.containsKey(w)) {
                        this.isAcyclic = false;
                        return;
                    } else {
                        visited.putIfAbsent(w, x);
                        queue.add(w);
                    }
                }
            }
        }
    }

    public VF2PatternGraph getPattern() {
        return pattern;
    }

    public void setPatternSupport(double patternSupport) {
        this.patternSupport = patternSupport;
    }

    public Double getPatternSupport() {
        return this.patternSupport;
    }

    public void setIsPruned() {
        System.out.println("Marking "+this.pattern+" as pruned.");
        this.isPruned = true;
    }

    public boolean isPruned() {
        return this.isPruned;
    }

    @Override
    public String toString() {
        return "PatternTreeNode{" +
                "pattern=" + (pattern.getPattern().edgeSet().size() > 0 ? pattern : pattern.getPattern().vertexSet()) +
                ",\n support=" + patternSupport +
                '}';
    }

    public void addZeroEntityDependency(AttributeDependency dependency) {
        this.zeroEntityDependencies.add(dependency);
    }

    public void addLowSupportDependency(AttributeDependency dependency) {
        this.lowSupportDependencies.add(dependency);
    }

    public ArrayList<AttributeDependency> getLowSupportDependencies() {
        return this.lowSupportDependencies;
    }

    public ArrayList<AttributeDependency> getZeroEntityDependencies() {
        return this.zeroEntityDependencies;
    }

    public ArrayList<AttributeDependency> getLowSupportDependenciesOnThisPath() {
        PatternTreeNode currPatternTreeNode = this;
        ArrayList<AttributeDependency> lowSupportPaths = new ArrayList<>(currPatternTreeNode.getLowSupportDependencies());
        for (PatternTreeNode parentNode: subgraphParents) {
            lowSupportPaths.addAll(parentNode.getLowSupportDependenciesOnThisPath());
        }
        return lowSupportPaths;
    }

    public ArrayList<AttributeDependency> getZeroEntityDependenciesOnThisPath() {
        PatternTreeNode currPatternTreeNode = this;
        ArrayList<AttributeDependency> zeroEntityPaths = new ArrayList<>(currPatternTreeNode.getZeroEntityDependencies());
        for (PatternTreeNode parentNode: subgraphParents) {
            zeroEntityPaths.addAll(parentNode.getZeroEntityDependenciesOnThisPath());
        }
        return zeroEntityPaths;
    }

    public void addMinimalConstantDependency(AttributeDependency constantPath) {
        this.minimalConstantDependencies.add(constantPath);
    }

    public ArrayList<AttributeDependency> getMinimalConstantDependencies() {
        return this.minimalConstantDependencies;
    }

    public ArrayList<AttributeDependency> getAllMinimalConstantDependenciesOnThisPath() {
        PatternTreeNode currPatternTreeNode = this;
        ArrayList<AttributeDependency> minimalConstantPaths = new ArrayList<>(currPatternTreeNode.getMinimalConstantDependencies());
        for (PatternTreeNode parentNode: subgraphParents) {
            minimalConstantPaths.addAll(parentNode.getAllMinimalConstantDependenciesOnThisPath());
        }
        return minimalConstantPaths;
    }

    public void addMinimalDependency(AttributeDependency dependency) {
        this.minimalDependencies.add(dependency);
    }

    public ArrayList<AttributeDependency> getMinimalDependencies() {
        return this.minimalDependencies;
    }

    public ArrayList<AttributeDependency> getAllMinimalDependenciesOnThisPath() {
        PatternTreeNode currPatternTreeNode = this;
        ArrayList<AttributeDependency> minimalPaths = new ArrayList<>(currPatternTreeNode.getMinimalDependencies());
        for (PatternTreeNode parentNode: subgraphParents) {
            minimalPaths.addAll(parentNode.getAllMinimalDependenciesOnThisPath());
        }
        return minimalPaths;
    }

    public PatternTreeNode getParentNode() {
        return this.parentNode;
    }

    public String getEdgeString() {
        return this.edgeString;
    }

    public List<String> getAllEdgeStrings() {
        List<String> edgeStrings = new ArrayList<>();
        edgeStrings.add(this.edgeString);
        PatternTreeNode currentNode = this;
        while (currentNode.getParentNode() != null && currentNode.getParentNode().getEdgeString() != null) {
            currentNode = currentNode.getParentNode();
            edgeStrings.add(currentNode.getEdgeString());
        }
        return edgeStrings;
    }

    public void addSubgraphParent(PatternTreeNode otherPatternNode) {
        this.subgraphParents.add(otherPatternNode);
    }

    public ArrayList<PatternTreeNode> getSubgraphParents() {
        return this.subgraphParents;
    }

    public void setCenterVertexParent(PatternTreeNode centerVertexParent) {
        this.centerVertexParent = centerVertexParent;
    }

    public PatternTreeNode getCenterVertexParent() {
        return this.centerVertexParent;
    }

    public Map<String, List<Integer>> getEntityURIs() {
        return entityURIs;
    }

    public void setEntityURIs(Map<String, List<Integer>> entityURIs) {
        System.out.println("Number of center vertex matches for this pattern: " + entityURIs.size());
        this.entityURIs = entityURIs;
    }

//    public PatternType getPatternType() {
//        return patternType;
//    }
//
//    public void setPatternType(PatternType patternType) {
//        this.patternType = patternType;
//    }
}
