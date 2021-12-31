package Infra;

import org.jgrapht.Graph;

import java.util.ArrayList;

public class PatternTreeNode {
    private final VF2PatternGraph pattern;
    private Double patternSupport = null;
    private final PatternTreeNode parentNode;
    private ArrayList<PatternTreeNode> subgraphParents = new ArrayList<>();
    private PatternTreeNode centerVertexParent = null;
    private final String edgeString;
    private boolean isPruned = false;
    private ArrayList<AttributeDependency> lowSupportDependencies = new ArrayList<>();
    private ArrayList<AttributeDependency> minimalDependencies = new ArrayList<>();
    private ArrayList<AttributeDependency> minimalConstantDependencies = new ArrayList<>();
//    private HashMap<AttributeDependency, ArrayList<TgfdDiscovery.Pair>> lowSupportGeneralTgfdList = new HashMap<>();
    private ArrayList<ArrayList<DataVertex>> listOfCenterVertices = null;

    public PatternTreeNode(VF2PatternGraph pattern, PatternTreeNode parentNode, String edgeString) {
        this.pattern = pattern;
        this.parentNode = parentNode;
        this.edgeString = edgeString;
    }

    public PatternTreeNode(VF2PatternGraph pattern) {
        this.pattern = pattern;
        this.parentNode = null;
        this.edgeString = null;
    }

    public Graph<Vertex, RelationshipEdge> getGraph() {
        return pattern.getPattern();
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

    public PatternTreeNode parentNode() {
        return this.parentNode;
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

    public void addLowSupportDependency(AttributeDependency dependency) {
        this.lowSupportDependencies.add(dependency);
    }

    public ArrayList<AttributeDependency> getLowSupportDependencies() {
        return this.lowSupportDependencies;
    }

    public ArrayList<AttributeDependency> getLowSupportDependenciesOnThisPath() {
        PatternTreeNode currPatternTreeNode = this;
        ArrayList<AttributeDependency> zeroEntityPaths = new ArrayList<>(currPatternTreeNode.getLowSupportDependencies());
        for (PatternTreeNode parentNode: subgraphParents) {
            zeroEntityPaths.addAll(parentNode.getLowSupportDependenciesOnThisPath());
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

//    public void addToLowSupportGeneralTgfdList(AttributeDependency dependencyPath, TgfdDiscovery.Pair deltaPair) {
//        this.lowSupportGeneralTgfdList.putIfAbsent(dependencyPath, new ArrayList<>());
//        this.lowSupportGeneralTgfdList.get(dependencyPath).add(deltaPair);
//    }

//    public HashMap<AttributeDependency, ArrayList<TgfdDiscovery.Pair>> getLowSupportGeneralTgfdList() {
//        return this.lowSupportGeneralTgfdList;
//    }

    public PatternTreeNode getParentNode() {
        return this.parentNode;
    }

//    public HashMap<AttributeDependency, ArrayList<TgfdDiscovery.Pair>> getAllLowSupportGeneralTgfds() {
//        HashMap<AttributeDependency, ArrayList<TgfdDiscovery.Pair>> allTGFDs = new HashMap<>(this.getLowSupportGeneralTgfdList());
//        for (PatternTreeNode parentNode: subgraphParents) {
//            for (Map.Entry<AttributeDependency, ArrayList<TgfdDiscovery.Pair>> tgfdEntry : parentNode.getAllLowSupportGeneralTgfds().entrySet()) {
//                allTGFDs.putIfAbsent(tgfdEntry.getKey(), new ArrayList<>());
//                allTGFDs.get(tgfdEntry.getKey()).addAll(tgfdEntry.getValue());
//            }
//        }
//        return allTGFDs;
//    }

    public String getEdgeString() {
        return this.edgeString;
    }

    public ArrayList<String> getAllEdgeStrings() {
        ArrayList<String> edgeStrings = new ArrayList<>();
        edgeStrings.add(this.edgeString);
        PatternTreeNode currentNode = this;
        while (currentNode.getParentNode().getEdgeString() != null) {
            currentNode = currentNode.getParentNode();
            edgeStrings.add(currentNode.getEdgeString());
        }
        return edgeStrings;
    }

    public ArrayList<ArrayList<DataVertex>> getListOfCenterVertices() {
        return this.listOfCenterVertices;
    }

    public void setListOfCenterVertices(ArrayList<ArrayList<DataVertex>> listOfCenterVertices) {
        this.listOfCenterVertices = listOfCenterVertices;
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
}
