package Infra;

import TgfdDiscovery.TgfdDiscovery;
import org.jgrapht.Graph;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

public class PatternTreeNode {
    private final VF2PatternGraph pattern;
    private Double patternSupport = null;
    private final PatternTreeNode parentNode;
    private ArrayList<PatternTreeNode> subgraphParents = new ArrayList<>();
    private PatternTreeNode centerVertexParent = null;
    private final String edgeString;
    private boolean isPruned = false;
    private ArrayList<AttributeDependency> zeroEntityDependencies = new ArrayList<>();
    private ArrayList<AttributeDependency> minimalDependencies = new ArrayList<>();
    private HashMap<AttributeDependency, ArrayList<TgfdDiscovery.Pair>> lowSupportGeneralTgfdList = new HashMap<>();
    private ArrayList<ArrayList<DataVertex>> matchesOfCenterVertices = null;

    public PatternTreeNode(VF2PatternGraph pattern, PatternTreeNode parentNode, String edgeString) {
        this.pattern = pattern;
        this.parentNode = parentNode;
        this.edgeString = edgeString;
    }

    public PatternTreeNode(VF2PatternGraph pattern, double patternSupport) {
        this.pattern = pattern;
        this.patternSupport = patternSupport;
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

    public double getPatternSupport() {
        return this.patternSupport;
    }

    public PatternTreeNode parentNode() {
        return this.parentNode;
    }

    public void setIsPruned() {
        this.isPruned = true;
    }

    public boolean isPruned() {
        return this.isPruned;
    }

    @Override
    public String toString() {
        return "PatternTreeNode{" +
                "pattern=" + pattern +
                ",\n support=" + patternSupport +
                '}';
    }

    public void addZeroEntityDependency(AttributeDependency dependency) {
        this.zeroEntityDependencies.add(dependency);
    }

    public ArrayList<AttributeDependency> getZeroEntityDependencies() {
        return this.zeroEntityDependencies;
    }

    public ArrayList<AttributeDependency> getAllZeroEntityDependenciesOnThisPath() {
        PatternTreeNode currPatternTreeNode = this;
        ArrayList<AttributeDependency> zeroEntityPaths = new ArrayList<>(currPatternTreeNode.getZeroEntityDependencies());
        for (PatternTreeNode parentNode: subgraphParents) {
            zeroEntityPaths.addAll(parentNode.getAllZeroEntityDependenciesOnThisPath());
        }
        return zeroEntityPaths;
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

    public void addToLowSupportGeneralTgfdList(AttributeDependency dependencyPath, TgfdDiscovery.Pair deltaPair) {
        this.lowSupportGeneralTgfdList.putIfAbsent(dependencyPath, new ArrayList<>());
        this.lowSupportGeneralTgfdList.get(dependencyPath).add(deltaPair);
    }

    public HashMap<AttributeDependency, ArrayList<TgfdDiscovery.Pair>> getLowSupportGeneralTgfdList() {
        return this.lowSupportGeneralTgfdList;
    }

    public PatternTreeNode getParentNode() {
        return this.parentNode;
    }

    public HashMap<AttributeDependency, ArrayList<TgfdDiscovery.Pair>> getAllLowSupportGeneralTgfds() {
        HashMap<AttributeDependency, ArrayList<TgfdDiscovery.Pair>> allTGFDs = new HashMap<>(this.getLowSupportGeneralTgfdList());
        for (PatternTreeNode parentNode: subgraphParents) {
            for (Map.Entry<AttributeDependency, ArrayList<TgfdDiscovery.Pair>> tgfdEntry : parentNode.getAllLowSupportGeneralTgfds().entrySet()) {
                allTGFDs.putIfAbsent(tgfdEntry.getKey(), new ArrayList<>());
                allTGFDs.get(tgfdEntry.getKey()).addAll(tgfdEntry.getValue());
            }
        }
        return allTGFDs;
    }

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

    public ArrayList<ArrayList<DataVertex>> getMatchesOfCenterVertices() {
        return this.matchesOfCenterVertices;
    }

    public void setMatchesOfCenterVertices(ArrayList<ArrayList<DataVertex>> matchesOfCenterVertices) {
        this.matchesOfCenterVertices = matchesOfCenterVertices;
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
