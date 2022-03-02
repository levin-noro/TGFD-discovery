package Infra;

import java.util.ArrayList;
import java.util.List;

public class PatternTree {
    public ArrayList<ArrayList<PatternTreeNode>> tree;

    public PatternTree() {
        setTree(new ArrayList<>());
    }

    public void addLevel() {
        getTree().add(new ArrayList<>());
        System.out.println("GenerationTree levels: " + getTree().size());
    }

    public PatternTreeNode createNodeAtLevel(int level, VF2PatternGraph pattern, PatternTreeNode parentNode, String candidateEdgeString, boolean considerAlternativeParents) {
        PatternTreeNode node = new PatternTreeNode(pattern, parentNode, candidateEdgeString);
        getTree().get(level).add(node);
        findSubgraphParents(level-1, node);
        findCenterVertexParent(level-1, node, considerAlternativeParents); // TO-DO: Why do we only need one parent?
        return node;
    }

    public PatternTreeNode createNodeAtLevel(int level, VF2PatternGraph pattern) {
        PatternTreeNode node = new PatternTreeNode(pattern);
        getTree().get(level).add(node);
        return node;
    }

    public void findSubgraphParents(int level, PatternTreeNode node) {
        if (level < 0) return;
        System.out.println("Finding subgraph parents...");
        if (level == 0) {
            System.out.println("Finding subgraph parents...");
            ArrayList<String> newPatternVertices = new ArrayList<>();
            node.getGraph().vertexSet().forEach((vertex) -> {newPatternVertices.add(vertex.getTypes().iterator().next());});
            for (PatternTreeNode otherPatternNode : this.getTree().get(level)) {
                if (newPatternVertices.containsAll(otherPatternNode.getGraph().vertexSet().iterator().next().getTypes())) {
                    System.out.println("New pattern: " + node.getPattern());
                    System.out.println("is a child of subgraph  parent pattern: " + otherPatternNode.getGraph().vertexSet());
                    node.addSubgraphParent(otherPatternNode);
                }
            }
            return;
        }
        ArrayList<String> newPatternEdges = new ArrayList<>();
        node.getGraph().edgeSet().forEach((edge) -> {newPatternEdges.add(edge.toString());});
        for (PatternTreeNode otherPatternNode : this.getTree().get(level)) {
            ArrayList<String> otherPatternEdges = new ArrayList<>();
            otherPatternNode.getGraph().edgeSet().forEach((edge) -> {otherPatternEdges.add(edge.toString());});
            if (newPatternEdges.containsAll(otherPatternEdges)) { // TO-DO: Should we also check for center vertex equality here?
                System.out.println("New pattern: " + node.getPattern());
                if (otherPatternNode.getGraph().edgeSet().size() == 0) {
                    System.out.println("is a child of subgraph parent pattern: " + otherPatternNode.getGraph().vertexSet());
                } else {
                    System.out.println("is a child of subgraph parent pattern: " + otherPatternNode.getPattern());
                }
                node.addSubgraphParent(otherPatternNode);
            }
        }
    }

    public void findCenterVertexParent(int level, PatternTreeNode node, boolean considerAlternativeParents) {
        if (level < 0) return;
        System.out.println("Finding center vertex parent...");
        ArrayList<String> newPatternEdges = new ArrayList<>();
        node.getGraph().edgeSet().forEach((edge) -> {newPatternEdges.add(edge.toString());});
        List<PatternTreeNode> almostParents = new ArrayList<>();
        for (PatternTreeNode otherPatternNode : this.getTree().get(level)) {
            ArrayList<String> otherPatternEdges = new ArrayList<>();
            otherPatternNode.getGraph().edgeSet().forEach((edge) -> {otherPatternEdges.add(edge.toString());});
            if (newPatternEdges.containsAll(otherPatternEdges)) {
                almostParents.add(otherPatternNode);
                if (otherPatternNode.getPattern().getCenterVertexType().equals(node.getPattern().getCenterVertexType())) {
                    System.out.println("New pattern: " + node.getPattern());
                    if (otherPatternNode.getGraph().edgeSet().size() == 0) {
                        System.out.println("is a child of center vertex parent pattern: " + otherPatternNode.getGraph().vertexSet());
                    } else {
                        System.out.println("is a child of center vertex parent pattern: " + otherPatternNode.getPattern());
                    }
                    node.setCenterVertexParent(otherPatternNode);
                    return;
                }
            }
        }
        if (node.getCenterVertexParent() == null && considerAlternativeParents) {
            for (PatternTreeNode almostParent: almostParents) {
                for (Vertex v: node.getGraph().vertexSet()) {
                    Vertex almostParentCenterVertex = almostParent.getPattern().getCenterVertex();
                    if (v.getTypes().containsAll(almostParentCenterVertex.getTypes())) {
                        if (node.getPattern().calculateRadiusForGivenVertex(v) == node.getPattern().getRadius()) {
                            node.getPattern().setCenterVertex(almostParentCenterVertex);
                            System.out.println("New pattern: " + node.getPattern());
                            if (almostParent.getGraph().edgeSet().size() == 0) {
                                System.out.println("is a child of center vertex parent pattern: " + almostParent.getGraph().vertexSet());
                            } else {
                                System.out.println("is a child of center vertex parent pattern: " + almostParent.getPattern());
                            }
                            node.setCenterVertexParent(almostParent);
                            return;
                        }
                    }
                }
            }
        }
        if (node.getCenterVertexParent() == null) {
            for (PatternTreeNode otherPatternNode : this.getTree().get(0)) {
                if (otherPatternNode.getPattern().getCenterVertexType().equals(node.getPattern().getCenterVertexType())) {
                    System.out.println("New pattern: " + node.getPattern());
                    if (otherPatternNode.getGraph().edgeSet().size() == 0) {
                        System.out.println("is a child of center vertex parent pattern: " + otherPatternNode.getGraph().vertexSet());
                    }
                    node.setCenterVertexParent(otherPatternNode);
                    return;
                }
            }
        }
    }

    public ArrayList<PatternTreeNode> getLevel(int i) {
        return this.getTree().get(i);
    }

    public ArrayList<ArrayList<PatternTreeNode>> getTree() {
        return tree;
    }

    public void setTree(ArrayList<ArrayList<PatternTreeNode>> tree) {
        this.tree = tree;
    }
}
