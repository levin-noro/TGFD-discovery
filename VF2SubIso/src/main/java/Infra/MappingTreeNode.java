package Infra;

import java.util.HashSet;
import java.util.Set;

public class MappingTreeNode {
    private final String patternVertexType;
    private boolean isPruned = false;
    private final MappingTreeNode parent;
    private final DataVertex dataVertex;

    public MappingTreeNode(DataVertex dataVertex, String patternVertexType, MappingTreeNode parent) {
        this.dataVertex = dataVertex;
        this.patternVertexType = patternVertexType;
        this.parent = parent;
    }

    public Set<MappingTreeNode> getPathToRoot() {
        Set<MappingTreeNode> dataVertexSet = new HashSet<>();
        dataVertexSet.add(this);
        MappingTreeNode parentMappingNode = parent;
        while (parentMappingNode != null) {
            dataVertexSet.add(parentMappingNode);
            parentMappingNode = parentMappingNode.getParent();
        }
        return dataVertexSet;
    }

    public DataVertex getDataVertex() {
        return this.dataVertex;
    }

    public MappingTreeNode getParent() {
        return this.parent;
    }

    public String getPatternVertexType() {
        return patternVertexType;
    }

    public boolean isPruned() {
        return isPruned;
    }

    public void setPruned(boolean pruned) {
        isPruned = pruned;
    }
}
