package Infra;

import java.util.ArrayList;

public class LiteralTree {
    private final ArrayList<ArrayList<LiteralTreeNode>> tree;

    public LiteralTree() {
        this.tree = new ArrayList<>();
    }

    public void addLevel() {
        this.tree.add(new ArrayList<>());
        System.out.println("TgfdDiscovery.LiteralTree levels: " + this.tree.size());
    }

    public LiteralTreeNode createNodeAtLevel(int level, ConstantLiteral literal, LiteralTreeNode parentNode) {
        LiteralTreeNode node = new LiteralTreeNode(literal, parentNode);
        this.tree.get(level).add(node);
        return node;
    }

    public ArrayList<LiteralTreeNode> getLevel(int i) {
        return this.tree.get(i);
    }
}
