package Infra;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Objects;

public class AttributeDependency {
    HashSet<ConstantLiteral> lhs;
    ConstantLiteral rhs;
    private Delta delta;

    public AttributeDependency(ArrayList<ConstantLiteral> dependency) {
        lhs = new HashSet<>(dependency.subList(0, dependency.size() - 1));
        rhs = dependency.get(dependency.size() - 1);
    }

    public AttributeDependency(ArrayList<ConstantLiteral> pathToRoot, ConstantLiteral literal) {
        lhs = new HashSet<>(pathToRoot);
        rhs = literal;
    }

    public AttributeDependency(HashSet<ConstantLiteral> pathToRoot, ConstantLiteral literal, Delta deltaInterval) {
        lhs = new HashSet<>(pathToRoot);
        rhs = literal;
        setDelta(deltaInterval);
    }

    public AttributeDependency() {
        lhs = new HashSet<>();
    }

    public HashSet<ConstantLiteral> getLhs() {
        return lhs;
    }

    public ConstantLiteral getRhs() {
        return rhs;
    }

    public void addToLhs(ConstantLiteral l) {
        this.lhs.add(l);
    }

    public void setRhs(ConstantLiteral rhs) {
        this.rhs = rhs;
    }

    public int size() {
        return this.lhs.size() + 1;
    }

    @Override
    public String toString() {
        return "Dependency: " + "\n\tY=" + rhs + ",\n\tX={" + lhs + "\n\t}";
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AttributeDependency that = (AttributeDependency) o;
        return lhs.equals(that.lhs) && rhs.equals(that.rhs);
    }

    @Override
    public int hashCode() {
        return Objects.hash(lhs.hashCode(), rhs.hashCode());
    }

    public Delta getDelta() {
        return delta;
    }

    public void setDelta(Delta delta) {
        this.delta = delta;
    }

    public boolean isSuperSetOfPath(ArrayList<AttributeDependency> zeroEntityDependenciesOnThisPath) {
        boolean isPruned = false;
        for (AttributeDependency prunedPath : zeroEntityDependenciesOnThisPath) {
            if (this.getRhs().equals(prunedPath.getRhs()) && this.getLhs().containsAll(prunedPath.getLhs())) {
                System.out.println("Candidate path " + this + " is a superset of pruned path " + prunedPath);
                isPruned = true;
            }
        }
        return isPruned;
    }

    public boolean isSuperSetOfPathAndSubsetOfDelta(ArrayList<AttributeDependency> minimalDependenciesOnThisPath) {
        boolean isPruned = false;
        for (AttributeDependency prunedPath : minimalDependenciesOnThisPath) {
            if (this.getRhs().equals(prunedPath.getRhs()) && this.getLhs().containsAll(prunedPath.getLhs())) {
                System.out.println("Candidate path " + this + " is a superset of pruned path " + prunedPath);
                if (this.getDelta().subsetOf(prunedPath.getDelta())) {
                    System.out.println("Candidate path delta " + this.getDelta()
                            + "\n with pruned path delta " + prunedPath.getDelta()
                            + ".");
                    isPruned = true;
                }
            }
        }
        return isPruned;
    }
}
