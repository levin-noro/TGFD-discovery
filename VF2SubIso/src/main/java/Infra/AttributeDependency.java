package Infra;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;

public class AttributeDependency {
    HashSet<ConstantLiteral> lhs;
    ConstantLiteral rhs;
    private Delta delta;

    public AttributeDependency(List<ConstantLiteral> dependency) {
        lhs = new HashSet<>(dependency.subList(0, dependency.size() - 1));
        rhs = dependency.get(dependency.size() - 1);
    }

    public AttributeDependency(List<ConstantLiteral> pathToRoot, ConstantLiteral literal) {
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

    public Delta getDelta() {
        return delta;
    }

    public void setDelta(Delta delta) {
        this.delta = delta;
    }

    public boolean isSuperSetOfPath(ArrayList<AttributeDependency> prunedDependencies) {
        for (AttributeDependency prunedPath : prunedDependencies) {
            if (this.getRhs().equals(prunedPath.getRhs()) && this.getLhs().containsAll(prunedPath.getLhs())) {
                System.out.println("Candidate path " + this + " is a superset of pruned path " + prunedPath);
                return true;
            }
        }
        return false;
    }

    public boolean isSuperSetOfPathAndSubsetOfDelta(ArrayList<AttributeDependency> minimalDependenciesOnThisPath) {
        for (AttributeDependency prunedPath : minimalDependenciesOnThisPath) {
            if (this.getRhs().equals(prunedPath.getRhs()) && this.getLhs().containsAll(prunedPath.getLhs())) {
                System.out.println("Candidate path " + this + " is a superset of pruned path " + prunedPath);
                if (this.getDelta().subsetOf(prunedPath.getDelta())) {
                    System.out.println("Candidate path delta " + this.getDelta()
                            + "\n with pruned path delta " + prunedPath.getDelta()
                            + ".");
                    return true;
                }
            }
        }
        return false;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AttributeDependency that = (AttributeDependency) o;
        return lhs.equals(that.lhs) && rhs.equals(that.rhs) && Objects.equals(delta, that.delta); // Objects.equals ensure delta is not null
    }

    @Override
    public int hashCode() {
        return Objects.hash(lhs, rhs, delta);
    }
}
