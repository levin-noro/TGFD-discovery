package Infra;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Objects;

public class AttributeDependency {
    HashSet<ConstantLiteral> lhs;
    ConstantLiteral rhs;

    public AttributeDependency(ArrayList<ConstantLiteral> dependency) {
        lhs = new HashSet<>(dependency.subList(0, dependency.size() - 1));
        rhs = dependency.get(dependency.size() - 1);
    }

    public AttributeDependency(ArrayList<ConstantLiteral> pathToRoot, ConstantLiteral literal) {
        lhs = new HashSet<>(pathToRoot);
        rhs = literal;
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
}
