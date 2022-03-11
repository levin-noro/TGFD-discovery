package Infra;

import java.time.Duration;
import java.time.Period;
import java.util.Objects;

public class Delta {

    private final Period min;
    private final Period max;
    private final Duration granularity;

    public Delta(Period min, Period max, Duration granularity)
    {
        this.min=min;
        this.max=max;
        this.granularity=granularity;
    }

    public Period getMax() {
        return max;
    }

    public Period getMin() {
        return min;
    }

    public Duration getGranularity() {
        return granularity;
    }


    @Override
    public String toString() {
        return "Delta{" +
                "min=" + min +
                ", max=" + max +
                ", granularity=" + granularity +
                '}';
    }

    public boolean subsetOf(Delta delta) {
        return this.getMin().getDays() >= delta.getMin().getDays() && this.getMax().getDays() <= delta.getMax().getDays();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Delta delta = (Delta) o;
        return min.getDays() == delta.min.getDays() && max.getDays() == delta.max.getDays() && granularity.toDays() == delta.granularity.toDays();
    }

    @Override
    public int hashCode() {
        return Objects.hash(min, max, granularity);
    }
}
