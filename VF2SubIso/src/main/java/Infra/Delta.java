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

    // TODO: Which unit of time should we use?
    public boolean subsetOf(Delta delta) {
        return this.getMin().getYears() >= delta.getMin().getYears() && this.getMax().getYears() <= delta.getMax().getYears();
    }

    // TODO: Which unit of time should we use?
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Delta delta = (Delta) o;
        return min.getYears() == delta.min.getYears() && max.getYears() == delta.max.getYears() && granularity.toDays() == delta.granularity.toDays();
    }

    @Override
    public int hashCode() {
        return Objects.hash(min, max, granularity);
    }

    public int getIntervalWidth() {
        return this.getMax().getYears();
    }
}
