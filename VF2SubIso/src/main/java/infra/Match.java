package infra;

import org.jgrapht.Graph;
import org.jgrapht.GraphMapping;

import java.util.ArrayList;
import java.util.List;

/**
 * Represents a match.
 * @note We do not need the edges of a match
 */
public final class Match {
    //region --[Fields: Private]---------------------------------------
    // Intervals where the match exists.
    private List<Interval> intervals;

    // Graph mapping from pattern graph to match graph.
    private GraphMapping<Vertex, RelationshipEdge> mapping;

    // Pattern graph.
    private VF2PatternGraph pattern;
    //endregion

    //region --[Constructors]------------------------------------------
    /**
     * Creates a new Match.
     */
    public Match() {
        // TODO: add argument for X to be used in getSignature [2021-02-12]
    }
    //endregion

    //region --[Methods: Public]---------------------------------------
    /**
     * Gets the signature of a match for comparison across time w.r.t. the x of the dependency.
     * @param pattern Pattern of the match.
     * @param mapping Mapping of the match.
     * @param xLiterals Literals of the X dependency.
     */
    public static String signatureFromX(
        VF2PatternGraph pattern,
        GraphMapping<Vertex, RelationshipEdge> mapping,
        ArrayList<Literal> xLiterals)
    {
        var builder = new StringBuilder();
        for (var patternVertex : pattern.getGraph().vertexSet())
        {
            var matchVertex = mapping.getVertexCorrespondence(patternVertex, false);
            if (matchVertex == null)
                continue;

            for (Literal literal : xLiterals)
            {
                if (literal instanceof ConstantLiteral)
                {
                    var constantLiteral = (ConstantLiteral)literal;
                    if (!matchVertex.getTypes().contains(constantLiteral.getVertexType()))
                        continue;

                    //if (matchVertex.attContains())
                    //{
                    //    //if(matchVertex.attContains())
                    //}
                }
                else if (literal instanceof VariableLiteral)
                {
                }
            }
        }
        // TODO: consider returning a hash [2021-02-13]
        return builder.toString();

        //var builder = new StringBuilder();
        //getVertices()
        //    .stream()
        //    .sorted() // Ensure stable sorting of vertices
        //    .forEach(vertex -> {
        //        vertex
        //            .getAllAttributesList()
        //            .stream()
        //            .sorted() // Ensure stable sorting of attributes
        //            .forEach(attr -> {
        //                // TODO: filter for only attributes of X [2021-02-12]
        //                builder.append(attr.getAttrValue());
        //                builder.append(",");
        //            });
        //    });
    }

    /**
     * Gets the signature of a match for comparison across time w.r.t. the dependency.
     * @param pattern Pattern of the match.
     * @param mapping Mapping of the match.
     * @param dependency TGFD dependency.
     */
    public static String signatureFromDependency(
        VF2PatternGraph pattern,
        GraphMapping<Vertex, RelationshipEdge> mapping,
        Dependency dependency)
    {
        throw new UnsupportedOperationException("not implemented");
    }
    //endregion

    //region --[Properties: Public]------------------------------------
    /**
     * Gets the intervals of the match.
     */
    public List<Interval> getIntervals() {
        return this.intervals;
    }

    /**
     * Gets the vertices of the match.
     */
    public GraphMapping<Vertex, RelationshipEdge> getMapping() {
        return this.mapping;
    }

    /**
     * Gets the pattern graph.
     */
    public VF2PatternGraph getPattern() { return pattern; }

    /**
     * Gets the vertices of the match that are valid for the corresponding intervals.
     */
    public List<DataVertex> getVertices() {
        // TODO: remove if not needed (if TGFD ond Signature just uses pattern + mapping) [2021-02-13]
        throw new UnsupportedOperationException("not implemented");
    }
    //endregion
}