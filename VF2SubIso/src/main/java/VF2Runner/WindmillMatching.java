package VF2Runner;

import Infra.*;
import TgfdDiscovery.TgfdDiscovery;
import graphLoader.GraphLoader;

import java.util.*;

public class WindmillMatching extends FastMatching {


    private final VF2PatternGraph windMillBlades;
    private final VF2PatternGraph windMillStem;
    private final VF2PatternGraph originalPattern;
    private String stemConnectorType;

    public WindmillMatching(VF2PatternGraph pattern, PatternTreeNode centerVertexParent, int T, boolean interestingOnly, Map<String, Set<String>> vertexTypesToAttributesMap, boolean reUseMatches) {
        super(pattern, centerVertexParent, T, interestingOnly, vertexTypesToAttributesMap, reUseMatches);
        originalPattern = pattern;
        windMillBlades = pattern.getWindMillBlades();
        windMillStem = pattern.getWindMillStem();
        stemConnectorType = pattern.getWindmillStemStartVertex().getTypes().iterator().next();
    }

    public void findMatches(List<GraphLoader> graphs, int T) {
        for (int t = 0; t < T; t++) {
            findMatchesInSnapshot(graphs.get(t), t);
        }
    }

    public void findMatchesInSnapshot(GraphLoader graph, int t) {
        findMatchesUsingEntityURIs(graph, t);
    }

    private void findMatchesUsingEntityURIs(GraphLoader graph, int t) {

        Map<String, List<Integer>> existingEntityURIs = new HashMap<>();
        System.out.println("Finding URIs of windmill blades center vertex type "+ windMillBlades.getCenterVertexType());
        extractListOfCenterVerticesInSnapshot(windMillBlades.getCenterVertexType(), existingEntityURIs, t, graph);

        this.pattern = windMillBlades;
        Set<Set<ConstantLiteral>> bladesSet = new HashSet<>();
        int processedCenterVertices = 0;
        for (Map.Entry<String, List<Integer>> entry: existingEntityURIs.entrySet()) {
            if (entry.getValue().get(t) > 0) {
                String centerVertexUri = entry.getKey();
                DataVertex centerVertex = (DataVertex) graph.getGraph().getNode(centerVertexUri);
                Set<Set<ConstantLiteral>> matchesOfBladesAroundCenterVertex = new HashSet<>();
                findAllMatchesOfStarPatternInSnapshotUsingCenterVertex(graph, centerVertex, t, matchesOfBladesAroundCenterVertex);
                bladesSet.addAll(matchesOfBladesAroundCenterVertex);
                if (bladesSet.size() > 0 && bladesSet.size() % 100000 == 0)
                    System.out.println("Found "+bladesSet.size()+" matches");
            }
            processedCenterVertices++;
            if (((int)(existingEntityURIs.size() * 0.25)) > 0 && processedCenterVertices % ((int)(existingEntityURIs.size() * 0.25)) == 0)
                System.out.println("Processed " + processedCenterVertices + "/" + existingEntityURIs.size() + " center vertices");
        }

        this.pattern = windMillStem;
        Set<Set<ConstantLiteral>> completeMatches = new HashSet<>();
        Map<String,Set<Set<ConstantLiteral>>> matchesOfStemConnector = new HashMap<>();
        System.out.println("For all windmill blades matches, finding stems that connect to "+stemConnectorType);
        int processedWindmillBlades = 0;
        for (Set<ConstantLiteral> bladesMatch: bladesSet) {
            for (ConstantLiteral constantLiteral: bladesMatch) {
                if (constantLiteral.getAttrName().equals("uri") && constantLiteral.getVertexType().equals(stemConnectorType)) {
                    String centerVertexUri = constantLiteral.getAttrValue();
                    if (!matchesOfStemConnector.containsKey(centerVertexUri)) {
                        DataVertex centerVertex = (DataVertex) graph.getGraph().getNode(centerVertexUri);
                        Set<Set<ConstantLiteral>> matchesAroundCenterVertex = new HashSet<>();
                        findAllMatchesOfK1patternInSnapshotUsingCenterVertex(graph, centerVertex, t, matchesAroundCenterVertex);
                        matchesOfStemConnector.put(centerVertexUri, matchesAroundCenterVertex);
                    }
                    for (Set<ConstantLiteral> stemMatch: matchesOfStemConnector.get(centerVertexUri)) {
                        Set<ConstantLiteral> completeMatch = new HashSet<>();
                        completeMatch.addAll(bladesMatch);
                        completeMatch.addAll(stemMatch);
                        completeMatches.add(completeMatch);
                    }
                }
            }
            processedWindmillBlades++;
            if (((int)(bladesSet.size() * 0.25)) > 0 && processedWindmillBlades % ((int)(bladesSet.size() * 0.25)) == 0)
                System.out.println("Processed " + processedWindmillBlades + "/" + bladesSet.size() + " windmill blades matches");
        }

        System.out.println("Number of matches found in t="+t+" that contain active attributes: " + completeMatches.size());

        this.entityURIs = new HashMap<>();
        for (Set<ConstantLiteral> completeMatch: completeMatches) {
            for (ConstantLiteral constantLiteral: completeMatch) {
                if (constantLiteral.getAttrName().equals("uri") && constantLiteral.getVertexType().equals(this.originalPattern.getCenterVertexType())) {
                    String entityURI = constantLiteral.getAttrValue();
                    this.entityURIs.putIfAbsent(entityURI, TgfdDiscovery.createEmptyArrayListOfSize(this.T));
                    this.entityURIs.get(entityURI).set(t, this.entityURIs.get(entityURI).get(t) + 1);
                }
            }
        }

        matchesPerTimestamp.get(t).addAll(completeMatches);
    }
}
