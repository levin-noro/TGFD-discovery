package VF2Runner;

import Infra.*;
import TgfdDiscovery.TgfdDiscovery;
import graphLoader.GraphLoader;
import org.jetbrains.annotations.NotNull;
import org.jgrapht.Graph;
import org.jgrapht.GraphMapping;
import org.jgrapht.alg.isomorphism.VF2AbstractIsomorphismInspector;

import java.util.*;
import java.util.stream.Collectors;

public class LocalizedVF2Matching {
    protected final int T;
    protected final boolean interestingOnly;
    protected Map<String, List<Integer>> entityURIs = new HashMap<>();
    protected final List<Set<Set<ConstantLiteral>>> matchesPerTimestamp;
    protected final PatternTreeNode centerVertexParent;
    protected final boolean reUseMatches;
    protected VF2PatternGraph pattern;
    Map<String, Set<String>> vertexTypesToAttributesMap;

    public LocalizedVF2Matching(VF2PatternGraph pattern, PatternTreeNode centerVertexParent, int T, boolean interestingOnly, Map<String, Set<String>> vertexTypesToAttributesMap, boolean reUseMatches) {
        this.centerVertexParent = centerVertexParent;
        this.T = T;
        matchesPerTimestamp = new ArrayList<>();
        for (int t = 0; t < this.T; t++) {
            matchesPerTimestamp.add(new HashSet<>());
        }
        this.interestingOnly = interestingOnly;
        this.vertexTypesToAttributesMap = vertexTypesToAttributesMap;
        this.reUseMatches = reUseMatches;
        this.pattern = pattern;
    }

    public void findMatches(List<GraphLoader> graphs, int T) {
        for (int t = 0; t < T; t++) {
            findMatchesInSnapshot(graphs.get(t), t);
        }
    }

    public void findMatchesInSnapshot(GraphLoader graph, int t) {
        if (pattern.getPatternType() == PatternType.SingleNode)
            findMatchesOfSingleVertex(graph, t);
        else
            findMatchesUsingEntityURIs(graph, t);
    }

    private void findMatchesOfSingleVertex(GraphLoader graph, int t) {
        String patternVertexType = pattern.getCenterVertexType();
        int numOfMatches = 0;
        Set<Set<ConstantLiteral>> matchesInThisTimestamp = new HashSet<>();
        for (Vertex v : graph.getGraph().getGraph().vertexSet()) {
            if (v.getTypes().contains(patternVertexType)) {
                numOfMatches++;
                DataVertex dataVertex = (DataVertex) v;
                findMatchesAroundThisCenterVertexUsingVF2(graph, dataVertex, t, matchesInThisTimestamp);
            }
        }
        System.out.println("Number of center vertex matches found: " + numOfMatches);
        System.out.println("Number of center vertex matches found containing active attributes: " + matchesInThisTimestamp.size());
        this.matchesPerTimestamp.get(t).addAll(matchesInThisTimestamp);
    }

    private void findMatchesUsingEntityURIs(GraphLoader graph, int t) {
        Set<Set<ConstantLiteral>> matchesSet = new HashSet<>();

        Map<String, List<Integer>> existingEntityURIs = getExistingEntityURIs(graph, t);

        int processedCenterVertices = 0;
        for (Map.Entry<String, List<Integer>> entry: existingEntityURIs.entrySet()) {
            if (entry.getValue().get(t) > 0) {
                String centerVertexUri = entry.getKey();
                DataVertex centerVertex = (DataVertex) graph.getGraph().getNode(centerVertexUri);
                Set<Set<ConstantLiteral>> matchesAroundCenterVertex = new HashSet<>();
                findMatchesAroundThisCenterVertexUsingVF2(graph, centerVertex, t, matchesAroundCenterVertex);
                matchesSet.addAll(matchesAroundCenterVertex);
                if (matchesSet.size() > 0 && matchesSet.size() % 100000 == 0)
                    System.out.println("Found "+matchesSet.size()+" matches");
            }
            processedCenterVertices++;
            if (((int)(existingEntityURIs.size() * 0.25)) > 0 && processedCenterVertices % ((int)(existingEntityURIs.size() * 0.25)) == 0)
                System.out.println("Processed " + processedCenterVertices + "/" + existingEntityURIs.size() + " center vertices");
        }

        System.out.println("Number of matches found in t="+t+" that contain active attributes: " + matchesSet.size());
        matchesPerTimestamp.get(t).addAll(matchesSet);
    }

    @NotNull
    protected Map<String, List<Integer>> getExistingEntityURIs(GraphLoader graph, int t) {
        Map<String, List<Integer>> existingEntityURIs = new HashMap<>();
        if (this.reUseMatches) {
            existingEntityURIs = this.centerVertexParent.getEntityURIs();
            System.out.println("Found center vertex parent: "+this.centerVertexParent);
            System.out.println("Parent pattern has "+existingEntityURIs.size()+" URIs for center vertex type "+ pattern.getCenterVertexType());
        } else {
            System.out.println("Finding center vertex URIs for center vertex type "+ pattern.getCenterVertexType());
            extractListOfCenterVerticesInSnapshot(pattern.getCenterVertexType(), existingEntityURIs, t, graph);
        }
        return existingEntityURIs;
    }

    public void extractListOfCenterVerticesInSnapshot(String patternVertexType, Map<String, List<Integer>> centerVertexEntityURIs, int timestamp, GraphLoader graph) {
        final long startTime = System.currentTimeMillis();
        int numOfCenterVerticesFound = 0;
        int numOfRelevantCenterVerticesFound = 0;
        Set<String> activeAttributesInCenterVertex = this.vertexTypesToAttributesMap.get(patternVertexType);
        for (Vertex vertex : graph.getGraph().getGraph().vertexSet()) {
            if (vertex.getTypes().contains(patternVertexType)) {
                numOfCenterVerticesFound++;
                DataVertex dataVertex = (DataVertex) vertex;
                if (this.interestingOnly) {
                    // Check if vertex contains at least one active attribute
                    boolean containsActiveAttributes = false;
                    for (String attrName : vertex.getAllAttributesNames()) {
                        if (activeAttributesInCenterVertex.contains(attrName)) {
                            containsActiveAttributes = true;
                            break;
                        }
                    }
                    if (!containsActiveAttributes) {
                        continue;
                    }
                }
                if (centerVertexEntityURIs != null) {
                    String entityURI = dataVertex.getVertexURI();
                    centerVertexEntityURIs.putIfAbsent(entityURI, TgfdDiscovery.createEmptyArrayListOfSize(this.T));
                    centerVertexEntityURIs.get(entityURI).set(timestamp, centerVertexEntityURIs.get(entityURI).get(timestamp) + 1);
                    numOfRelevantCenterVerticesFound++;
                }
            }
        }
        System.out.println("Number of center vertices found: " + numOfCenterVerticesFound);
        System.out.println("Number of relevant center vertices found: " + numOfRelevantCenterVerticesFound);
        TgfdDiscovery.printWithTime("Finding URIs of center vertex type", System.currentTimeMillis()-startTime);
    }

    // TODO: Does this method contain duplicate code that is common with other findMatch methods?
    private void findMatchesAroundThisCenterVertexUsingVF2(GraphLoader currentSnapshot, DataVertex dataVertex, int timestamp, Set<Set<ConstantLiteral>> matchesAroundCenterVertex) {
        Set<String> edgeLabels = this.pattern.getGraph().edgeSet().stream().map(RelationshipEdge::getLabel).collect(Collectors.toSet());
        int diameter = pattern.getRadius();
        Graph<Vertex, RelationshipEdge> subgraph = currentSnapshot.getGraph().getSubGraphWithinDiameter(dataVertex, diameter, edgeLabels, this.pattern.getGraph().edgeSet());
        VF2AbstractIsomorphismInspector<Vertex, RelationshipEdge> results = new VF2SubgraphIsomorphism().execute2(subgraph, pattern, false);

        if (results.isomorphismExists())
            extractMatches(results.getMappings(), matchesAroundCenterVertex, timestamp);
    }

    protected void extractMatches(Iterator<GraphMapping<Vertex, RelationshipEdge>> iterator, Set<Set<ConstantLiteral>> matchesAroundCenterVertex, int timestamp) {
        while (iterator.hasNext()) {
            GraphMapping<Vertex, RelationshipEdge> result = iterator.next();
            Set<ConstantLiteral> literalsInMatch = new HashSet<>();
            Map<String, Integer> interestingnessMap = new HashMap<>();
            String entityURI = extractMatch(result, literalsInMatch, interestingnessMap);
            // ensures that the match is not empty and contains more than just the uri attribute
            if (this.interestingOnly && interestingnessMap.values().stream().anyMatch(n -> n < 2))
                continue;
            else if (!this.interestingOnly && literalsInMatch.size() < this.pattern.getGraph().vertexSet().size())
                continue;

            if (entityURI != null) {
                this.entityURIs.putIfAbsent(entityURI, TgfdDiscovery.createEmptyArrayListOfSize(this.T));
                this.entityURIs.get(entityURI).set(timestamp, this.entityURIs.get(entityURI).get(timestamp)+1);
            }
            matchesAroundCenterVertex.add(literalsInMatch);
        }
    }

    // TODO: Merge with other extractMatch method?
    protected String extractMatch(GraphMapping<Vertex, RelationshipEdge> result, Set<ConstantLiteral> match, Map<String, Integer> interestingnessMap) {
        String entityURI = null;
        for (Vertex v : this.pattern.getGraph().vertexSet()) {
            Vertex currentMatchedVertex = result.getVertexCorrespondence(v, false);
            if (currentMatchedVertex == null) continue;
            String patternVertexType = v.getTypes().iterator().next();
            if (entityURI == null) {
                entityURI = extractAttributes(patternVertexType, match, currentMatchedVertex, interestingnessMap);
            } else {
                extractAttributes(patternVertexType, match, currentMatchedVertex, interestingnessMap);
            }
        }
        return entityURI;
    }

    protected String extractAttributes(String patternVertexType, Set<ConstantLiteral> match, Vertex currentMatchedVertex, Map<String, Integer> interestingnessMap) {
        String entityURI = null;
        String centerVertexType = pattern.getCenterVertexType();
        Set<String> matchedVertexTypes = currentMatchedVertex.getTypes();
        for (ConstantLiteral activeAttribute : getActiveAttributesInPattern(true)) {
            if (!matchedVertexTypes.contains(activeAttribute.getVertexType())) continue;
            for (String matchedAttrName : currentMatchedVertex.getAllAttributesNames()) {
                if (matchedVertexTypes.contains(centerVertexType) && matchedAttrName.equals("uri")) {
                    entityURI = currentMatchedVertex.getAttributeValueByName(matchedAttrName);
                }
                if (!activeAttribute.getAttrName().equals(matchedAttrName)) continue;
                String matchedAttrValue = currentMatchedVertex.getAttributeValueByName(matchedAttrName);
                ConstantLiteral xLiteral = new ConstantLiteral(patternVertexType, matchedAttrName, matchedAttrValue);
                interestingnessMap.merge(patternVertexType, 1, Integer::sum);
                match.add(xLiteral);
            }
        }
        return entityURI;
    }

    protected HashSet<ConstantLiteral> getActiveAttributesInPattern(boolean considerURI) {
        Map<String, Set<String>> patternVerticesAttributes = new HashMap<>();
        for (Vertex vertex : this.pattern.getGraph().vertexSet()) {
            for (String vertexType : vertex.getTypes()) {
                patternVerticesAttributes.put(vertexType, new HashSet<>());
                Set<String> attrNameSet = this.vertexTypesToAttributesMap.get(vertexType);
                for (String attrName : attrNameSet) {
                    patternVerticesAttributes.get(vertexType).add(attrName);
                }
            }
        }
        return getActiveAttributesForSpecifiedTypes(considerURI, patternVerticesAttributes);
    }

    @NotNull
    protected HashSet<ConstantLiteral> getActiveAttributesForSpecifiedTypes(boolean considerURI, Map<String, Set<String>> patternVerticesAttributes) {
        HashSet<ConstantLiteral> literals = new HashSet<>();
        for (String vertexType : patternVerticesAttributes.keySet()) {
            if (considerURI) literals.add(new ConstantLiteral(vertexType,"uri",null));
            for (String attrName : patternVerticesAttributes.get(vertexType)) {
                ConstantLiteral literal = new ConstantLiteral(vertexType, attrName, null);
                literals.add(literal);
            }
        }
        return literals;
    }

    public List<Set<Set<ConstantLiteral>>> getMatchesPerTimestamp() {
        return matchesPerTimestamp;
    }

    public Map<String, List<Integer>> getEntityURIs() {
        return this.entityURIs;
    }

    public void printEntityURIs() {
        for (Map.Entry<String, List<Integer>> entry: entityURIs.entrySet()) {
            System.out.println(entry);
        }
    }
}
