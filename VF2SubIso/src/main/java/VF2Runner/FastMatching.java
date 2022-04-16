package VF2Runner;

import Infra.*;
import TgfdDiscovery.TgfdDiscovery;
import graphLoader.GraphLoader;
import org.jetbrains.annotations.NotNull;
import org.jgrapht.Graph;
import org.jgrapht.alg.isomorphism.VF2AbstractIsomorphismInspector;

import java.util.*;
import java.util.stream.Collectors;

public class FastMatching extends LocalizedVF2Matching {

    public FastMatching(VF2PatternGraph pattern, PatternTreeNode centerVertexParent, int T, boolean interestingOnly, Map<String, Set<String>> vertexTypesToAttributesMap, boolean reUseMatches) {
        super(pattern, centerVertexParent, T, interestingOnly, vertexTypesToAttributesMap, reUseMatches);
        System.out.println("Pattern Type: "+this.pattern.getPatternType().name());
    }

    public void findMatches(List<GraphLoader> graphs, int T) {
        for (int t = 0; t < T; t++)
            findMatchesInSnapshot(graphs.get(t), t);
    }

    public void findMatchesInSnapshot(GraphLoader graph, int t) {
        if (this.pattern.getPatternType() == PatternType.SingleNode)
            findMatchesOfSingleVertex(graph, t);
        else
            findMatchesUsingEntityURIs(graph, t);
    }

    private void findMatchesOfSingleVertex(GraphLoader graph, int t) {
        String patternVertexType = this.pattern.getCenterVertexType();
        Set<Set<ConstantLiteral>> matchesInThisTimestamp = new HashSet<>();
        findAllMatchesOfK0patternInSnapshotUsingCenterVertices(graph, t, patternVertexType, matchesInThisTimestamp);
        System.out.println("Number of center vertex matches found containing active attributes: " + matchesInThisTimestamp.size());
        this.matchesPerTimestamp.get(t).addAll(matchesInThisTimestamp);
    }

    private void findAllMatchesOfK0patternInSnapshotUsingCenterVertices(GraphLoader graph, int t, String patternVertexType, Set<Set<ConstantLiteral>> matchesInThisTimestamp) {
        int numOfMatches = 0;
        for (Vertex v : graph.getGraph().getGraph().vertexSet()) {
            if (v.getTypes().contains(patternVertexType)) {
                numOfMatches++;
                DataVertex dataVertex = (DataVertex) v;
                Set<ConstantLiteral> match = new HashSet<>();
                Map<String, Integer> interestingnessMap = new HashMap<>();
                String entityURI = extractAttributes(patternVertexType, match, dataVertex, interestingnessMap);
                if (this.interestingOnly && interestingnessMap.values().stream().anyMatch(n -> n < 2)) {
                    continue;
                } else if (!this.interestingOnly && match.size() < this.pattern.getGraph().vertexSet().size()) {
                    continue;
                }
                if (entityURI != null) {
                    this.entityURIs.putIfAbsent(entityURI, TgfdDiscovery.createEmptyArrayListOfSize(this.T));
                    this.entityURIs.get(entityURI).set(t, this.entityURIs.get(entityURI).get(t) + 1);
                }
                matchesInThisTimestamp.add(match);
            }
        }
        System.out.println("Number of center vertex matches found: " + numOfMatches);
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
                findMatchesAroundThisCenterVertex(graph, t, centerVertex, matchesAroundCenterVertex);
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

    private void findMatchesAroundThisCenterVertex(GraphLoader currentSnapshot, int year, DataVertex dataVertex, Set<Set<ConstantLiteral>> matchesAroundCenterVertex) {
        PatternType patternType = this.pattern.getPatternType();
        switch (patternType) {
            case SingleNode -> throw new IllegalArgumentException("this.getCurrentVSpawnLevel() < 1");
            case SingleEdge -> findAllMatchesOfK1patternInSnapshotUsingCenterVertex(currentSnapshot, dataVertex, year, matchesAroundCenterVertex);
            case DoubleEdge -> findAllMatchesOfK2PatternInSnapshotUsingCenterVertex(currentSnapshot, dataVertex, year, matchesAroundCenterVertex);
            case Star -> findAllMatchesOfStarPatternInSnapshotUsingCenterVertex(currentSnapshot, dataVertex, year, matchesAroundCenterVertex);
            case Line -> findAllMatchesOfLinePatternInSnapshotUsingCenterVertex(currentSnapshot, dataVertex, year, matchesAroundCenterVertex, false);
            case Circle -> findAllMatchesOfLinePatternInSnapshotUsingCenterVertex(currentSnapshot, dataVertex, year, matchesAroundCenterVertex, true);
            case Complex -> findAllMatchesOfPatternInThisSnapshotUsingCenterVertex(currentSnapshot, dataVertex, year, matchesAroundCenterVertex);
            default -> throw new IllegalArgumentException("Unrecognized pattern type");
        }
    }

    // TODO: Does this method contain duplicate code that is common with other findMatch methods?
    private void findAllMatchesOfPatternInThisSnapshotUsingCenterVertex(GraphLoader currentSnapshot, DataVertex dataVertex, int timestamp, Set<Set<ConstantLiteral>> matchesAroundCenterVertex) {
        Set<String> edgeLabels = this.pattern.getGraph().edgeSet().stream().map(RelationshipEdge::getLabel).collect(Collectors.toSet());
        int diameter = this.pattern.getRadius();
        Graph<Vertex, RelationshipEdge> subgraph = currentSnapshot.getGraph().getSubGraphWithinDiameter(dataVertex, diameter, edgeLabels, this.pattern.getGraph().edgeSet());
        VF2AbstractIsomorphismInspector<Vertex, RelationshipEdge> results = new VF2SubgraphIsomorphism().execute2(subgraph, this.pattern, false);

        if (results.isomorphismExists())
            extractMatches(results.getMappings(), matchesAroundCenterVertex, timestamp);
    }

    // TODO: Does this method contain duplicate code that is common with other findMatch methods?
    private void findAllMatchesOfLinePatternInSnapshotUsingCenterVertex(GraphLoader currentSnapshot, DataVertex startDataVertex, int year, Set<Set<ConstantLiteral>> matchesAroundCenterVertex, boolean isCyclic) {

        Vertex startVertex = this.pattern.getCenterVertex();
        String startVertexType = startVertex.getTypes().iterator().next();

        Vertex currentPatternVertex = startVertex;
        Set<RelationshipEdge> visited = new HashSet<>();
        List<RelationshipEdge> patternEdgePath = new ArrayList<>();
        System.out.println("Pattern edge path:");
        while (visited.size() < this.pattern.getGraph().edgeSet().size()) {
            for (RelationshipEdge patternEdge : this.pattern.getGraph().edgesOf(currentPatternVertex)) {
                if (!visited.contains(patternEdge)) {
                    boolean outgoing = patternEdge.getSource().equals(currentPatternVertex);
                    currentPatternVertex = outgoing ? patternEdge.getTarget() : patternEdge.getSource();
                    patternEdgePath.add(patternEdge);
                    System.out.println(patternEdge);
                    visited.add(patternEdge);
                    if (isCyclic)
                        break;
                }
            }
        }

        MappingTree mappingTree = new MappingTree();
        mappingTree.addLevel();
        System.out.println("Added level to MappingTree. MappingTree levels: " + mappingTree.getTree().size());
        MappingTreeNode rootMappingTreeNode = new MappingTreeNode(startDataVertex, startVertexType, null);
        mappingTree.createNodeAtLevel(0, rootMappingTreeNode);
        System.out.println("Added node to MappingTree level "+mappingTree.getTree().size()+".");
        System.out.println("MappingTree level "+mappingTree.getTree().size()+" size = "+mappingTree.getTree().get(0).size());

        for (int index = 0; index < this.pattern.getGraph().edgeSet().size(); index++) {

            String currentPatternEdgeLabel = patternEdgePath.get(index).getLabel();
            PatternVertex currentPatternSourceVertex = (PatternVertex) patternEdgePath.get(index).getSource();
            String currentPatternSourceVertexLabel = currentPatternSourceVertex.getTypes().iterator().next();
            PatternVertex currentPatternTargetVertex = (PatternVertex) patternEdgePath.get(index).getTarget();
            String currentPatternTargetVertexLabel = currentPatternTargetVertex.getTypes().iterator().next();

            if (mappingTree.getLevel(index).size() == 0) {
                break;
            } else {
                mappingTree.addLevel();
            }
            for (MappingTreeNode currentMappingTreeNode: mappingTree.getLevel(index)) {

                DataVertex currentDataVertex = currentMappingTreeNode.getDataVertex();

                for (RelationshipEdge dataEdge : currentSnapshot.getGraph().getGraph().outgoingEdgesOf(currentDataVertex)) {
                    if (dataEdge.getLabel().equals(currentPatternEdgeLabel)
                            && dataEdge.getSource().getTypes().contains(currentPatternSourceVertexLabel)
                            && dataEdge.getTarget().getTypes().contains(currentPatternTargetVertexLabel)) {
                        DataVertex otherVertex = (DataVertex) dataEdge.getTarget();
                        MappingTreeNode newMappingTreeNode = new MappingTreeNode(otherVertex, currentPatternTargetVertexLabel, currentMappingTreeNode);
                        if (isCyclic && index == this.pattern.getGraph().edgeSet().size()-1) {
                            if (!newMappingTreeNode.getDataVertex().getVertexURI().equals(startDataVertex.getVertexURI())) {
                                newMappingTreeNode.setPruned(true);
                            }
                        }
                        mappingTree.createNodeAtLevel(index+1, newMappingTreeNode);
                        System.out.println("Added node to MappingTree level "+mappingTree.getTree().size()+".");
                        System.out.println("MappingTree level "+mappingTree.getTree().size()+" size = "+mappingTree.getTree().get(index+1).size());
                    }
                }
                for (RelationshipEdge dataEdge : currentSnapshot.getGraph().getGraph().incomingEdgesOf(currentDataVertex)) {
                    if (dataEdge.getLabel().equals(currentPatternEdgeLabel)
                            && dataEdge.getSource().getTypes().contains(currentPatternSourceVertexLabel)
                            && dataEdge.getTarget().getTypes().contains(currentPatternTargetVertexLabel)) {
                        DataVertex otherVertex = (DataVertex) dataEdge.getSource();
                        MappingTreeNode newMappingTreeNode = new MappingTreeNode(otherVertex, currentPatternSourceVertexLabel, currentMappingTreeNode);
                        if (isCyclic && index == this.pattern.getGraph().edgeSet().size()-1) {
                            if (!newMappingTreeNode.getDataVertex().getVertexURI().equals(startDataVertex.getVertexURI())) {
                                newMappingTreeNode.setPruned(true);
                            }
                        }
                        mappingTree.createNodeAtLevel(index+1, newMappingTreeNode);
                        System.out.println("Added node to MappingTree level "+mappingTree.getTree().size()+".");
                        System.out.println("MappingTree level "+mappingTree.getTree().size()+" size = "+mappingTree.getTree().get(index+1).size());
                    }
                }
            }
        }

        if (mappingTree.getTree().size() == this.pattern.getGraph().vertexSet().size()) {
            extractMatchesFromMappingTree(startDataVertex, year, mappingTree, matchesAroundCenterVertex);
        }
    }

    // TODO: Does this method contain duplicate code that is common with other findMatch methods?
    protected void findAllMatchesOfStarPatternInSnapshotUsingCenterVertex(GraphLoader currentSnapshot, DataVertex centerDataVertex, int t, Set<Set<ConstantLiteral>> matchesAroundCenterVertex) {

        Map<PatternVertex, Set<DataVertex>> patternVertexToDataVerticesMap = getPatternVertexToDataVerticesMap(currentSnapshot, centerDataVertex);

        ArrayList<Map.Entry<PatternVertex, Set<DataVertex>>> vertexSets = new ArrayList<>(patternVertexToDataVerticesMap.entrySet());
        MappingTree mappingTree = new MappingTree();
        for (int i = 0; i < patternVertexToDataVerticesMap.keySet().size(); i++) {
            if (i == 0) {
                mappingTree.addLevel();
                for (DataVertex dv: vertexSets.get(i).getValue()) {
                    mappingTree.createNodeAtLevel(i, dv, vertexSets.get(i).getKey().getTypes().iterator().next(), null);
                }
            } else {
                List<MappingTreeNode> mappingTreeNodeList = mappingTree.getLevel(i-1);
                mappingTree.addLevel();
                for (MappingTreeNode parentNode: mappingTreeNodeList) {
                    for (DataVertex dv: vertexSets.get(i).getValue()) {
                        mappingTree.createNodeAtLevel(i, dv, vertexSets.get(i).getKey().getTypes().iterator().next(), parentNode);
                    }
                }
            }
        }

        extractMatchesFromMappingTree(centerDataVertex, t, mappingTree, matchesAroundCenterVertex);
    }

    private void extractMatchesFromMappingTree(DataVertex centerDataVertex, int t, MappingTree mappingTree, Set<Set<ConstantLiteral>> matchesAroundCenterVertex) {
        Set<Vertex> patternVertexSet = this.pattern.getGraph().vertexSet();
        Set<ConstantLiteral> centerDataVertexLiterals = getCenterDataVertexLiterals(centerDataVertex);
        for (MappingTreeNode leafNode: mappingTree.getLevel(mappingTree.getTree().size()-1).stream().filter(mappingTreeNode -> !mappingTreeNode.isPruned()).collect(Collectors.toList())) {
            Set<MappingTreeNode> mapping = leafNode.getPathToRoot();
            List<Integer> interestingnessMap = new ArrayList<>();
            HashSet<ConstantLiteral> match = new HashSet<>(centerDataVertexLiterals);
            interestingnessMap.add(centerDataVertexLiterals.size());
            for (MappingTreeNode mappingTreeNode: mapping) {
                DataVertex dv = mappingTreeNode.getDataVertex();
                String patternVertexType = mappingTreeNode.getPatternVertexType();
                interestingnessMap.add(0);
                for (String matchedAttrName: dv.getAllAttributesNames()) {
                    for (ConstantLiteral activeAttribute : getActiveAttributesInPattern(true)) {
                        if (!patternVertexType.equals(activeAttribute.getVertexType())) continue;
                        if (!matchedAttrName.equals(activeAttribute.getAttrName())) continue;
                        String matchedAttrValue = dv.getAttributeValueByName(matchedAttrName);
                        ConstantLiteral literal = new ConstantLiteral(patternVertexType, matchedAttrName, matchedAttrValue);
                        match.add(literal);
                        interestingnessMap.set(interestingnessMap.size()-1, interestingnessMap.get(interestingnessMap.size()-1)+1);
                    }
                }
            }
            if (this.interestingOnly) {
                boolean skip = false;
                for (int count: interestingnessMap) {
                    if (count < 2) {
                        skip = true;
                        break;
                    }
                }
                if (skip) continue;
            } else if (match.size() <= patternVertexSet.size()) {
                continue;
            }
            matchesAroundCenterVertex.add(match);
        }

        String entityURI = centerDataVertex.getVertexURI();
        // TODO: The following lines are common among all findMatch methods. Can they be combined?
        if (matchesAroundCenterVertex.size() > 0) { // equivalent to entityURI != null
            this.entityURIs.putIfAbsent(entityURI, TgfdDiscovery.createEmptyArrayListOfSize(this.T));
            this.entityURIs.get(entityURI).set(t, this.entityURIs.get(entityURI).get(t)+matchesAroundCenterVertex.size());
        }
    }

    protected void findAllMatchesOfK1patternInSnapshotUsingCenterVertex(GraphLoader currentSnapshot, DataVertex dataVertex, int timestamp, Set<Set<ConstantLiteral>> matchesAroundCenterVertex) {
        String centerVertexType = this.pattern.getCenterVertexType();
        Set<String> edgeLabels = this.pattern.getGraph().edgeSet().stream().map(RelationshipEdge::getLabel).collect(Collectors.toSet());
        String sourceType = this.pattern.getGraph().edgeSet().iterator().next().getSource().getTypes().iterator().next();
        String targetType = this.pattern.getGraph().edgeSet().iterator().next().getTarget().getTypes().iterator().next();

        Set<RelationshipEdge> edgeSet;
        if (centerVertexType.equals(sourceType)) {
            edgeSet = currentSnapshot.getGraph().getGraph().outgoingEdgesOf(dataVertex).stream().filter(e -> edgeLabels.contains(e.getLabel()) && e.getTarget().getTypes().contains(targetType)).collect(Collectors.toSet());
        } else {
            edgeSet = currentSnapshot.getGraph().getGraph().incomingEdgesOf(dataVertex).stream().filter(e -> edgeLabels.contains(e.getLabel()) && e.getSource().getTypes().contains(sourceType)).collect(Collectors.toSet());
        }

        extractMatches(edgeSet, matchesAroundCenterVertex, timestamp);
    }

    // Use this for k=2 instead of findAllMatchesOfStarPattern to avoid overhead of creating a MappingTree
    private void findAllMatchesOfK2PatternInSnapshotUsingCenterVertex(GraphLoader currentSnapshot, DataVertex dataVertex, int t, Set<Set<ConstantLiteral>> matchesAroundCenterVertex) {
        Set<Vertex> patternVertexSet = this.pattern.getGraph().vertexSet();

        Map<PatternVertex, Set<DataVertex>> patternVertexToDataVerticesMap = getPatternVertexToDataVerticesMap(currentSnapshot, dataVertex);

        Set<ConstantLiteral> centerDataVertexLiterals = getCenterDataVertexLiterals(dataVertex);
        ArrayList<Map.Entry<PatternVertex, Set<DataVertex>>> vertexSets = new ArrayList<>(patternVertexToDataVerticesMap.entrySet());

        Map.Entry<PatternVertex, Set<DataVertex>> nonCenterVertexEntry1 = vertexSets.get(0);
        String nonCenterPatternVertex1Type = nonCenterVertexEntry1.getKey().getTypes().iterator().next();
        for (DataVertex nonCenterDataVertex1: nonCenterVertexEntry1.getValue()) {
            Map.Entry<PatternVertex, Set<DataVertex>> nonCenterVertexEntry2 = vertexSets.get(1);
            String nonCenterPatternVertex2Type = nonCenterVertexEntry2.getKey().getTypes().iterator().next();
            for (DataVertex nonCenterDataVertex2: nonCenterVertexEntry2.getValue()) {
                HashSet<ConstantLiteral> match = new HashSet<>();
                int[] interestingnessMap = {0, 0, 0};
                for (String matchedAttrName: nonCenterDataVertex1.getAllAttributesNames()) {
                    for (ConstantLiteral activeAttribute : getActiveAttributesInPattern(true)) {
                        if (!nonCenterPatternVertex1Type.equals(activeAttribute.getVertexType())) continue;
                        if (!matchedAttrName.equals(activeAttribute.getAttrName())) continue;
                        String matchedAttrValue = nonCenterDataVertex1.getAttributeValueByName(matchedAttrName);
                        ConstantLiteral literal = new ConstantLiteral(nonCenterPatternVertex1Type, matchedAttrName, matchedAttrValue);
                        match.add(literal);
                        interestingnessMap[0] += 1;
                    }
                }
                for (String matchedAttrName: nonCenterDataVertex2.getAllAttributesNames()) {
                    for (ConstantLiteral activeAttribute : getActiveAttributesInPattern(true)) {
                        if (!nonCenterPatternVertex2Type.equals(activeAttribute.getVertexType())) continue;
                        if (!matchedAttrName.equals(activeAttribute.getAttrName())) continue;
                        String matchedAttrValue = nonCenterDataVertex2.getAttributeValueByName(matchedAttrName);
                        ConstantLiteral literal = new ConstantLiteral(nonCenterPatternVertex2Type, matchedAttrName, matchedAttrValue);
                        match.add(literal);
                        interestingnessMap[1] += 1;
                    }
                }
                interestingnessMap[2] += centerDataVertexLiterals.size();
                match.addAll(centerDataVertexLiterals);
                if (this.interestingOnly) {
                    boolean skip = false;
                    for (int count: interestingnessMap) {
                        if (count < 2) {
                            skip = true;
                            break;
                        }
                    }
                    if (skip) continue;
                } else if (match.size() <= patternVertexSet.size()) {
                    continue;
                }
                matchesAroundCenterVertex.add(match);
            }
        }
        String entityURI = dataVertex.getVertexURI();
        if (matchesAroundCenterVertex.size() > 0) { // equivalent to entityURI != null
            this.entityURIs.putIfAbsent(entityURI, TgfdDiscovery.createEmptyArrayListOfSize(this.T));
            this.entityURIs.get(entityURI).set(t, this.entityURIs.get(entityURI).get(t)+matchesAroundCenterVertex.size());
        }
    }

    @NotNull
    private Map<PatternVertex, Set<DataVertex>> getPatternVertexToDataVerticesMap(GraphLoader currentSnapshot, DataVertex dataVertex) {
        Map<PatternVertex, Set<DataVertex>> patternVertexToDataVerticesMap = new HashMap<>();
        Vertex centerPatternVertex = this.pattern.getCenterVertex();
        for (RelationshipEdge patternEdge: this.pattern.getGraph().incomingEdgesOf(centerPatternVertex)) {
            PatternVertex nonCenterPatternVertex = (PatternVertex) patternEdge.getSource();
            patternVertexToDataVerticesMap.put(nonCenterPatternVertex, new HashSet<>());
            for (RelationshipEdge dataEdge: currentSnapshot.getGraph().getGraph().incomingEdgesOf(dataVertex)) {
                if (dataEdge.getLabel().equals(patternEdge.getLabel())
                        && dataEdge.getSource().getTypes().contains(nonCenterPatternVertex.getTypes().iterator().next())) {
                    patternVertexToDataVerticesMap.get(nonCenterPatternVertex).add((DataVertex) dataEdge.getSource());
                }
            }
        }
        for (RelationshipEdge patternEdge: this.pattern.getGraph().outgoingEdgesOf(centerPatternVertex)) {
            PatternVertex nonCenterPatternVertex = (PatternVertex) patternEdge.getTarget();
            patternVertexToDataVerticesMap.put(nonCenterPatternVertex, new HashSet<>());
            for (RelationshipEdge dataEdge: currentSnapshot.getGraph().getGraph().outgoingEdgesOf(dataVertex)) {
                if (dataEdge.getLabel().equals(patternEdge.getLabel())
                        && dataEdge.getTarget().getTypes().contains(nonCenterPatternVertex.getTypes().iterator().next())) {
                    patternVertexToDataVerticesMap.get(nonCenterPatternVertex).add((DataVertex) dataEdge.getTarget());
                }
            }
        }
        return patternVertexToDataVerticesMap;
    }

    @NotNull
    private Set<ConstantLiteral> getCenterDataVertexLiterals(DataVertex dataVertex) {
        Set<ConstantLiteral> centerDataVertexLiterals = new HashSet<>();
        String centerVertexType = this.pattern.getCenterVertexType();
        for (String matchedAttrName: dataVertex.getAllAttributesNames()) {
            for (ConstantLiteral activeAttribute : getActiveAttributesInPattern(true)) {
                if (!centerVertexType.equals(activeAttribute.getVertexType())) continue;
                if (!matchedAttrName.equals(activeAttribute.getAttrName())) continue;
                String matchedAttrValue = dataVertex.getAttributeValueByName(matchedAttrName);
                ConstantLiteral literal = new ConstantLiteral(centerVertexType, matchedAttrName, matchedAttrValue);
                centerDataVertexLiterals.add(literal);
            }
        }
        return centerDataVertexLiterals;
    }

    private void extractMatches(Set<RelationshipEdge> edgeSet, Set<Set<ConstantLiteral>> matchesAroundCenterVertex, int timestamp) {
        String patternEdgeLabel = this.pattern.getGraph().edgeSet().iterator().next().getLabel();
        String sourceVertexType = this.pattern.getGraph().edgeSet().iterator().next().getSource().getTypes().iterator().next();
        String targetVertexType = this.pattern.getGraph().edgeSet().iterator().next().getTarget().getTypes().iterator().next();
        for (RelationshipEdge edge: edgeSet) {
            String matchedEdgeLabel = edge.getLabel();
            Set<String> matchedSourceVertexType = edge.getSource().getTypes();
            Set<String> matchedTargetVertexType = edge.getTarget().getTypes();
            if (matchedEdgeLabel.equals(patternEdgeLabel) && matchedSourceVertexType.contains(sourceVertexType) && matchedTargetVertexType.contains(targetVertexType)) {
                HashSet<ConstantLiteral> literalsInMatch = new HashSet<>();
                Map<String, Integer> interestingnessMap = new HashMap<>();
                String entityURI = extractMatch(edge.getSource(), sourceVertexType, edge.getTarget(), targetVertexType, literalsInMatch, interestingnessMap);
                if (this.interestingOnly && interestingnessMap.values().stream().anyMatch(n -> n < 2)) {
                    continue;
                } else if (!this.interestingOnly && literalsInMatch.size() < this.pattern.getGraph().vertexSet().size()) {
                    continue;
                }
                if (entityURI != null) {
                    this.entityURIs.putIfAbsent(entityURI, TgfdDiscovery.createEmptyArrayListOfSize(this.T));
                    this.entityURIs.get(entityURI).set(timestamp, this.entityURIs.get(entityURI).get(timestamp)+1);
                }
                matchesAroundCenterVertex.add(literalsInMatch);
            }
        }
    }

    private String extractMatch(Vertex currentSourceVertex, String sourceVertexType, Vertex currentTargetVertex, String targetVertexType, HashSet<ConstantLiteral> match, Map<String, Integer> interestingnessMap) {
        String entityURI = null;
        List<String> patternVerticesTypes = Arrays.asList(sourceVertexType, targetVertexType);
        List<Vertex> vertices = Arrays.asList(currentSourceVertex, currentTargetVertex);
        for (int index = 0; index < vertices.size(); index++) {
            Vertex currentMatchedVertex = vertices.get(index);
            String patternVertexType = patternVerticesTypes.get(index);
            if (entityURI == null) {
                entityURI = extractAttributes(patternVertexType, match, currentMatchedVertex, interestingnessMap);
            } else {
                extractAttributes(patternVertexType, match, currentMatchedVertex, interestingnessMap);
            }
        }
        return entityURI;
    }
}
