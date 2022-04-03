package IncrementalRunner;

import VF2Runner.VF2SubgraphIsomorphism;
import changeExploration.*;
import Infra.*;
import org.jgrapht.Graph;
import org.jgrapht.GraphMapping;
import org.jgrapht.graph.DefaultDirectedGraph;

import java.util.*;
import java.util.stream.Collectors;

public class IncUpdates {

    public VF2DataGraph getBaseGraph() {
        return baseGraph;
    }

    private VF2DataGraph baseGraph;
    private VF2SubgraphIsomorphism VF2;

    /** Map of the relevant TGFDs for each entity type */
    private HashMap<String, HashSet<String>> relaventTGFDs=new HashMap <>();


    public IncUpdates(VF2DataGraph baseGraph, List<TGFD> tgfds)
    {
        this.baseGraph=baseGraph;
        this.VF2= new VF2SubgraphIsomorphism();

        for (TGFD tgfd:tgfds) {
            extractValidTypesFromTGFD(tgfd);
        }
    }

    public void updateEntireGraph(List<Change> allChanges) {
        for (Change change: allChanges) {
            if (change instanceof VertexChange) {
                if (change.getTypeOfChange() == ChangeType.insertVertex){
                    baseGraph.addVertex(((VertexChange) change).getVertex());
                } else if (change.getTypeOfChange() == ChangeType.deleteVertex) {
                    baseGraph.deleteVertex(((VertexChange) change).getVertex());
                }
            }
            else if(change instanceof EdgeChange) {
                EdgeChange edgeChange = (EdgeChange) change;
                DataVertex v1 = (DataVertex) baseGraph.getNode(edgeChange.getSrc());
                DataVertex v2 = (DataVertex) baseGraph.getNode(edgeChange.getDst());
                if (v1 == null || v2 == null) {
                    // Node doesn't exist in the base graph, we need to igonre the change
                    // We keep the number of these ignored edges in a variable
                    continue;
                }
                if (edgeChange.getTypeOfChange()== ChangeType.insertEdge) {
                    baseGraph.addEdge(v1, v2, new RelationshipEdge(edgeChange.getLabel()));
                }
                else if (edgeChange.getTypeOfChange()== ChangeType.deleteEdge) {
                    baseGraph.removeEdge(v1, v2, new RelationshipEdge(edgeChange.getLabel()));
                }
                else
                    throw new IllegalArgumentException("The change is instance of EdgeChange, but type of change is: " + change.getTypeOfChange());
            }
            else if (change instanceof AttributeChange || change instanceof TypeChange) {
                String uri;
                if (change instanceof AttributeChange)
                    uri = ((AttributeChange) change).getUri();
                else
                    uri = ((TypeChange) change).getPreviousVertex().getVertexURI();
                DataVertex v1=(DataVertex) baseGraph.getNode(uri);
                if (v1 == null) {
                    // Node doesn't exist in the base graph, we need to igonre the change
                    // We store the number of these ignored changes
                    continue;
                }
                if (change instanceof AttributeChange) {
                    AttributeChange attributeChange = (AttributeChange) change;

                    if (attributeChange.getTypeOfChange() == ChangeType.changeAttr || attributeChange.getTypeOfChange() == ChangeType.insertAttr) {
                        v1.putAttribute(attributeChange.getAttribute());
                    } else if (attributeChange.getTypeOfChange() == ChangeType.deleteAttr) {
                        v1.deleteAttribute(attributeChange.getAttribute());
                    }
                } else {
                    Set<String> removedTypes = new HashSet<>();
                    DataVertex newVertex = ((TypeChange)change).getNewVertex();
                    for (String oldVertexType: v1.getTypes()) {
                        if (!newVertex.getTypes().contains(oldVertexType)) {
                            removedTypes.add(oldVertexType);
                        }
                    }
                    Set<String> newTypes = new HashSet<>();
                    for (String newVertexType: newVertex.getTypes()) {
                        if (!v1.getTypes().contains(newVertexType)) {
                            newTypes.add(newVertexType);
                        }
                    }
                    for (String removedType: removedTypes) {
                        v1.getTypes().remove(removedType);
                    }
                    for (String newType: newTypes) {
                        v1.getTypes().add(newType);
                    }
                }
            }
        }
    }

    public HashMap<String,IncrementalChange> updateGraphByGroupOfChanges(HashSet<Change> changes, HashMap<String, TGFD> tgfdsByName, boolean performVertexChange)
    {
        // Remove TGFDs from the Affected TGFD lists of the change if that TGFD is not loaded.
        changes.forEach(change -> change
                        .getTGFDs()
                        .removeIf(TGFDName -> !tgfdsByName.containsKey(TGFDName)));

        if(changes.size()==0)
            return null;

        Change change=changes.iterator().next();

        if (performVertexChange && change instanceof VertexChange) {
            VertexChange vertexChange = (VertexChange) change;
            // TODO: If findRelevantTGFDs returns 0 should we skip?
            if (change.getTypeOfChange() == ChangeType.deleteVertex) {
                DataVertex v1 = (DataVertex) baseGraph.getNode(vertexChange.getVertex().getVertexURI());
                if (v1 == null)
                    return null;
                if (change.getTGFDs().size() == 0)
                    findRelevantTGFDs(vertexChange,v1);
                if (change.getTGFDs().size() == 0)
                    return null;
                return updateGraphByVertex(v1,change,change.getTGFDs(),tgfdsByName, false);
            } else if (change.getTypeOfChange() == ChangeType.insertVertex) {
                if (change.getTGFDs().size() == 0)
                    findRelevantTGFDs(vertexChange, ((VertexChange) change).getVertex());
                if (change.getTGFDs().size() == 0)
                    return null;
                DataVertex v1 = (DataVertex) baseGraph.getNode(vertexChange.getVertex().getVertexURI());
                if (v1 != null) // if a node with identical uri wasn't deleted before, delete it now
                    baseGraph.deleteVertex(v1);
                return updateGraphByVertex(((VertexChange) change).getVertex(),change,change.getTGFDs(),tgfdsByName, true);
            }
            else
                return null;
        }
        else if(change instanceof EdgeChange)
        {
            EdgeChange edgeChange=(EdgeChange) change;
            DataVertex v1= (DataVertex) baseGraph.getNode(edgeChange.getSrc());
            DataVertex v2= (DataVertex) baseGraph.getNode(edgeChange.getDst());
            if(v1==null || v2==null)
            {
                // Node doesn't exist in the base graph, we need to igonre the change
                // We keep the number of these ignored edges in a variable
                return null;
            }
            // If TGFD list in the change is empty (specifically for synthetic graph),
            // we need to find the relevant TGFDs and add to the TGFD list
            if(change.getTGFDs().size()==0)
            {
                findRelevantTGFDs(edgeChange,v1);
                findRelevantTGFDs(edgeChange,v2);
            }
            // TODO: If change.getTGFDs().size() is still 0, can we return null?
            if (edgeChange.getTypeOfChange()== ChangeType.insertEdge)
                return updateGraphByEdge(v1,v2,new RelationshipEdge(edgeChange.getLabel()),change.getTGFDs(),tgfdsByName,true);
            else if (edgeChange.getTypeOfChange()== ChangeType.deleteEdge)
                return updateGraphByEdge(v1,v2,new RelationshipEdge(edgeChange.getLabel()),change.getTGFDs(),tgfdsByName,false);
            else
                throw new IllegalArgumentException("The change is instance of EdgeChange, but type of change is: " + edgeChange.getTypeOfChange());
        }
        else if(change instanceof AttributeChange || change instanceof TypeChange)
        {
            String uri = change instanceof AttributeChange ? ((AttributeChange) change).getUri() : ((TypeChange) change).getPreviousVertex().getVertexURI();
            DataVertex v1=(DataVertex) baseGraph.getNode(uri);
            if(v1==null)
            {
                // Node doesn't exist in the base graph, we need to igonre the change
                // We store the number of these ignored changes
                return null;
            }
            // If TGFD list in the change is empty (specifically for synthetic graph),
            // we need to find the relevant TGFDs and add to the TGFD list
            Set<String> affectedTGFDs = new HashSet<>();
            boolean includesTypeChange = false;
            if(change.getTGFDs().size()==0)
            {
//                findRelevantTGFDs(change1,v1);
                for (Change c: changes) {
                    if (c instanceof AttributeChange) {
                        findRelevantTGFDs(c, v1);
                        affectedTGFDs.addAll(c.getTGFDs());
                    } else {
                        findRelevantTGFDs(c, v1);
                        findRelevantTGFDs(c, ((TypeChange)c).getNewVertex());
                        affectedTGFDs.addAll(c.getTGFDs());
                        includesTypeChange = true;
                    }
                }
            }
            // TODO: If change.getTGFDs().size() is still 0, can we return null?
            return updateGraphBySetOfAttributes(v1,changes,affectedTGFDs,tgfdsByName,includesTypeChange);
        }
        else
            return null;
    }

    private HashMap<String, IncrementalChange> updateGraphByVertex(DataVertex v1, Change change, Set<String> affectedTGFDNames, HashMap<String, TGFD> tgfdsByName, boolean insertVertex) {
        HashMap<String,IncrementalChange> incrementalChangeHashMap=new HashMap <>();
        Graph<Vertex, RelationshipEdge> subgraph;
        if (insertVertex) {
            subgraph = new DefaultDirectedGraph<>(RelationshipEdge.class);
            for (String tgfdName:affectedTGFDNames) {
                Iterator<GraphMapping<Vertex, RelationshipEdge>> beforeChange = Collections.emptyIterator();
                IncrementalChange incrementalChange = new IncrementalChange(beforeChange, tgfdsByName.get(tgfdName).getPattern());
                incrementalChangeHashMap.put(tgfdsByName.get(tgfdName).getName(), incrementalChange);
            }
        } else {
            subgraph = baseGraph.getSubGraphWithinDiameter(v1, 0, new HashSet<>(), new HashSet<>());
            for (String tgfdName:affectedTGFDNames) {
                Iterator<GraphMapping<Vertex, RelationshipEdge>> beforeChange = VF2.execute(subgraph,tgfdsByName.get(tgfdName).getPattern(),false);
                IncrementalChange incrementalChange=new IncrementalChange(beforeChange,tgfdsByName.get(tgfdName).getPattern());
                incrementalChangeHashMap.put(tgfdsByName.get(tgfdName).getName(),incrementalChange);
            }
        }

        if (insertVertex) {
//            subgraph.addVertex(v1);
            baseGraph.addVertex(v1);
            v1 = (DataVertex) baseGraph.getNode(v1.getVertexURI());
            subgraph = baseGraph.getSubGraphWithinDiameter(v1, 0, new HashSet<>(), new HashSet<>());
        } else {
            subgraph.removeVertex(v1);
            baseGraph.deleteVertex(((VertexChange) change).getVertex());
        }

        // Run VF2 again...
        for (String tgfdName:affectedTGFDNames) {
            Iterator<GraphMapping<Vertex, RelationshipEdge>> afterChange;
//            if (insertVertex) {
            afterChange = VF2.execute(subgraph,tgfdsByName.get(tgfdName).getPattern(),false);
//            } else {
//                afterChange = VF2.execute(subgraph,tgfdsByName.get(tgfdName).getPattern(),false);
//            }

            String res = incrementalChangeHashMap.get(tgfdsByName.get(tgfdName).getName()).addAfterMatches(afterChange);

            //System.out.print("  ** Add: " + (System.currentTimeMillis()-runtime) + " - " + res + " \n");
        }
        return incrementalChangeHashMap;
    }

    public void deleteVertices(List<Change> allChange)
    {
        for (Change change:allChange) {
            if(change instanceof VertexChange && change.getTypeOfChange()==ChangeType.deleteVertex)
            {
                baseGraph.deleteVertex(((VertexChange) change).getVertex());
            }
        }
    }

    public void AddNewVertices(List<Change> allChange)
    {
        for (Change change:allChange) {
            if(change instanceof VertexChange && change.getTypeOfChange()==ChangeType.insertVertex)
            {
                baseGraph.addVertex(((VertexChange) change).getVertex());
            }
        }
    }

    private HashMap<String,IncrementalChange> updateGraphByEdge(
            DataVertex v1, DataVertex v2, RelationshipEdge edge,Set <String> affectedTGFDNames, HashMap<String,TGFD> tgfdsByName, boolean addEdge)
    {
        //long runtime=System.currentTimeMillis();
        HashMap<String,IncrementalChange> incrementalChangeHashMap=new HashMap <>();
        DataVertex vertex = v1;
        int radius;
        if (getPatternSize(affectedTGFDNames, tgfdsByName) < 1) {
            radius = 0;
        } else if (getPatternSize(affectedTGFDNames, tgfdsByName) == 1) {
            radius = 1;
        } else { // if (getPatternSize(affectedTGFDNames, tgfdsByName) > 1) {
            int v1Radius = getDiameter(affectedTGFDNames, tgfdsByName, v1);
            int v2Radius = getDiameter(affectedTGFDNames, tgfdsByName, v2);
            vertex = v1Radius <= v2Radius ? v1 : v2;
            radius = Math.min(v1Radius, v2Radius);
        }
        Graph<Vertex, RelationshipEdge> subgraph = baseGraph.getSubGraphWithinDiameter(vertex,radius,getEdgeLabels(affectedTGFDNames,tgfdsByName),getPatternEdgeSet(affectedTGFDNames,tgfdsByName));

        //System.out.print("Load: " + (System.currentTimeMillis()-runtime));
        //runtime=System.currentTimeMillis();

        // run VF2
        for (String tgfdName:affectedTGFDNames) {
            Iterator<GraphMapping<Vertex, RelationshipEdge>> beforeChange = VF2.execute(subgraph,tgfdsByName.get(tgfdName).getPattern(),false);
            IncrementalChange incrementalChange=new IncrementalChange(beforeChange,tgfdsByName.get(tgfdName).getPattern());
            incrementalChangeHashMap.put(tgfdsByName.get(tgfdName).getName(),incrementalChange);
        }
        //perform the change...
        if(addEdge) // Update the graph by adding the edge
        {
            DataVertex otherVertex = vertex == v1 ? v2: v1;
            if(!subgraph.containsVertex(otherVertex))
            {
                subgraph.addVertex(otherVertex);
            }
            if (subgraph.containsVertex(v1) && subgraph.containsVertex(v2)) {
                subgraph.addEdge(v1, v2, edge);
            }
            baseGraph.addEdge(v1, v2,edge);
        }
        else // update the graph by removing the edge
        {
            // Now, perform the change and remove the edge from the subgraph
            if (subgraph.containsVertex(v1)) {
                for (RelationshipEdge e : subgraph.outgoingEdgesOf(v1)) {
                    DataVertex target = (DataVertex) e.getTarget();
                    if (target.getVertexURI().equals(v2.getVertexURI()) && edge.getLabel().equals(e.getLabel())) {
                        subgraph.removeEdge(e);
                        break;
                    }
                }
            }
            //remove from the base graph.
            baseGraph.removeEdge(v1,v2,edge);
        }


        // Run VF2 again...
        for (String tgfdName:affectedTGFDNames) {
            Iterator<GraphMapping<Vertex, RelationshipEdge>> afterChange = VF2.execute(subgraph,tgfdsByName.get(tgfdName).getPattern(),false);
            String res = incrementalChangeHashMap.get(tgfdsByName.get(tgfdName).getName()).addAfterMatches(afterChange);

            //System.out.print("  ** Add: " + (System.currentTimeMillis()-runtime) + " - " + res + " \n");
        }
        return incrementalChangeHashMap;
    }

    private HashMap<String,IncrementalChange> updateGraphBySetOfAttributes(DataVertex v1, HashSet<Change> changes, Set<String> affectedTGFDNames, HashMap<String, TGFD> tgfdsByName, boolean includesTypeChange)
    {
        //long runtime=System.currentTimeMillis();

        HashMap<String,IncrementalChange> incrementalChangeHashMap=new HashMap <>();
        Graph<Vertex, RelationshipEdge> subgraph;
        if (includesTypeChange) {
            subgraph = baseGraph.getSubGraphWithinDiameter(v1,getDiameter(affectedTGFDNames,tgfdsByName,v1),getEdgeLabels(affectedTGFDNames,tgfdsByName),getPatternEdgeSet(affectedTGFDNames,tgfdsByName),getChangedTypeURIs(changes));
        } else {
            subgraph = baseGraph.getSubGraphWithinDiameter(v1,getDiameter(affectedTGFDNames,tgfdsByName,v1),getEdgeLabels(affectedTGFDNames,tgfdsByName),getPatternEdgeSet(affectedTGFDNames,tgfdsByName));
        }

        //System.out.print("Load: " + (System.currentTimeMillis()-runtime));
        //runtime=System.currentTimeMillis();

        // run VF2
        for (String tgfdName:affectedTGFDNames) {
            Iterator<GraphMapping<Vertex, RelationshipEdge>> beforeChange = VF2.execute(subgraph,tgfdsByName.get(tgfdName).getPattern(),false);
            IncrementalChange incrementalChange=new IncrementalChange(beforeChange,tgfdsByName.get(tgfdName).getPattern());
            incrementalChangeHashMap.put(tgfdsByName.get(tgfdName).getName(),incrementalChange);

        }

        // Apply all the changes
        for (Change change:changes) {
            if (change instanceof AttributeChange) {
                AttributeChange attributeChange = (AttributeChange) change;

                if (attributeChange.getTypeOfChange() == ChangeType.changeAttr || attributeChange.getTypeOfChange() == ChangeType.insertAttr) {
                    v1.putAttribute(attributeChange.getAttribute());
                } else if (attributeChange.getTypeOfChange() == ChangeType.deleteAttr) {
                    v1.deleteAttribute(attributeChange.getAttribute());
                }
            } else {
                Set<String> removedTypes = new HashSet<>();
                DataVertex newVertex = ((TypeChange)change).getNewVertex();
                for (String oldVertexType: v1.getTypes()) {
                    if (!newVertex.getTypes().contains(oldVertexType)) {
                        removedTypes.add(oldVertexType);
                    }
                }
                Set<String> newTypes = new HashSet<>();
                for (String newVertexType: newVertex.getTypes()) {
                    if (!v1.getTypes().contains(newVertexType)) {
                        newTypes.add(newVertexType);
                    }
                }
                for (String removedType: removedTypes) {
                    v1.getTypes().remove(removedType);
                }
                for (String newType: newTypes) {
                    v1.getTypes().add(newType);
                }
            }
        }

        // Run VF2 again...
        for (String tgfdName:affectedTGFDNames) {
            Iterator<GraphMapping<Vertex, RelationshipEdge>> afterChange = VF2.execute(subgraph,tgfdsByName.get(tgfdName).getPattern(),false);
            String res = incrementalChangeHashMap.get(tgfdsByName.get(tgfdName).getName()).addAfterMatches(afterChange);

            //System.out.print("  ** Upd: " + (System.currentTimeMillis()-runtime) + " - " + res + " \n");
        }
        return incrementalChangeHashMap;
    }

    private Set<String> getChangedTypeURIs(HashSet<Change> changes) {
        Set<String> uris = new HashSet<>();
        for (Change change: changes) {
            if (change instanceof TypeChange) {
                uris.add(((TypeChange)change).getPreviousVertex().getVertexURI());
                uris.add(((TypeChange)change).getNewVertex().getVertexURI());
            }
        }
        return uris;
    }

    private int getDiameter(Set<String> affectedTGFDNames, HashMap<String, TGFD> tgfdsByName, DataVertex dataVertex)
    {
        //TODO: Need to get the max diameter
        int maxDiameter=0;
        for (String tgfdName:affectedTGFDNames) {
            VF2PatternGraph pattern = tgfdsByName.get(tgfdName).getPattern();
            Graph<Vertex,RelationshipEdge> patternGraph = pattern.getPattern();
            if (patternGraph.edgeSet().size() == 1) {
                return pattern.getRadius();
            } else {
                if (dataVertex.getTypes().contains(pattern.getCenterVertexType())) {
                    return pattern.getRadius();
                }
                for (Vertex v : patternGraph.vertexSet()) {
                    if (dataVertex.getTypes().containsAll(v.getTypes())) {
                        return pattern.calculateRadiusForGivenVertex(v);
                    }
                }
            }
            return pattern.getDiameter();
        }
        return maxDiameter;
    }

    private Set<String> getEdgeLabels(Set<String> affectedTGFDNames, HashMap<String, TGFD> tgfdsByName) {
        for (String tgfdName:affectedTGFDNames) {
            return tgfdsByName.get(tgfdName).getPattern().getPattern().edgeSet().stream().map(RelationshipEdge::getLabel).collect(Collectors.toSet());
        }
        return new HashSet<>();
    }

    private Set<RelationshipEdge> getPatternEdgeSet(Set<String> affectedTGFDNames, HashMap<String, TGFD> tgfdsByName) {
        for (String tgfdName:affectedTGFDNames) {
            return tgfdsByName.get(tgfdName).getPattern().getPattern().edgeSet();
        }
        return new HashSet<>();
    }

    private int getPatternSize(Set<String> affectedTGFDNames, HashMap<String, TGFD> tgfdsByName) {
        for (String tgfdName:affectedTGFDNames) {
            return tgfdsByName.get(tgfdName).getPattern().getPattern().edgeSet().size();
        }
        return 0;
    }

    private void findRelevantTGFDs(Change change,DataVertex v)
    {
        //change.addTGFD(v.getTypes().stream().filter(type -> relaventTGFDs.containsKey(type)));
        for (String type:v.getTypes())
            if (relaventTGFDs.containsKey(type))
                change.addTGFD(relaventTGFDs.get(type));
    }

    /**
     * Extracts all the types being used in a TGFD from from X->Y dependency and the graph pattern
     * For each type, add the TGFD name to the HashMap so we know
     * what TGFDs are affected if a an entity of a specific type had a change
     * @param tgfd input TGFD
     */
    private void extractValidTypesFromTGFD(TGFD tgfd)
    {
        for (Literal x:tgfd.getDependency().getX()) {
            if(x instanceof ConstantLiteral)
                addRelevantType(((ConstantLiteral) x).getVertexType(),tgfd.getName());
            else if(x instanceof VariableLiteral)
            {
                addRelevantType(((VariableLiteral) x).getVertexType_1(),tgfd.getName());
                addRelevantType(((VariableLiteral) x).getVertexType_2(),tgfd.getName());
            }
        }
        for (Literal y:tgfd.getDependency().getY()) {
            if(y instanceof ConstantLiteral)
                addRelevantType(((ConstantLiteral) y).getVertexType(),tgfd.getName());
            else if(y instanceof VariableLiteral)
            {
                addRelevantType(((VariableLiteral) y).getVertexType_1(),tgfd.getName());
                addRelevantType(((VariableLiteral) y).getVertexType_2(),tgfd.getName());
            }
        }
        for (Vertex v:tgfd.getPattern().getPattern().vertexSet()) {
            if(v instanceof PatternVertex)
                for (String type:v.getTypes())
                    addRelevantType(type,tgfd.getName());
        }
    }

    /**
     * This method adds the TGFD name to the HashSet of the input type
     * @param type input type
     * @param TGFDName input TGFD name
     */
    private void addRelevantType(String type, String TGFDName)
    {
        if(!relaventTGFDs.containsKey(type))
            relaventTGFDs.put(type,new HashSet <>());
        relaventTGFDs.get(type).add(TGFDName);
    }

}
