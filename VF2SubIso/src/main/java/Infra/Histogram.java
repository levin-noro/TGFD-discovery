package Infra;

import IncrementalRunner.IncUpdates;
import TgfdDiscovery.TgfdDiscovery;
import changeExploration.Change;
import changeExploration.ChangeLoader;
import changeExploration.ChangeType;
import changeExploration.TypeChange;
import graphLoader.DBPediaLoader;
import graphLoader.GraphLoader;
import graphLoader.IMDBLoader;
import graphLoader.SyntheticLoader;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.json.simple.JSONArray;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

public class Histogram {
    private final int frequentSetSize;
    private final int gamma;
    private final Set<String> interestLabelsSet;
    public double MEDIAN_SUPER_VERTEX_TYPE_INDEGREE_FLOOR = 25.0;
    public final double DEFAULT_MAX_SUPER_VERTEX_DEGREE = 1500.0;
    public final double DEFAULT_AVG_SUPER_VERTEX_DEGREE = 30.0;
    private final int T;
    private final long histogramStarTime;
    private final Map<String, Integer> vertexTypesHistogram = new HashMap<>();
    private final Map<String, Set<String>> vertexTypesToAttributesMap = new HashMap<>();
    private final Map<String, Set<String>> attrDistributionMap = new HashMap<>();
    private final Map<String, List<Integer>> vertexTypesToInDegreesMap = new HashMap<>();
    private final Map<String, Integer> edgeTypesHistogram = new HashMap<>();
    private final List<GraphLoader> graphs;
    private List<Map.Entry<String, List<String>>> timestampToPathsMap;
    private HashMap<String, JSONArray> changefilesToJsonArrayMap = null;
    String loader;
    private int numOfEdgesInAllGraphs;
    private int numOfVerticesInAllGraphs;
    private final Map<String, Double> vertexTypesToMedianInDegreeMap = new HashMap<>();
    private final Map<String, Integer> vertexHistogram = new HashMap<>();
    private final boolean[] allSnapshotsCheckList;
    private Set<String> activeAttributesSet;
    private List<Map.Entry<String, Integer>> sortedFrequentEdgesHistogram;
    private ArrayList<Map.Entry<String, Integer>> sortedVertexTypesHistogram;
    private long totalHistogramTime;
    private Map<String, Set<String>> vertexTypesToActiveAttributesMap;
    private Map<String, Set<String>> typeChangesURIs;

    public Histogram(int T, List<Map.Entry<String, List<String>>> timestampToPathsMap, String loader, int frequentSetSize, int gamma, Set<String> interestLabelsSet) {
        this.T = T;
        this.histogramStarTime = System.currentTimeMillis();
        this.timestampToPathsMap = timestampToPathsMap;
        this.loader = loader;
        this.graphs = new ArrayList<>();
        this.allSnapshotsCheckList = new boolean[this.T];
        this.frequentSetSize = frequentSetSize;
        this.gamma = gamma;
        this.interestLabelsSet = interestLabelsSet;
    }

    public Histogram(int T, List<GraphLoader> graphs, int frequentSetSize, int gamma, Set<String> interestLabelsSet) {
        if (graphs.size() != T)
            throw new IllegalArgumentException("List<GraphLoader> graphs does not contain paths to T snapshots");
        this.T = T;
        this.histogramStarTime = System.currentTimeMillis();
        this.graphs = graphs;
        this.allSnapshotsCheckList = new boolean[this.T];
        this.frequentSetSize = frequentSetSize;
        this.gamma = gamma;
        this.interestLabelsSet = interestLabelsSet;
    }

    public void computeHistogramUsingChangefilesAll(List<String> changefilePaths, boolean storeInMemory, Integer superVertexDegree, boolean dissolveTypes) {
        if (changefilePaths == null || changefilePaths.size() == 0)
            throw new IllegalArgumentException("No paths specified for changefiles.");
        System.out.println("-----------Snapshot (1)-----------");
        GraphLoader graphLoader = createGraphForTimestamp(timestampToPathsMap.get(0));
        computeHistogramOfSnapshot(graphLoader, superVertexDegree); allSnapshotsCheckList[0] = true;
        for (int i = 0; i < this.T-1; i++) {
            System.out.println("-----------Snapshot (" + (i + 2) + ")-----------");
            updateGraphUsingChangefile(graphLoader, changefilePaths.get(i), storeInMemory);
            computeHistogramOfSnapshot(graphLoader, superVertexDegree); allSnapshotsCheckList[i+1] = true;
        }
        this.printVertexAndEdgeStatisticsForEntireTemporalGraph();
        final long superVertexHandlingTime = System.currentTimeMillis();
        // TODO: What is the best way to estimate a good value for SUPER_VERTEX_DEGREE for each run?
//		this.calculateAverageInDegree(vertexTypesToInDegreesMap);
        double degreeForSuperVertexTypes = this.calculateMedianInDegree();
//		this.calculateMaxInDegree(vertexTypesToInDegreesMap);
        if (dissolveTypes) {
            if (this.graphs.size() != this.T)
                throw new IllegalArgumentException("Cannot dissolve types without storing graphs in memory");
            System.out.println("Collapsing vertices with an in-degree above " + degreeForSuperVertexTypes);
            this.dissolveSuperVertexTypesAndUpdateHistograms(degreeForSuperVertexTypes);
            TgfdDiscovery.printWithTime("Super vertex types dissolution", (System.currentTimeMillis() - superVertexHandlingTime));
        }
        performRecordKeeping();
    }

    public void computeHistogramByReadingGraphsFromFile(boolean storeInMemory, Integer superVertexDegree) {
        if (timestampToPathsMap.size() != T)
            throw new IllegalArgumentException("timestampToPathsMap does not contain paths to T snapshots");
        int t = 0;
        for (Map.Entry<String, List<String>> timestampToPathEntry : timestampToPathsMap) {
            GraphLoader graphLoader = createGraphForTimestamp(timestampToPathEntry);
            computeHistogramOfSnapshot(graphLoader, superVertexDegree); allSnapshotsCheckList[t] = true; t++;
            if (storeInMemory)
                this.graphs.add(graphLoader);
        }
        this.printVertexAndEdgeStatisticsForEntireTemporalGraph();
        this.calculateMedianInDegree();
        performRecordKeeping();
    }

    public void computeHistogramUsingGraphs(Integer superVertexDegree) {
        int t = 0;
        for (GraphLoader graph : graphs) {
            computeHistogramOfSnapshot(graph, superVertexDegree); allSnapshotsCheckList[t] = true; t++;
        }
        this.printVertexAndEdgeStatisticsForEntireTemporalGraph();
        this.calculateMedianInDegree();
        performRecordKeeping();
    }

    public void computeHistogramByReadingGraphsFromFileAndDissolveTypes() {
        if (timestampToPathsMap.size() != T)
            throw new IllegalArgumentException("timestampToPathsMap does not contain paths to T snapshots");
        int t = 0;
        for (Map.Entry<String, List<String>> timestampToPathEntry : timestampToPathsMap) {
            GraphLoader graphLoader = createGraphForTimestamp(timestampToPathEntry);
            computeHistogramOfSnapshot(graphLoader, null); allSnapshotsCheckList[t] = true; t++;
            this.graphs.add(graphLoader);
        }
        this.printVertexAndEdgeStatisticsForEntireTemporalGraph();
        performRecordKeeping();
    }

    private void performRecordKeeping() {

        setActiveAttributeSet();
        vertexTypesToActiveAttributesMap();

        setSortedFrequentEdgeHistogram();
        setSortedFrequentVerticesUsingFrequentEdges();

        // TODO: Should number of snapshots always be set to T?
//		this.findAndSetNumOfSnapshots();
//		this.setNumOfSnapshots(this.getT());

        this.totalHistogramTime = System.currentTimeMillis() - this.getHistogramStarTime();
        TgfdDiscovery.printWithTime("All snapshots histogram", this.totalHistogramTime);
        printHistogram();
        printHistogramStatistics();
    }

    public void printHistogram() {

        System.out.println("Number of vertex types: " + this.sortedVertexTypesHistogram.size());
        System.out.println("Frequent Vertices:");
        for (Map.Entry<String, Integer> entry : this.sortedVertexTypesHistogram) {
            String vertexType = entry.getKey();
            Set<String> attributes = this.vertexTypesToActiveAttributesMap.get(vertexType);
            System.out.println(vertexType + "={count=" + entry.getValue() + ", support=" + (1.0 * entry.getValue() / this.getNumOfVerticesInAllGraphs()) + ", attributes=" + attributes + "}");
        }

        System.out.println();
        System.out.println("Size of active attributes set: " + this.getActiveAttributesSet().size());
        System.out.println("Attributes:");
        for (String attrName : this.getActiveAttributesSet()) {
            System.out.println(attrName);
        }
        System.out.println();
        System.out.println("Number of edge types: " + this.sortedFrequentEdgesHistogram.size());
        System.out.println("Frequent Edges:");
        for (Map.Entry<String, Integer> entry : this.sortedFrequentEdgesHistogram) {
            System.out.println("edge=\"" + entry.getKey() + "\", count=" + entry.getValue() + ", support=" +(1.0 * entry.getValue() / this.getNumOfEdgesInAllGraphs()));
        }
        System.out.println();
    }


    private void printVertexAndEdgeStatisticsForEntireTemporalGraph() {
        System.out.println("Number of vertices across all graphs: " + this.numOfVerticesInAllGraphs);
        System.out.println("Number of vertex types across all graphs: " + this.vertexTypesHistogram.size());
        System.out.println("Number of edge lables across all graphs: " + this.edgeTypesHistogram.size());
        System.out.println("Number of edges across all graphs before dissolving: " + this.numOfEdgesInAllGraphs);
    }

    private void updateGraphUsingChangefile(GraphLoader graphLoader, String changeFilePath, boolean storeInMemory) {
        final long graphUpdateTime = System.currentTimeMillis();
        JSONArray jsonArray = TgfdDiscovery.readJsonArrayFromFile(changeFilePath);
        ChangeLoader changeLoader = new ChangeLoader(jsonArray, null, null, true);
        IncUpdates incUpdatesOnDBpedia = new IncUpdates(graphLoader.getGraph(), new ArrayList<>());
        this.typeChangesURIs = new HashMap<>();
        for (Change change: changeLoader.getAllChanges()) {
            if (change.getTypeOfChange() == ChangeType.changeType) {
                String uri = ((TypeChange) change).getUri();
                this.typeChangesURIs.putIfAbsent(uri, new HashSet<>());
                this.typeChangesURIs.get(uri).addAll(change.getTypes());
            }
        }
        TgfdDiscovery.sortChanges(changeLoader.getAllChanges());
        incUpdatesOnDBpedia.updateEntireGraph(changeLoader.getAllChanges());
        TgfdDiscovery.printWithTime("Graph update time", (System.currentTimeMillis() - graphUpdateTime));
        if (storeInMemory) {
            if (this.changefilesToJsonArrayMap == null)
                this.changefilesToJsonArrayMap = new HashMap<>();
            this.changefilesToJsonArrayMap.put(changeFilePath, jsonArray);
        }
    }

    public GraphLoader createGraphForTimestamp(Map.Entry<String, List<String>> timestampToPathEntry) {
        final long graphLoadTime = System.currentTimeMillis();
        Model model = ModelFactory.createDefaultModel();
        for (String path : timestampToPathEntry.getValue()) {
            if (!path.toLowerCase().endsWith(".ttl") && !path.toLowerCase().endsWith(".nt"))
                continue;
            if (path.toLowerCase().contains("literals") || path.toLowerCase().contains("objects"))
                continue;
            Path input = Paths.get(path);
            model.read(input.toUri().toString());
        }
        Model dataModel = ModelFactory.createDefaultModel();
        for (String path : timestampToPathEntry.getValue()) {
            if (!path.toLowerCase().endsWith(".ttl") && !path.toLowerCase().endsWith(".nt"))
                continue;
            if (path.toLowerCase().contains("types"))
                continue;
            Path input = Paths.get(path);
            System.out.println("Reading data graph: " + path);
            dataModel.read(input.toUri().toString());
        }
        GraphLoader graphLoader = switch (this.loader) {
            case "dbpedia" -> new DBPediaLoader(new ArrayList<>(), Collections.singletonList(model), Collections.singletonList(dataModel));
            case "synthetic" -> new SyntheticLoader(new ArrayList<>(), timestampToPathEntry.getValue());
            case "imdb" -> new IMDBLoader(new ArrayList<>(), Collections.singletonList(dataModel));
            default -> throw new IllegalArgumentException("Specified loader " + this.loader + " is not supported.");
        };
        TgfdDiscovery.printWithTime("Single graph load", (System.currentTimeMillis() - graphLoadTime));
        return graphLoader;
    }

    private void computeHistogramOfSnapshot(GraphLoader graph, Integer superVertexDegree) {
        long graphHistogramTime = System.currentTimeMillis();
        readVertexTypesAndAttributeNamesFromGraph(graph, superVertexDegree);
        readEdgesInfoFromGraph(graph);
        TgfdDiscovery.printWithTime("Single graph histogram", (System.currentTimeMillis() - graphHistogramTime));
    }

    private void readVertexTypesAndAttributeNamesFromGraph(GraphLoader graph, Integer superVertexDegree) {
        int initialEdgeCount = graph.getGraph().getGraph().edgeSet().size();
        System.out.println("Initial count of edges in graph: " + initialEdgeCount);

        int numOfAttributesAdded = 0;
        int numOfEdgesDeleted = 0;
        int numOfVerticesInGraph = 0;
        int numOfAttributesInGraph = 0;
        for (Vertex v: graph.getGraph().getGraph().vertexSet()) {
            numOfVerticesInGraph++;
            for (String vertexType : v.getTypes()) {
                this.vertexTypesHistogram.merge(vertexType, 1, Integer::sum);
                this.vertexTypesToAttributesMap.putIfAbsent(vertexType, new HashSet<>());

                for (String attrName: v.getAllAttributesNames()) {
                    if (attrName.equals("uri"))
                        continue;
                    numOfAttributesInGraph++;
                    if (this.vertexTypesToAttributesMap.containsKey(vertexType)) {
                        this.vertexTypesToAttributesMap.get(vertexType).add(attrName);
                    }
                    if (!this.attrDistributionMap.containsKey(attrName)) {
                        this.attrDistributionMap.put(attrName, new HashSet<>());
                    }
                    this.attrDistributionMap.get(attrName).add(vertexType);
                }
            }

            int inDegree = graph.getGraph().getGraph().incomingEdgesOf(v).size();
            if (superVertexDegree != null && inDegree > superVertexDegree) {
                List<RelationshipEdge> edgesToDelete = new ArrayList<>(graph.getGraph().getGraph().incomingEdgesOf(v));
                for (RelationshipEdge e : edgesToDelete) {
                    Vertex sourceVertex = e.getSource();
                    Map<String, Attribute> sourceVertexAttrMap = sourceVertex.getAllAttributesHashMap();
                    String newAttrName = e.getLabel();
                    if (sourceVertexAttrMap.containsKey(newAttrName)) {
                        newAttrName = e.getLabel() + "value";
                        if (!sourceVertexAttrMap.containsKey(newAttrName)) {
                            sourceVertex.putAttributeIfAbsent(new Attribute(newAttrName, v.getAttributeValueByName("uri")));
                            numOfAttributesAdded++;
                        }
                    }
                    if (graph.getGraph().getGraph().removeEdge(e)) {
                        numOfEdgesDeleted++;
                    }
                    for (String subjectVertexType : sourceVertex.getTypes()) {
                        for (String objectVertexType : e.getTarget().getTypes()) {
                            String uniqueEdge = subjectVertexType + " " + e.getLabel() + " " + objectVertexType;
                            this.edgeTypesHistogram.merge(uniqueEdge, 1, Integer::sum);
                        }
                    }
                }
                // Update all attribute related histograms
                for (String vertexType : v.getTypes()) {
                    this.vertexTypesToAttributesMap.putIfAbsent(vertexType, new HashSet<>());
                    for (String attrName : v.getAllAttributesNames()) {
                        if (attrName.equals("uri")) continue;
                        if (this.vertexTypesToAttributesMap.containsKey(vertexType)) {
                            this.vertexTypesToAttributesMap.get(vertexType).add(attrName);
                        }
                        if (!this.attrDistributionMap.containsKey(attrName)) {
                            this.attrDistributionMap.put(attrName, new HashSet<>());
                        }
                        this.attrDistributionMap.get(attrName).add(vertexType);
                    }
                }
            }
            for (String vertexType : v.getTypes()) {
                if (!this.vertexTypesToInDegreesMap.containsKey(vertexType)) {
                    this.vertexTypesToInDegreesMap.put(vertexType, new ArrayList<>());
                }
                if (inDegree > 0) {
                    this.vertexTypesToInDegreesMap.get(vertexType).add(inDegree);
                }
            }
        }
        for (Map.Entry<String, List<Integer>> entry: this.vertexTypesToInDegreesMap.entrySet()) {
            entry.getValue().sort(Comparator.naturalOrder());
        }
        System.out.println("Number of vertices in graph: " + numOfVerticesInGraph);
        System.out.println("Number of attributes in graph: " + numOfAttributesInGraph);

        System.out.println("Number of attributes added to graph: " + numOfAttributesAdded);
        System.out.println("Updated count of attributes in graph: " + (numOfAttributesInGraph+numOfAttributesAdded));

        System.out.println("Number of edges deleted from graph: " + numOfEdgesDeleted);
        int newEdgeCount = graph.getGraph().getGraph().edgeSet().size();
        System.out.println("Updated count of edges in graph: " + newEdgeCount);

        this.numOfVerticesInAllGraphs += numOfVerticesInGraph;
    }

    private void readEdgesInfoFromGraph(GraphLoader graph) {
        int numOfEdges = 0;
        for (RelationshipEdge e: graph.getGraph().getGraph().edgeSet()) {
            numOfEdges++;
            Vertex sourceVertex = e.getSource();
            String predicateName = e.getLabel();
            Vertex objectVertex = e.getTarget();
            for (String subjectVertexType: sourceVertex.getTypes()) {
                for (String objectVertexType: objectVertex.getTypes()) {
                    String uniqueEdge = subjectVertexType + " " + predicateName + " " + objectVertexType;
                    this.edgeTypesHistogram.merge(uniqueEdge, 1, Integer::sum);
                }
            }
        }
        System.out.println("Number of edges in graph: " + numOfEdges);
        this.numOfEdgesInAllGraphs += numOfEdges;
    }

    private double calculateAverageInDegree(Map<String, List<Integer>> vertexTypesToInDegreesMap) {
        System.out.println("Average in-degrees of vertex types...");
        List<Double> avgInDegrees = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> entry: vertexTypesToInDegreesMap.entrySet()) {
            if (entry.getValue().size() == 0) continue;
            entry.getValue().sort(Comparator.naturalOrder());
            double avgInDegree = (double) entry.getValue().stream().mapToInt(Integer::intValue).sum() / (double) entry.getValue().size();
            System.out.println(entry.getKey()+": "+avgInDegree);
            avgInDegrees.add(avgInDegree);
            this.vertexTypesToMedianInDegreeMap.put(entry.getKey(), avgInDegree);
        }
//		double avgInDegree = avgInDegrees.stream().mapToDouble(Double::doubleValue).sum() / (double) avgInDegrees.size();
        double avgInDegree = this.getHighOutlierThreshold(avgInDegrees);
        double degreeForSuperVertexTypes = (Math.max(avgInDegree, DEFAULT_AVG_SUPER_VERTEX_DEGREE));
        System.out.println("Super vertex degree is "+ degreeForSuperVertexTypes);
        return degreeForSuperVertexTypes;
    }

    protected double calculateMedianInDegree() {
        System.out.println("Median in-degrees of vertex types...");
        List<Double> medianInDegrees = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> entry: this.vertexTypesToInDegreesMap.entrySet()) {
            if (entry.getValue().size() == 0) {
                this.vertexTypesToMedianInDegreeMap.put(entry.getKey(), 0.0);
                continue;
            }
            entry.getValue().sort(Comparator.naturalOrder());
            double medianInDegree;
            if (entry.getValue().size() % 2 == 0) {
                medianInDegree = (entry.getValue().get(entry.getValue().size()/2) + entry.getValue().get(entry.getValue().size()/2-1))/2.0;
            } else {
                medianInDegree = entry.getValue().get(entry.getValue().size()/2);
            }
            System.out.println(entry.getKey()+": "+medianInDegree);
            medianInDegrees.add(medianInDegree);
            this.vertexTypesToMedianInDegreeMap.put(entry.getKey(), medianInDegree);
        }
        double medianInDegree = this.getHighOutlierThreshold(medianInDegrees);
        double degreeForSuperVertexTypes = Math.max(medianInDegree, MEDIAN_SUPER_VERTEX_TYPE_INDEGREE_FLOOR);
        System.out.println("Super vertex degree is "+ degreeForSuperVertexTypes);
        return degreeForSuperVertexTypes;
    }

    private void calculateMaxInDegree(Map<String, List<Integer>> vertexTypesToInDegreesMap) {
        System.out.println("Max in-degrees of vertex types...");
        List<Double> maxInDegrees = new ArrayList<>();
        for (Map.Entry<String, List<Integer>> entry: vertexTypesToInDegreesMap.entrySet()) {
            if (entry.getValue().size() == 0) continue;
            double maxInDegree = Collections.max(entry.getValue()).doubleValue();
            System.out.println(entry.getKey()+": "+maxInDegree);
            maxInDegrees.add(maxInDegree);
            this.vertexTypesToMedianInDegreeMap.put(entry.getKey(), maxInDegree);
        }
        double maxInDegree = getHighOutlierThreshold(maxInDegrees);
        System.out.println("Based on histogram, high outlier threshold for in-degree is "+maxInDegree);
        double degreeForSuperVertexTypes = (Math.max(maxInDegree, DEFAULT_MAX_SUPER_VERTEX_DEGREE));
        System.out.println("Super vertex degree is "+ degreeForSuperVertexTypes);
    }

    private double getHighOutlierThreshold(List<Double> listOfDegrees) {
        listOfDegrees.sort(Comparator.naturalOrder());
        if (listOfDegrees.size() == 1) return listOfDegrees.get(0);
        double q1, q3;
        if (listOfDegrees.size() % 2 == 0) {
            int halfSize = listOfDegrees.size()/2;
            q1 = listOfDegrees.get(halfSize/2);
            q3 = listOfDegrees.get((halfSize+ listOfDegrees.size())/2);
        } else {
            int middleIndex = listOfDegrees.size()/2;
            List<Double> firstHalf = listOfDegrees.subList(0,middleIndex);
            q1 = firstHalf.get(firstHalf.size()/2);
            List<Double> secondHalf = listOfDegrees.subList(middleIndex, listOfDegrees.size());
            q3 = secondHalf.get(secondHalf.size()/2);
        }
        double iqr = q3 - q1;
        return q3 + (9 * iqr);
    }

    protected void dissolveSuperVertexTypesAndUpdateHistograms(double degreeForSuperVertexTypes) {
        int numOfCollapsedSuperVertices = 0;
        this.numOfEdgesInAllGraphs = 0;
        this.numOfVerticesInAllGraphs = 0;
        for (Map.Entry<String, List<Integer>> entry: this.vertexTypesToInDegreesMap.entrySet()) {
            String superVertexType = entry.getKey();
            if (entry.getValue().size() == 0) continue;
            double medianDegree = this.vertexTypesToMedianInDegreeMap.get(entry.getKey());
            if (medianDegree > degreeForSuperVertexTypes) {
                System.out.println("Collapsing super vertex "+superVertexType+" with...");
                System.out.println("Degree = "+medianDegree+", Vertex Count = "+this.vertexTypesHistogram.get(superVertexType));
                numOfCollapsedSuperVertices++;
                for (GraphLoader graph: this.graphs) {
                    int numOfVertices = 0;
                    int numOfAttributes = 0;
                    int numOfAttributesAdded = 0;
                    int numOfEdgesDeleted = 0;
                    for (Vertex v: graph.getGraph().getGraph().vertexSet()) {
                        numOfVertices++;
                        if (v.getTypes().contains(superVertexType)) {
                            // Add edge label as an attribute and delete respective edge
                            List<RelationshipEdge> edgesToDelete = new ArrayList<>(graph.getGraph().getGraph().incomingEdgesOf(v));
                            for (RelationshipEdge e: edgesToDelete) {
                                Vertex sourceVertex = e.getSource();
                                Map<String, Attribute> sourceVertexAttrMap = sourceVertex.getAllAttributesHashMap();
                                String newAttrName = e.getLabel();
                                if (sourceVertexAttrMap.containsKey(newAttrName)) {
                                    newAttrName = e.getLabel() + "value";
                                    if (!sourceVertexAttrMap.containsKey(newAttrName)) {
                                        sourceVertex.putAttributeIfAbsent(new Attribute(newAttrName, v.getAttributeValueByName("uri")));
                                        numOfAttributesAdded++;
                                    }
                                }
                                if (graph.getGraph().getGraph().removeEdge(e)) {
                                    numOfEdgesDeleted++;
                                }
                                for (String subjectVertexType: sourceVertex.getTypes()) {
                                    for (String objectVertexType : e.getTarget().getTypes()) {
                                        String uniqueEdge = subjectVertexType + " " + e.getLabel() + " " + objectVertexType;
                                        edgeTypesHistogram.put(uniqueEdge, edgeTypesHistogram.get(uniqueEdge)-1);
                                    }
                                }
                            }
                            // Update all attribute related histograms
                            for (String vertexType : v.getTypes()) {
                                vertexTypesToAttributesMap.putIfAbsent(vertexType, new HashSet<>());
                                for (String attrName: v.getAllAttributesNames()) {
                                    if (attrName.equals("uri")) continue;
                                    numOfAttributes++;
                                    if (vertexTypesToAttributesMap.containsKey(vertexType)) {
                                        vertexTypesToAttributesMap.get(vertexType).add(attrName);
                                    }
                                    if (!attrDistributionMap.containsKey(attrName)) {
                                        attrDistributionMap.put(attrName, new HashSet<>());
                                    }
                                    attrDistributionMap.get(attrName).add(vertexType);
                                }
                            }
                        }
                    }
                    System.out.println("Updated count of vertices in graph: " + numOfVertices);
                    this.numOfVerticesInAllGraphs = this.getNumOfVerticesInAllGraphs()+numOfVertices;

                    System.out.println("Number of attributes added to graph: " + numOfAttributesAdded);
                    System.out.println("Updated count of attributes in graph: " + numOfAttributes);

                    System.out.println("Number of edges deleted from graph: " + numOfEdgesDeleted);
                    int newEdgeCount = graph.getGraph().getGraph().edgeSet().size();
                    System.out.println("Updated count of edges in graph: " + newEdgeCount);
                    this.numOfEdgesInAllGraphs = this.getNumOfEdgesInAllGraphs()+newEdgeCount;
                }
            }
        }
        System.out.println("Number of super vertices collapsed: "+numOfCollapsedSuperVertices);
    }

    public void setActiveAttributeSet() {
        System.out.println("Most distributed attributes:");
        ArrayList<Map.Entry<String,Set<String>>> sortedAttrDistributionMap = new ArrayList<>(this.attrDistributionMap.entrySet());
        sortedAttrDistributionMap.sort((o1, o2) -> o2.getValue().size() - o1.getValue().size());
        int exclusiveIndex = Math.min(gamma, sortedAttrDistributionMap.size());
        Set<String> activeAttributesSet = new HashSet<>();
        for (Map.Entry<String, Set<String>> attrNameEntry : sortedAttrDistributionMap.subList(0, exclusiveIndex)) {
            System.out.println(attrNameEntry);
            activeAttributesSet.add(attrNameEntry.getKey());
        }
        if (interestLabelsSet.size() > 0) {
            for (Map.Entry<String, Set<String>> attrNameEntry : sortedAttrDistributionMap.subList(exclusiveIndex, sortedAttrDistributionMap.size())) {
                if (interestLabelsSet.contains(attrNameEntry.getKey())) {
                    System.out.println(attrNameEntry);
                    activeAttributesSet.add(attrNameEntry.getKey());
                }
            }
        }
        System.out.println();
        this.activeAttributesSet = activeAttributesSet;
    }

    public void setSortedFrequentEdgeHistogram() {
        for (boolean checked: allSnapshotsCheckList)
            if (!checked) throw new IllegalArgumentException("Cannot create sorted frequent edges set. Histogram was not run on all T snapshots.");

        List<Map.Entry<String, Integer>> finalEdgesHist = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : this.edgeTypesHistogram.entrySet()) {
            String[] edgeString = entry.getKey().split(" ");
            String sourceType = edgeString[0];
            String targetType = edgeString[2];
            this.vertexHistogram.put(sourceType, vertexTypesHistogram.get(sourceType));
            this.vertexHistogram.put(targetType, vertexTypesHistogram.get(targetType));
            if (this.vertexTypesToActiveAttributesMap.get(sourceType).size() > 0 && this.vertexTypesToActiveAttributesMap.get(targetType).size() > 0) {
                finalEdgesHist.add(entry);
            }
        }
        finalEdgesHist.sort((o1, o2) -> o2.getValue() - o1.getValue());
        int exclusiveIndex = Math.min(finalEdgesHist.size(), frequentSetSize);
        this.sortedFrequentEdgesHistogram = new ArrayList<>(finalEdgesHist.subList(0, exclusiveIndex));
        if (this.interestLabelsSet.size() > 0) {
            for (Map.Entry<String, Integer> entry : finalEdgesHist.subList(exclusiveIndex, finalEdgesHist.size())) {
                String[] edgeString = entry.getKey().split(" ");
                String sourceType = edgeString[0];
                if (this.interestLabelsSet.contains(sourceType)) {
                    sortedFrequentEdgesHistogram.add(entry);
                    continue;
                }
                String edgeLabel = edgeString[1];
                if (this.interestLabelsSet.contains(edgeLabel)) {
                    sortedFrequentEdgesHistogram.add(entry);
                    continue;
                }
                String targetType = edgeString[2];
                if (this.interestLabelsSet.contains(targetType)) {
                    sortedFrequentEdgesHistogram.add(entry);
                    continue;
                }
            }
        }
    }

    public double getMedianEdgeFrequency() {
        List<Double> edgeFrequencies = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : this.edgeTypesHistogram.entrySet()) {
            double edgeFrequency = (double) entry.getValue() / (double) this.getNumOfEdgesInAllGraphs();
            edgeFrequencies.add(edgeFrequency);
        }
        Collections.sort(edgeFrequencies);
        double medianEdgeSupport = 0;
        if (edgeFrequencies.size() > 0) {
            medianEdgeSupport = edgeFrequencies.size() % 2 != 0 ? edgeFrequencies.get(edgeFrequencies.size() / 2) : ((edgeFrequencies.get(edgeFrequencies.size() / 2) + edgeFrequencies.get(edgeFrequencies.size() / 2 - 1)) / 2);
        }
        return medianEdgeSupport;
    }

    public double getMedianVertexFrequency() {
        List<Double> vertexFrequencies = new ArrayList<>();
        for (Map.Entry<String, Integer> entry : this.vertexTypesHistogram.entrySet()) {
            double edgeFrequency = (double) entry.getValue() / (double) this.getNumOfVerticesInAllGraphs();
            vertexFrequencies.add(edgeFrequency);
        }
        Collections.sort(vertexFrequencies);
        double medianVertexSupport = 0;
        if (vertexFrequencies.size() > 0) {
            medianVertexSupport = vertexFrequencies.size() % 2 != 0 ? vertexFrequencies.get(vertexFrequencies.size() / 2) : ((vertexFrequencies.get(vertexFrequencies.size() / 2) + vertexFrequencies.get(vertexFrequencies.size() / 2 - 1)) / 2);
        }
        return medianVertexSupport;
    }

    public void printHistogramStatistics() {
        for (boolean checked: allSnapshotsCheckList)
            if (!checked) throw new IllegalArgumentException("Cannot calculate histogram statistics. Histogram was not run on all T snapshots.");
        System.out.println("----------------Statistics for Histogram-----------------");
        System.out.println("Median Vertex Frequency: " + getMedianVertexFrequency());
        System.out.println("Median Edge Frequency: " + getMedianEdgeFrequency());
        // TODO: Add statistics for the median of the sorted sets?
    }

    public void setSortedFrequentVerticesUsingFrequentEdges() {
        for (boolean checked: allSnapshotsCheckList)
            if (!checked) throw new IllegalArgumentException("Cannot create sorted frequent vertices set. Histogram was not run on all T snapshots.");

        Set<String> relevantFrequentVertexTypes = new HashSet<>();
        for (Map.Entry<String, Integer> entry : this.sortedFrequentEdgesHistogram) {
            String[] edgeString = entry.getKey().split(" ");
            String sourceType = edgeString[0];
            relevantFrequentVertexTypes.add(sourceType);
            String targetType = edgeString[2];
            relevantFrequentVertexTypes.add(targetType);
        }
        Map<String, Integer> relevantVertexTypesHistogram = new HashMap<>();
        for (String relevantVertexType: relevantFrequentVertexTypes) {
            if (vertexTypesHistogram.containsKey(relevantVertexType)) {
                relevantVertexTypesHistogram.put(relevantVertexType, vertexTypesHistogram.get(relevantVertexType));
            }
        }
        this.sortedVertexTypesHistogram = new ArrayList<>(relevantVertexTypesHistogram.entrySet());
        sortedVertexTypesHistogram.sort((o1, o2) -> o2.getValue() - o1.getValue());
        for (Map.Entry<String, Integer> entry : sortedVertexTypesHistogram) {
            this.vertexHistogram.put(entry.getKey(), entry.getValue());
        }
    }

    public void vertexTypesToActiveAttributesMap() {
        this.vertexTypesToActiveAttributesMap = new HashMap<>();
        for (String vertexType : this.vertexTypesToAttributesMap.keySet()) {
            Set<String> attrNameSet = this.vertexTypesToAttributesMap.get(vertexType);
            vertexTypesToActiveAttributesMap.put(vertexType, new HashSet<>());
            for (String attrName : attrNameSet) {
                if (this.activeAttributesSet.contains(attrName)) { // Filters non-active attributes
                    vertexTypesToActiveAttributesMap.get(vertexType).add(attrName);
                }
            }
        }
    }

    public int getNumOfEdgesInAllGraphs() {
        return numOfEdgesInAllGraphs;
    }

    public int getNumOfVerticesInAllGraphs() {
        return numOfVerticesInAllGraphs;
    }

    public Map<String, Integer> getVertexTypesHistogram() {
        return vertexTypesHistogram;
    }

    public Map<String, Set<String>> getVertexTypesToAttributesMap() {
        return vertexTypesToAttributesMap;
    }

    public Map<String, Set<String>> getAttrDistributionMap() {
        return attrDistributionMap;
    }

    public Map<String, List<Integer>> getVertexTypesToInDegreesMap() {
        return vertexTypesToInDegreesMap;
    }

    public Map<String, Integer> getEdgeTypesHistogram() {
        return edgeTypesHistogram;
    }

    public long getHistogramStarTime() {
        return histogramStarTime;
    }

    public List<GraphLoader> getGraphs() {
        return graphs;
    }

    public HashMap<String, JSONArray> getChangefilesToJsonArrayMap() {
        return changefilesToJsonArrayMap;
    }

    public Map<String, Double> getVertexTypesToMedianInDegreeMap() {
        return vertexTypesToMedianInDegreeMap;
    }

    public Map<String, Integer> getVertexHistogram() {
        return vertexHistogram;
    }

    public Set<String> getActiveAttributesSet() {
        return activeAttributesSet;
    }

    public List<Map.Entry<String, Integer>> getSortedFrequentEdgesHistogram() {
        return sortedFrequentEdgesHistogram;
    }

    public ArrayList<Map.Entry<String, Integer>> getSortedVertexTypesHistogram() {
        return sortedVertexTypesHistogram;
    }

    public long getTotalHistogramTime() {
        return totalHistogramTime;
    }

    public Map<String, Set<String>> getVertexTypesToActiveAttributesMap() {
        return vertexTypesToActiveAttributesMap;
    }

    public Map<String, Set<String>> getTypeChangesURIs() {
        return typeChangesURIs;
    }
}
