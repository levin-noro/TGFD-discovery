//package TgfdDiscovery;
//
//import IncrementalRunner.IncUpdates;
//import Infra.ConstantLiteral;
//import Infra.PatternTreeNode;
//import Infra.TGFD;
//import changeExploration.ChangeLoader;
//import graphLoader.DBPediaLoader;
//import graphLoader.GraphLoader;
//import graphLoader.IMDBLoader;
//import org.apache.commons.cli.CommandLine;
//import org.apache.commons.cli.Options;
//import org.apache.jena.rdf.model.Model;
//import org.apache.jena.rdf.model.ModelFactory;
//import org.json.simple.JSONArray;
//import org.json.simple.parser.JSONParser;
//
//import java.io.FileNotFoundException;
//import java.io.FileReader;
//import java.io.PrintStream;
//import java.nio.file.Files;
//import java.nio.file.Path;
//import java.nio.file.Paths;
//import java.time.ZoneId;
//import java.time.ZonedDateTime;
//import java.time.format.DateTimeFormatter;
//import java.util.*;
//
//public class TestChangeFile extends TgfdDiscovery{
//    public TestChangeFile(String[] args) {
//        super();
//        init(args);
//        super.loadGraphsAndComputeHistogram(this.getTimestampToFilesMap().subList(0,1));
//    }
//
//    public TestChangeFile(String[] args, List<GraphLoader> graphs) {
//        super();
//        init(args);
////        this.readGraphsAndComputeHistogram(graphs);
//    }
//
//    public void readGraphsAndComputeHistogram(List<GraphLoader> graphs) {
//        System.out.println("Computing Histogram...");
//
//        final long histogramTime = System.currentTimeMillis();
//
//        Map<String, Integer> vertexTypesHistogram = new HashMap<>();
//        Map<String, Set<String>> tempVertexAttrFreqMap = new HashMap<>();
//        Map<String, Set<String>> attrDistributionMap = new HashMap<>();
//        Map<String, List<Integer>> vertexTypesToInDegreesMap = new HashMap<>();
//        Map<String, Integer> edgeTypesHistogram = new HashMap<>();
//
//        this.setGraphs(new ArrayList<>());
//
//        for (GraphLoader graph: graphs) {
//            if (this.isStoreInMemory() || this.getLoader().equalsIgnoreCase("dbpedia")) {
//                if (this.useChangeFile()) {
//                    if (this.getGraphs().size() == 0) {
//                        this.getGraphs().add(graph);
//                        this.loadChangeFilesIntoMemory();
//                    }
//                } else {
//                    this.getGraphs().add(graph);
//                }
//            }
//            final long graphReadTime = System.currentTimeMillis();
//            readVertexTypesAndAttributeNamesFromGraph(vertexTypesHistogram, tempVertexAttrFreqMap, attrDistributionMap, vertexTypesToInDegreesMap, graph, edgeTypesHistogram);
//            readEdgesInfoFromGraph(edgeTypesHistogram, graph);
//            printWithTime("Single graph read", (System.currentTimeMillis() - graphReadTime));
//        }
//
//        this.printVertexAndEdgeStatisticsForEntireTemporalGraph(graphs, vertexTypesHistogram);
//
//        final long superVertexHandlingTime = System.currentTimeMillis();
//        // TO-DO: What is the best way to estimate a good value for SUPER_VERTEX_DEGREE for each run?
////		this.calculateAverageInDegree(vertexTypesToInDegreesMap);
//        this.calculateMedianInDegree(vertexTypesToInDegreesMap);
////		this.calculateMaxInDegree(vertexTypesToInDegreesMap);
//        if (this.isDissolveSuperVertexTypes()) {
//            if (this.getGraphs().size() > 0) {
//                System.out.println("Collapsing vertices with an in-degree above " + this.getSuperVertexDegree());
//                this.dissolveSuperVerticesAndUpdateHistograms(tempVertexAttrFreqMap, attrDistributionMap, vertexTypesToInDegreesMap, edgeTypesHistogram);
//                printWithTime("Super vertex dissolution", (System.currentTimeMillis() - superVertexHandlingTime));
//            }
//        }
//
//        this.setActiveAttributeSet(attrDistributionMap);
//
//        this.setVertexTypesToAttributesMap(tempVertexAttrFreqMap);
//
//        System.out.println("Number of edges across all graphs: " + this.getNumOfEdgesInAllGraphs());
//        System.out.println("Number of edges labels across all graphs: " + edgeTypesHistogram.size());
//        this.setSortedFrequentEdgeHistogram(edgeTypesHistogram, vertexTypesHistogram);
//
//        this.findAndSetNumOfSnapshots();
//
//        printWithTime("All snapshots histogram", (System.currentTimeMillis() - histogramTime));
//        printHistogram();
//        printHistogramStatistics();
//    }
//
//    private void init(String[] args) {
//        String timeAndDateStamp = ZonedDateTime.now(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern("uuuu.MM.dd.HH.mm.ss"));
//        this.setExperimentDateAndTimeStamp(timeAndDateStamp);
//        this.setStartTime(System.currentTimeMillis());
//
//        Options options = TgfdDiscovery.initializeCmdOptions();
//        CommandLine cmd = TgfdDiscovery.parseArgs(options, args);
//
//        if (cmd.hasOption("path")) {
//            this.setPath(cmd.getOptionValue("path").replaceFirst("^~", System.getProperty("user.home")));
//            if (!Files.isDirectory(Path.of(this.getPath()))) {
//                System.out.println(Path.of(this.getPath()) + " is not a valid directory.");
//                System.exit(1);
//            }
//            this.setGraphSize(Path.of(this.getPath()).getFileName().toString());
//        }
//
//        if (!cmd.hasOption("loader")) {
//            System.out.println("No specifiedLoader is specified.");
//            System.exit(1);
//        } else {
//            this.setLoader(cmd.getOptionValue("loader"));
//        }
//
//        this.setUseChangeFile(this.getLoader().equalsIgnoreCase("imdb"));
//
//        if (cmd.hasOption("name")) {
//            this.setExperimentName(cmd.getOptionValue("name"));
//        } else  {
//            this.setExperimentName("experiment");
//        }
//
//        if (!cmd.hasOption("console")) {
//            PrintStream logStream = null;
//            try {
//                logStream = new PrintStream("tgfd-discovery-log-" + timeAndDateStamp + ".txt");
//            } catch (FileNotFoundException e) {
//                e.printStackTrace();
//            }
//            System.setOut(logStream);
//        }
//
//        this.setOnlyInterestingTGFDs(!cmd.hasOption("uninteresting"));
//
//        this.setGeneratek0Tgfds(cmd.hasOption("k0"));
//        this.setSkipK1(cmd.hasOption("skipK1"));
//
//        this.setGamma(cmd.getOptionValue("a") == null ? TgfdDiscovery.DEFAULT_GAMMA : Integer.parseInt(cmd.getOptionValue("a")));
//        this.setTgfdTheta(cmd.getOptionValue("theta") == null ? TgfdDiscovery.DEFAULT_TGFD_THETA : Double.parseDouble(cmd.getOptionValue("theta")));
//        this.setPatternTheta(cmd.getOptionValue("pTheta") == null ? this.getTgfdTheta() : Double.parseDouble(cmd.getOptionValue("pTheta")));
//        this.setK(cmd.getOptionValue("k") == null ? TgfdDiscovery.DEFAULT_K : Integer.parseInt(cmd.getOptionValue("k")));
//        this.setFrequentSetSize(cmd.getOptionValue(TgfdDiscovery.FREQUENT_SIZE_SET_PARAM) == null ? TgfdDiscovery.DEFAULT_FREQUENT_SIZE_SET : Integer.parseInt(cmd.getOptionValue(TgfdDiscovery.FREQUENT_SIZE_SET_PARAM)));
//
//        this.initializeTgfdLists();
//
//        if (cmd.hasOption("K")) this.markAsKexperiment();
//
//        if (cmd.hasOption("simplifySuperVertexTypes")) {
//            MEDIAN_SUPER_VERTEX_TYPE_INDEGREE_FLOOR = Double.parseDouble(cmd.getOptionValue("simplifySuperVertexTypes"));
//            this.setDissolveSuperVertexTypes(true);
//        } else if (cmd.hasOption("simplifySuperVertex")) {
//            INDIVIDUAL_VERTEX_INDEGREE_FLOOR = Integer.valueOf(cmd.getOptionValue("simplifySuperVertex"));
//            this.setDissolveSuperVerticesBasedOnCount(true);
//        }
//
//        switch (this.getLoader().toLowerCase()) {
//            case "dbpedia" -> this.setDBpediaTimestampsAndFilePaths(this.getPath());
//            case "citation" -> this.setCitationTimestampsAndFilePaths();
//            case "imdb" -> this.setImdbTimestampToFilesMapFromPath(this.getPath());
//            default -> {
//                System.out.println("No loader is specified.");
//                System.exit(1);
//            }
//        }
//    }
//
//    public static void main(String[] args) {
//        String overallTimeAndDateStamp = ZonedDateTime.now(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern("uuuu.MM.dd.HH.mm.ss"));
//        ArrayList<Long> runtimes = new ArrayList<>();
//        for (int index = 0; index <= 4; index++) {
//
//            TestChangeFile tgfdDiscoveryInit = new TestChangeFile(args);
//
//            if (tgfdDiscoveryInit.getGraphs().size() == 1) {
//                HashMap<String, org.json.simple.JSONArray> changeFilesMap = new HashMap<>();
//                for (int i = 1; i < 3; i++) {
//                    GraphLoader graph;
//                    Model model = ModelFactory.createDefaultModel();
//                    for (String path : tgfdDiscoveryInit.getTimestampToFilesMap().get(0).getValue()) {
//                        if (path.toLowerCase().contains("literals") || path.toLowerCase().contains("objects") || !path.toLowerCase().contains(".ttl"))
//                            continue;
//                        Path input = Paths.get(path);
//                        model.read(input.toUri().toString());
//                    }
//                    Model dataModel = ModelFactory.createDefaultModel();
//                    for (String path : tgfdDiscoveryInit.getTimestampToFilesMap().get(0).getValue()) {
//                        if (path.toLowerCase().contains("types") || !path.toLowerCase().contains(".ttl")) continue;
//                        Path input = Paths.get(path);
//                        System.out.println("Reading data graph: " + path);
//                        dataModel.read(input.toUri().toString());
//                    }
//                    if (tgfdDiscoveryInit.getLoader().equals("dbpedia")) {
//                        graph = new DBPediaLoader(new ArrayList<>(), Collections.singletonList(model), Collections.singletonList(dataModel));
//                    } else {
//                        graph = new IMDBLoader(new ArrayList<>(), Collections.singletonList(dataModel));
//                    }
//                    tgfdDiscoveryInit.getGraphs().add(graph);
//                    for (int j = 0; j < i; j++) {
//                        String changefilePath = "changes_t" + (j+1) + "_t" + (j+2) + "_" + tgfdDiscoveryInit.getGraphSize() + ".json";
//                        JSONParser parser = new JSONParser();
//                        Object json;
//                        org.json.simple.JSONArray jsonArray = new JSONArray();
//                        try {
//                            json = parser.parse(new FileReader(changefilePath));
//                            jsonArray = (org.json.simple.JSONArray) json;
//                        } catch (Exception e) {
//                            e.printStackTrace();
//                        }
//                        System.out.println("Storing " + changefilePath + " in memory");
//                        changeFilesMap.put(changefilePath, jsonArray);
//                        ChangeLoader changeLoader = new ChangeLoader(changeFilesMap.get(changefilePath), true);;
//                        IncUpdates incUpdatesOnDBpedia = new IncUpdates(graph.getGraph(), new ArrayList<>());
//                        sortChanges(changeLoader.getAllChanges());
//                        incUpdatesOnDBpedia.updateEntireGraph(changeLoader.getAllChanges());
//                    }
//                }
//                tgfdDiscoveryInit.setChangeFilesMap(changeFilesMap);
//            }
//            tgfdDiscoveryInit.setNumOfSnapshots(tgfdDiscoveryInit.getGraphs().size());
//
//            TestChangeFile realTgfdDiscovery = new TestChangeFile(args, tgfdDiscoveryInit.getGraphs());
//
//            realTgfdDiscovery.setUseChangeFile(false);
//            realTgfdDiscovery.setReUseMatches(false);
//            realTgfdDiscovery.setSupportPruning(false);
//            realTgfdDiscovery.setMinimalityPruning(false);
//            if (index == 0) {
//                realTgfdDiscovery.setReUseMatches(true);
//            } else if (index == 1) {
//                realTgfdDiscovery.setUseChangeFile(true);
//            } else if (index == 2) {
//                realTgfdDiscovery.setSupportPruning(true);
//            } else if (index == 3) {
//                realTgfdDiscovery.setMinimalityPruning(true);
//            }
//
//            realTgfdDiscovery.readGraphsAndComputeHistogram(tgfdDiscoveryInit.getGraphs());
//
//            realTgfdDiscovery.printInfo();
//
//            realTgfdDiscovery.initialize(realTgfdDiscovery.getGraphs());
//
//            while (realTgfdDiscovery.getCurrentVSpawnLevel() <= realTgfdDiscovery.getK()) {
//
//                System.out.println("VSpawn level " + realTgfdDiscovery.getCurrentVSpawnLevel());
//                System.out.println("Previous level node index " + realTgfdDiscovery.getPreviousLevelNodeIndex());
//                System.out.println("Candidate edge index " + realTgfdDiscovery.getCandidateEdgeIndex());
//
//                PatternTreeNode patternTreeNode = null;
//                long vSpawnTime = System.currentTimeMillis();
//                while (patternTreeNode == null && realTgfdDiscovery.getCurrentVSpawnLevel() <= realTgfdDiscovery.getK()) {
//                    patternTreeNode = realTgfdDiscovery.vSpawn();
//                }
//                vSpawnTime = System.currentTimeMillis() - vSpawnTime;
//                TgfdDiscovery.printWithTime("vSpawn", vSpawnTime);
//                realTgfdDiscovery.addToTotalVSpawnTime(vSpawnTime);
//                if (realTgfdDiscovery.getCurrentVSpawnLevel() > realTgfdDiscovery.getK()) break;
//                long matchingTime = System.currentTimeMillis();
//
//                assert patternTreeNode != null;
//                List<Set<Set<ConstantLiteral>>> matchesPerTimestamps;
//                if (realTgfdDiscovery.isValidationSearch()) {
//                    matchesPerTimestamps = realTgfdDiscovery.getMatchesForPatternUsingVF2(patternTreeNode);
//                    matchingTime = System.currentTimeMillis() - matchingTime;
//                    TgfdDiscovery.printWithTime("findMatchesUsingChangeFiles", (matchingTime));
//                    realTgfdDiscovery.addToTotalMatchingTime(matchingTime);
//                }
//                else if (realTgfdDiscovery.useChangeFile()) {
//                    matchesPerTimestamps = realTgfdDiscovery.getMatchesUsingChangeFiles(patternTreeNode);
//                    matchingTime = System.currentTimeMillis() - matchingTime;
//                    TgfdDiscovery.printWithTime("findMatchesUsingChangeFiles", (matchingTime));
//                    realTgfdDiscovery.addToTotalMatchingTime(matchingTime);
//                }
//                else {
//                    matchesPerTimestamps = realTgfdDiscovery.findMatchesUsingCenterVertices(realTgfdDiscovery.getGraphs(), patternTreeNode);
//                    matchingTime = System.currentTimeMillis() - matchingTime;
//                    TgfdDiscovery.printWithTime("findMatchesUsingCenterVertices", (matchingTime));
//                    realTgfdDiscovery.addToTotalMatchingTime(matchingTime);
//                }
//
//                if (patternTreeNode.getPatternSupport() < realTgfdDiscovery.getPatternTheta()) {
//                    System.out.println("Mark as pruned. Real pattern support too low for pattern " + patternTreeNode.getPattern());
//                    if (realTgfdDiscovery.hasSupportPruning()) patternTreeNode.setIsPruned();
//                    continue;
//                }
//
//                if (realTgfdDiscovery.isSkipK1() && realTgfdDiscovery.getCurrentVSpawnLevel() == 1) continue;
//
//                final long hSpawnStartTime = System.currentTimeMillis();
//                ArrayList<TGFD> tgfds = realTgfdDiscovery.hSpawn(patternTreeNode, matchesPerTimestamps);
//                TgfdDiscovery.printWithTime("hSpawn", (System.currentTimeMillis() - hSpawnStartTime));
//                realTgfdDiscovery.getDiscoveredTgfds().get(realTgfdDiscovery.getCurrentVSpawnLevel()).addAll(tgfds);
//            }
//            realTgfdDiscovery.printTimeStatistics();
//            final long endTime = System.currentTimeMillis() - realTgfdDiscovery.getStartTime();
//            System.out.println("Total execution time: " + (endTime));
//            runtimes.add(endTime);
//        }
//        PrintStream logStream = null;
//        try {
//            logStream = new PrintStream("changefile-test-runtimes-" + overallTimeAndDateStamp + ".txt");
//        } catch (FileNotFoundException e) {
//            e.printStackTrace();
//        }
//        System.setOut(logStream);
//        int i = 1;
//        for (Long runtime: runtimes) {
//            if (i < 5) {
//                printWithTime("Opt" + i, runtime);
//            } else {
//                printWithTime("Naive", runtime);
//            }
//            i++;
//        }
//    }
//}
