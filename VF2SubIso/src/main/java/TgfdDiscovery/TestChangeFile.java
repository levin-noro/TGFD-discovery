package TgfdDiscovery;

import IncrementalRunner.IncUpdates;
import Infra.ConstantLiteral;
import Infra.PatternTreeNode;
import Infra.TGFD;
import changeExploration.Change;
import changeExploration.ChangeLoader;
import graphLoader.DBPediaLoader;
import graphLoader.GraphLoader;
import graphLoader.IMDBLoader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class TestChangeFile extends TgfdDiscovery{
    public TestChangeFile(String[] args) {
        super();
        String timeAndDateStamp = ZonedDateTime.now(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern("uuuu.MM.dd.HH.mm.ss"));
        this.setExperimentDateAndTimeStamp(timeAndDateStamp);
        this.setStartTime(System.currentTimeMillis());

        Options options = TgfdDiscovery.initializeCmdOptions();
        CommandLine cmd = TgfdDiscovery.parseArgs(options, args);

        if (cmd.hasOption("path")) {
            this.setPath(cmd.getOptionValue("path").replaceFirst("^~", System.getProperty("user.home")));
            if (!Files.isDirectory(Path.of(this.getPath()))) {
                System.out.println(Path.of(this.getPath()) + " is not a valid directory.");
                return;
            }
            this.setGraphSize(Path.of(this.getPath()).getFileName().toString());
        }

        if (!cmd.hasOption("loader")) {
            System.out.println("No specifiedLoader is specified.");
            return;
        } else {
            this.setLoader(cmd.getOptionValue("loader"));
        }

        this.setUseChangeFile(this.getLoader().equalsIgnoreCase("imdb"));

        if (cmd.hasOption("name")) {
            this.setExperimentName(cmd.getOptionValue("name"));
        } else  {
            this.setExperimentName("experiment");
        }

        if (!cmd.hasOption("console")) {
            PrintStream logStream = null;
            try {
                logStream = new PrintStream("tgfd-discovery-log-" + timeAndDateStamp + ".txt");
            } catch (FileNotFoundException e) {
                e.printStackTrace();
            }
            System.setOut(logStream);
        }

        this.setOnlyInterestingTGFDs(!cmd.hasOption("uninteresting"));

        this.setGeneratek0Tgfds(cmd.hasOption("k0"));
        this.setSkipK1(cmd.hasOption("skipK1"));

        this.setGamma(cmd.getOptionValue("a") == null ? TgfdDiscovery.DEFAULT_GAMMA : Integer.parseInt(cmd.getOptionValue("a")));
        this.setTgfdTheta(cmd.getOptionValue("theta") == null ? TgfdDiscovery.DEFAULT_TGFD_THETA : Double.parseDouble(cmd.getOptionValue("theta")));
        this.setPatternTheta(cmd.getOptionValue("pTheta") == null ? this.getTgfdTheta() : Double.parseDouble(cmd.getOptionValue("pTheta")));
        this.setK(cmd.getOptionValue("k") == null ? TgfdDiscovery.DEFAULT_K : Integer.parseInt(cmd.getOptionValue("k")));
        this.setFrequentSetSize(cmd.getOptionValue(TgfdDiscovery.FREQUENT_SIZE_SET_PARAM) == null ? TgfdDiscovery.DEFAULT_FREQUENT_SIZE_SET : Integer.parseInt(cmd.getOptionValue(TgfdDiscovery.FREQUENT_SIZE_SET_PARAM)));

        this.initializeTgfdLists();

        if (cmd.hasOption("K")) this.markAsKexperiment();

        if (cmd.hasOption("simplifySuperVertexTypes")) {
            MEDIAN_SUPER_VERTEX_TYPE_INDEGREE_FLOOR = Double.parseDouble(cmd.getOptionValue("simplifySuperVertexTypes"));
            this.setDissolveSuperVertexTypes(true);
        } else if (cmd.hasOption("simplifySuperVertex")) {
            INDIVIDUAL_VERTEX_INDEGREE_FLOOR = Integer.valueOf(cmd.getOptionValue("simplifySuperVertex"));
            this.setDissolveSuperVerticesBasedOnCount(true);
        }

        switch (this.getLoader().toLowerCase()) {
            case "dbpedia" -> this.setDBpediaTimestampsAndFilePaths(this.getPath());
            case "citation" -> this.setCitationTimestampsAndFilePaths();
            case "imdb" -> this.setImdbTimestampToFilesMapFromPath(this.getPath());
            default -> {
                System.out.println("No loader is specified.");
                System.exit(1);
            }
        }
        this.loadGraphsAndComputeHistogram(this.getTimestampToFilesMap().subList(0,1));
//        this.loadChangeFilesIntoMemory();

        this.printInfo();
    }

    public static void main(String[] args) {
        String overallTimeAndDateStamp = ZonedDateTime.now(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern("uuuu.MM.dd.HH.mm.ss"));
        ArrayList<Long> runtimes = new ArrayList<>();
        for (int index = 0; index <= 4; index++) {

            TestChangeFile tgfdDiscovery = new TestChangeFile(args);

            tgfdDiscovery.setUseChangeFile(false);
            tgfdDiscovery.setReUseMatches(false);
            tgfdDiscovery.setSupportPruning(false);
            tgfdDiscovery.setMinimalityPruning(false);
            if (index == 0) {
                tgfdDiscovery.setReUseMatches(true);
            } else if (index == 1) {
                tgfdDiscovery.setUseChangeFile(true);
            } else if (index == 2) {
                tgfdDiscovery.setSupportPruning(true);
            } else if (index == 3) {
                tgfdDiscovery.setMinimalityPruning(true);
            }

            if (tgfdDiscovery.getGraphs().size() == 1) {
                HashMap<String, org.json.simple.JSONArray> changeFilesMap = new HashMap<>();
                for (int i = 1; i < 3; i++) {
                    GraphLoader graph;
                    Model model = ModelFactory.createDefaultModel();
                    for (String path : tgfdDiscovery.getTimestampToFilesMap().get(0).getValue()) {
                        if (path.toLowerCase().contains("literals") || path.toLowerCase().contains("objects") || !path.toLowerCase().contains(".ttl"))
                            continue;
                        Path input = Paths.get(path);
                        model.read(input.toUri().toString());
                    }
                    Model dataModel = ModelFactory.createDefaultModel();
                    for (String path : tgfdDiscovery.getTimestampToFilesMap().get(0).getValue()) {
                        if (path.toLowerCase().contains("types") || !path.toLowerCase().contains(".ttl")) continue;
                        Path input = Paths.get(path);
                        System.out.println("Reading data graph: " + path);
                        dataModel.read(input.toUri().toString());
                    }
                    if (tgfdDiscovery.getLoader().equals("dbpedia")) {
                        graph = new DBPediaLoader(new ArrayList<>(), Collections.singletonList(model), Collections.singletonList(dataModel));
                    } else {
                        graph = new IMDBLoader(new ArrayList<>(), Collections.singletonList(dataModel));
                    }
                    tgfdDiscovery.getGraphs().add(graph);
                    for (int j = 0; j < i; j++) {
                        List<HashMap<Integer, HashSet<Change>>> changes = new ArrayList<>();
                        String changefilePath = "changes_t" + (j+1) + "_t" + (j+2) + "_" + tgfdDiscovery.getGraphSize() + ".json";
                        JSONParser parser = new JSONParser();
                        Object json;
                        org.json.simple.JSONArray jsonArray = new JSONArray();
                        try {
                            json = parser.parse(new FileReader(changefilePath));
                            jsonArray = (org.json.simple.JSONArray) json;
                        } catch (Exception e) {
                            e.printStackTrace();
                        }
                        System.out.println("Storing " + changefilePath + " in memory");
                        changeFilesMap.put(changefilePath, jsonArray);
                        ChangeLoader changeLoader = new ChangeLoader(changeFilesMap.get(changefilePath));
                        HashMap<Integer, HashSet<Change>> newChanges = changeLoader.getAllGroupedChanges();
                        System.out.println("Total number of changes in changefile: " + newChanges.size());
                        changes.add(newChanges);
                        ArrayList<TGFD> tgfds = tgfdDiscovery.getDummyEdgeTypeTGFDs();
                        IncUpdates incUpdatesOnDBpedia = new IncUpdates(graph.getGraph(), tgfds);
                        incUpdatesOnDBpedia.AddNewVertices(changeLoader.getAllChanges());
                        HashMap<String, TGFD> tgfdsByName = new HashMap<>();
                        for (TGFD tgfd : tgfds) {
                            tgfdsByName.put(tgfd.getName(), tgfd);
                        }
                        for (HashMap<Integer, HashSet<Change>> changesByFile : changes) {
                            for (int changeID : changesByFile.keySet()) {
                                incUpdatesOnDBpedia.updateGraphByGroupOfChanges(changesByFile.get(changeID), tgfdsByName);
                            }
                        }
                    }
                }
                tgfdDiscovery.setChangeFilesMap(changeFilesMap);
            }
            tgfdDiscovery.setNumOfSnapshots(tgfdDiscovery.getGraphs().size());

            tgfdDiscovery.initialize(tgfdDiscovery.getGraphs());

            while (tgfdDiscovery.getCurrentVSpawnLevel() <= tgfdDiscovery.getK()) {

                System.out.println("VSpawn level " + tgfdDiscovery.getCurrentVSpawnLevel());
                System.out.println("Previous level node index " + tgfdDiscovery.getPreviousLevelNodeIndex());
                System.out.println("Candidate edge index " + tgfdDiscovery.getCandidateEdgeIndex());

                PatternTreeNode patternTreeNode = null;
                long vSpawnTime = System.currentTimeMillis();
                while (patternTreeNode == null && tgfdDiscovery.getCurrentVSpawnLevel() <= tgfdDiscovery.getK()) {
                    patternTreeNode = tgfdDiscovery.vSpawn();
                }
                vSpawnTime = System.currentTimeMillis() - vSpawnTime;
                TgfdDiscovery.printWithTime("vSpawn", vSpawnTime);
                tgfdDiscovery.addToTotalVSpawnTime(vSpawnTime);
                if (tgfdDiscovery.getCurrentVSpawnLevel() > tgfdDiscovery.getK()) break;
                long matchingTime = System.currentTimeMillis();

                assert patternTreeNode != null;
                List<Set<Set<ConstantLiteral>>> matchesPerTimestamps;
                if (tgfdDiscovery.isValidationSearch()) {
                    matchesPerTimestamps = tgfdDiscovery.getMatchesForPattern(tgfdDiscovery.getGraphs(), patternTreeNode);
                    matchingTime = System.currentTimeMillis() - matchingTime;
                    TgfdDiscovery.printWithTime("findMatchesUsingChangeFiles", (matchingTime));
                    tgfdDiscovery.addToTotalMatchingTime(matchingTime);
                }
                else if (tgfdDiscovery.useChangeFile()) {
                    matchesPerTimestamps = tgfdDiscovery.getMatchesUsingChangeFiles(patternTreeNode);
                    matchingTime = System.currentTimeMillis() - matchingTime;
                    TgfdDiscovery.printWithTime("findMatchesUsingChangeFiles", (matchingTime));
                    tgfdDiscovery.addToTotalMatchingTime(matchingTime);
                }
                else {
                    matchesPerTimestamps = tgfdDiscovery.findMatchesUsingCenterVertices(tgfdDiscovery.getGraphs(), patternTreeNode);
                    matchingTime = System.currentTimeMillis() - matchingTime;
                    TgfdDiscovery.printWithTime("findMatchesUsingCenterVertices", (matchingTime));
                    tgfdDiscovery.addToTotalMatchingTime(matchingTime);
                }

                if (patternTreeNode.getPatternSupport() < tgfdDiscovery.getPatternTheta()) {
                    System.out.println("Mark as pruned. Real pattern support too low for pattern " + patternTreeNode.getPattern());
                    if (tgfdDiscovery.hasSupportPruning()) patternTreeNode.setIsPruned();
                    continue;
                }

                if (tgfdDiscovery.isSkipK1() && tgfdDiscovery.getCurrentVSpawnLevel() == 1) continue;

                final long hSpawnStartTime = System.currentTimeMillis();
                ArrayList<TGFD> tgfds = tgfdDiscovery.hSpawn(patternTreeNode, matchesPerTimestamps);
                TgfdDiscovery.printWithTime("hSpawn", (System.currentTimeMillis() - hSpawnStartTime));
                tgfdDiscovery.getTgfds().get(tgfdDiscovery.getCurrentVSpawnLevel()).addAll(tgfds);
            }
            tgfdDiscovery.printTimeStatistics();
            final long endTime = System.currentTimeMillis() - tgfdDiscovery.getStartTime();
            System.out.println("Total execution time: " + (endTime));
            runtimes.add(endTime);
        }
        PrintStream logStream = null;
        try {
            logStream = new PrintStream("changefile-test-runtimes-" + overallTimeAndDateStamp + ".txt");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        System.setOut(logStream);
        int i = 1;
        for (Long runtime: runtimes) {
            if (i < 5) {
                printWithTime("Opt" + i, runtime);
            } else {
                printWithTime("Naive", runtime);
            }
            i++;
        }
    }
}
