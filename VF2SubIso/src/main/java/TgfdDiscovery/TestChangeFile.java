package TgfdDiscovery;

import IncrementalRunner.IncUpdates;
import Infra.ConstantLiteral;
import Infra.PatternTreeNode;
import Infra.TGFD;
import changeExploration.Change;
import changeExploration.ChangeLoader;
import graphLoader.GraphLoader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;

import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

public class TestChangeFile {
    public static void testChangeFile(String[] args) {
        for (int index = 0; index < 3; index++) {
            final long startTime = System.currentTimeMillis();
            Options options = TgfdDiscovery.initializeCmdOptions();
            CommandLine cmd = TgfdDiscovery.parseArgs(options, args);

            String path;
            String graphSize = null;
            if (cmd.hasOption("path")) {
                path = cmd.getOptionValue("path").replaceFirst("^~", System.getProperty("user.home"));
                if (!Files.isDirectory(Path.of(path))) {
                    System.out.println(Path.of(path) + " is not a valid directory.");
                    return;
                }
                graphSize = Path.of(path).getFileName().toString();
            }

            String timeAndDateStamp = ZonedDateTime.now(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern("uuuu.MM.dd.HH.mm.ss"));

            String experimentName;
            if (cmd.hasOption("name")) {
                experimentName = cmd.getOptionValue("name");
            } else {
                experimentName = "experiment";
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

            boolean noMinimalityPruning = cmd.hasOption("noMinimalityPruning");
            boolean noSupportPruning = cmd.hasOption("noSupportPruning");
            boolean dontSortHistogram = cmd.hasOption("dontSortHistogram");
            boolean interestingTGFDs = cmd.hasOption("interesting");
            boolean useChangeFile = false;
            boolean useSubgraph = false;
            if (index == 0) {
                useChangeFile = true;
            } else if (index == 1) {
                useSubgraph = true;
            }
            boolean generatek0Tgfds = cmd.hasOption("k0");
            boolean skipK1 = cmd.hasOption("skipK1");

            int gamma = cmd.getOptionValue("a") == null ? TgfdDiscovery.DEFAULT_GAMMA : Integer.parseInt(cmd.getOptionValue("a"));
            double theta = cmd.getOptionValue("theta") == null ? TgfdDiscovery.DEFAULT_THETA : Double.parseDouble(cmd.getOptionValue("theta"));
            int k = cmd.getOptionValue("k") == null ? TgfdDiscovery.DEFAULT_K : Integer.parseInt(cmd.getOptionValue("k"));
            double patternSupportThreshold = cmd.getOptionValue("p") == null ? TgfdDiscovery.DEFAULT_PATTERN_SUPPORT_THRESHOLD : Double.parseDouble(cmd.getOptionValue("p"));

            TgfdDiscovery tgfdDiscovery = new TgfdDiscovery(experimentName, k, theta, gamma, graphSize, patternSupportThreshold, noMinimalityPruning, interestingTGFDs, useChangeFile, noSupportPruning, dontSortHistogram, useSubgraph, generatek0Tgfds, skipK1);

            ArrayList<GraphLoader> graphs;
            if (!tgfdDiscovery.isUseChangeFile()) {
                tgfdDiscovery.setUseChangeFile(true);
                graphs = tgfdDiscovery.loadDBpediaSnapshotsFromPath(cmd.getOptionValue("path"));
                tgfdDiscovery.setUseChangeFile(false);
            } else {
                graphs = tgfdDiscovery.loadDBpediaSnapshotsFromPath(cmd.getOptionValue("path"));
            }

            tgfdDiscovery.histogram(graphs); // TO-DO: This histogram is only computed on the first snapshot

            if (graphs.size() == 1) {
                for (int i = 0; i < 2; i++) {
                    List<HashMap<Integer, HashSet<Change>>> changes = new ArrayList<>();
                    String changefilePath = "changes_t" + (i + 1) + "_t" + (i + 2) + "_" + tgfdDiscovery.graphSize + ".json";
                    HashMap<String, org.json.simple.JSONArray> changeFilesMap = new HashMap<>();
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
                    ArrayList<TGFD> tgfds = tgfdDiscovery.getDummyTGFDs();
                    IncUpdates incUpdatesOnDBpedia = new IncUpdates(graphs.get(0).getGraph(), tgfds);
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
                    GraphLoader graphLoader = new GraphLoader();
                    graphLoader.setGraph(incUpdatesOnDBpedia.getBaseGraph());
                    graphs.add(graphLoader);
                }
            }

            tgfdDiscovery.setExperimentDateAndTimeStamp(timeAndDateStamp);
            tgfdDiscovery.initialize(graphs);
            if (cmd.hasOption("K")) tgfdDiscovery.markAsKexperiment();
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
                ArrayList<ArrayList<HashSet<ConstantLiteral>>> matches = new ArrayList<>();
                for (int timestamp = 0; timestamp < tgfdDiscovery.getNumOfSnapshots(); timestamp++) {
                    matches.add(new ArrayList<>());
                }
                long matchingTime = System.currentTimeMillis();

                assert patternTreeNode != null;
                if (tgfdDiscovery.isUseSubgraph()) {
                    tgfdDiscovery.getMatchesUsingCenterVertices(graphs, patternTreeNode, matches);
                    matchingTime = System.currentTimeMillis() - matchingTime;
                    TgfdDiscovery.printWithTime("getMatchesUsingCenterVertices", (matchingTime));
                    tgfdDiscovery.addToTotalMatchingTime(matchingTime);
                } else if (tgfdDiscovery.isUseChangeFile()) {
                    tgfdDiscovery.getMatchesUsingChangefiles(graphs, patternTreeNode, matches);
                    TgfdDiscovery.printWithTime("getMatchesUsingChangefiles", (System.currentTimeMillis() - matchingTime));
                } else {
                    // TO-DO: Investigate - why is there a slight discrepancy between the # of matches found via snapshot vs. changefile?
                    // TO-DO: For full-sized dbpedia, can we store the models and create an optimized graph for every search?
                    tgfdDiscovery.getMatchesForPattern(graphs, patternTreeNode, matches); // this can be called repeatedly on many graphs
                    TgfdDiscovery.printWithTime("getMatchesForPattern", (System.currentTimeMillis() - matchingTime));
                }

                if (patternTreeNode.getPatternSupport() < tgfdDiscovery.getTheta()) {
                    System.out.println("Mark as pruned. Real pattern support too low for pattern " + patternTreeNode.getPattern());
                    if (!tgfdDiscovery.hasNoSupportPruning()) patternTreeNode.setIsPruned();
                    continue;
                }

                if (tgfdDiscovery.isSkipK1() && tgfdDiscovery.getCurrentVSpawnLevel() == 1) continue;

                final long hSpawnStartTime = System.currentTimeMillis();
                ArrayList<TGFD> tgfds = tgfdDiscovery.hSpawn(patternTreeNode, matches);
                TgfdDiscovery.printWithTime("hSpawn", (System.currentTimeMillis() - hSpawnStartTime));
                tgfdDiscovery.getTgfds().get(tgfdDiscovery.getCurrentVSpawnLevel()).addAll(tgfds);
            }
            tgfdDiscovery.printTimeStatistics();
            System.out.println("Total execution time: " + (System.currentTimeMillis() - startTime));
        }
    }
}
