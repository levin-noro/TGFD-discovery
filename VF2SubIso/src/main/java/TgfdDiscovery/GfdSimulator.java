package TgfdDiscovery;

import IncrementalRunner.IncUpdates;
import Infra.*;
import VF2Runner.FastMatching;
import VF2Runner.LocalizedVF2Matching;
import VF2Runner.VF2SubgraphIsomorphism;
import VF2Runner.WindmillMatching;
import changeExploration.ChangeLoader;
import graphLoader.GraphLoader;
import graphLoader.SyntheticLoader;
import org.jetbrains.annotations.NotNull;
import org.jgrapht.alg.isomorphism.VF2AbstractIsomorphismInspector;
import org.json.simple.JSONArray;

import java.io.FileWriter;
import java.io.IOException;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class GfdSimulator extends TgfdDiscovery {
    public GfdSimulator(String[] args) {
        super(args);
    }

    public static void main(String[] args) {
        String overallTimeAndDateStamp = ZonedDateTime.now(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern("uuuu.MM.dd.HH.mm.ss"));
        FileWriter file = null;
        try {
            file = new FileWriter("gfd-discovery-test-runtimes-" + overallTimeAndDateStamp + ".txt");
        } catch (IOException e) {
            e.printStackTrace();
        }

        for (int t = 1; t <= 21; t++) {
                final long startTime = System.currentTimeMillis();
                GfdSimulator gfdDiscovery = new GfdSimulator(args);
                List<String> firstSnapshotPathsEntry = gfdDiscovery.getTimestampToFilesMap().get(0).getValue();
                GraphLoader graph = new SyntheticLoader(firstSnapshotPathsEntry);
                for (int i = 1; i < t; i++) {
                    gfdDiscovery.updateGraphUsingChangefile(graph, i);
                }
                gfdDiscovery.loadGraphsAndComputeHistogram2(graph);
                gfdDiscovery.initialize();
                while (gfdDiscovery.getCurrentVSpawnLevel() <= gfdDiscovery.getK()) {

                    PatternTreeNode patternTreeNode = null;
                    while (patternTreeNode == null && gfdDiscovery.getCurrentVSpawnLevel() <= gfdDiscovery.getK())
                        patternTreeNode = gfdDiscovery.vSpawn();

                    if (gfdDiscovery.getCurrentVSpawnLevel() > gfdDiscovery.getK())
                        break;

                    if (patternTreeNode == null)
                        throw new NullPointerException("patternTreeNode == null");

                    List<Set<Set<ConstantLiteral>>> matchesPerTimestamps;
                    long matchingTime = System.currentTimeMillis();
                    if (gfdDiscovery.isValidationSearch())
                        matchesPerTimestamps = gfdDiscovery.getMatchesForPatternUsingVF2(patternTreeNode);
                    else if (gfdDiscovery.useChangeFile())
                        matchesPerTimestamps = gfdDiscovery.getMatchesUsingChangeFiles3(patternTreeNode);
                    else
                        matchesPerTimestamps = gfdDiscovery.findMatchesUsingCenterVertices2(gfdDiscovery.getGraphs(), patternTreeNode);

                    matchingTime = System.currentTimeMillis() - matchingTime;
                    TgfdDiscovery.printWithTime("Pattern matching", (matchingTime));
                    gfdDiscovery.addToTotalMatchingTime(matchingTime);

                    double S = gfdDiscovery.getVertexHistogram().get(patternTreeNode.getPattern().getCenterVertexType());
                    double patternSupport = GfdSimulator.calculatePatternSupport(patternTreeNode.getEntityURIs(), S, gfdDiscovery.getT());
                    gfdDiscovery.patternSupportsListForThisSnapshot.add(patternSupport);
                    patternTreeNode.setPatternSupport(patternSupport);

                    if (gfdDiscovery.doesNotSatisfyTheta(patternTreeNode)) {
                        System.out.println("Mark as pruned. Real pattern support too low for pattern " + patternTreeNode.getPattern());
                        if (gfdDiscovery.hasSupportPruning())
                            patternTreeNode.setIsPruned();
                        continue;
                    }

                    if (gfdDiscovery.isSkipK1() && gfdDiscovery.getCurrentVSpawnLevel() == 1)
                        continue;

                    final long hSpawnStartTime = System.currentTimeMillis();
                    ArrayList<TGFD> tgfds = gfdDiscovery.hSpawn(patternTreeNode, matchesPerTimestamps);
                    TgfdDiscovery.printWithTime("hSpawn", (System.currentTimeMillis() - hSpawnStartTime));
                    gfdDiscovery.getDiscoveredTgfds().get(gfdDiscovery.getCurrentVSpawnLevel()).addAll(tgfds);
                }
                System.out.println("---------------------------------------------------------------");
                System.out.println("                          Summary                              ");
                System.out.println("---------------------------------------------------------------");
                for (int level = 0; level <= gfdDiscovery.getK(); level++) {
                    gfdDiscovery.printSupportStatisticsForThisSnapshot(level);
                    gfdDiscovery.printTimeStatisticsForThisSnapshot(level);
                }
                gfdDiscovery.printTimeStatistics();
                final long endTime = System.currentTimeMillis() - startTime;
                System.out.println("Total execution time: " + endTime);

                long minutes = (endTime / 1000)  / 60;
                int seconds = (int)((endTime / 1000) % 60);
            try {
                file.write("t = "+t+", "+minutes+seconds+"\n");
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        try {
            file.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public void initialize() {

        vSpawnInit();

        if (this.isGeneratek0Tgfds()) {
            this.printTgfdsToFile(this.getExperimentName(), this.getDiscoveredTgfds().get(this.getCurrentVSpawnLevel()));
        }
        this.getkRuntimes().add(System.currentTimeMillis() - this.getDiscoveryStartTime());
        this.printSupportStatisticsForThisSnapshot();
        this.printTimeStatisticsForThisSnapshot(this.getCurrentVSpawnLevel());
        this.patternTree.addLevel();
        this.setCurrentVSpawnLevel(this.getCurrentVSpawnLevel() + 1);
    }

    public void vSpawnInit() {
        this.patternTree = new PatternTree();
        this.patternTree.addLevel();

        System.out.println("VSpawn Level 0");
        for (int i = 0; i < this.getSortedVertexHistogram().size(); i++) {
            long vSpawnTime = System.currentTimeMillis();
            System.out.println("VSpawnInit with single-node pattern " + (i+1) + "/" + this.getSortedVertexHistogram().size());
            String patternVertexType = this.getSortedVertexHistogram().get(i).getKey();

            if (this.getVertexTypesToActiveAttributesMap().get(patternVertexType).size() == 0)
                continue; // TODO: Should these frequent types without active attribute be filtered out much earlier?

//            int numOfInstancesOfVertexType = this.getSortedVertexHistogram().get(i).getValue();
//            int numOfInstancesOfAllVertexTypes = this.getNumOfVerticesInAllGraphs();

//			double frequency = (double) numOfInstancesOfVertexType / (double) numOfInstancesOfAllVertexTypes;
//			System.out.println("Frequency of vertex type: " + numOfInstancesOfVertexType + " / " + numOfInstancesOfAllVertexTypes + " = " + frequency);

            System.out.println("Vertex type: "+patternVertexType);
            VF2PatternGraph candidatePattern = new VF2PatternGraph();
            PatternVertex patternVertex = new PatternVertex(patternVertexType);
            candidatePattern.addVertex(patternVertex);
            candidatePattern.getCenterVertexType();
            System.out.println("VSpawnInit with single-node pattern " + (i+1) + "/" + this.getSortedVertexHistogram().size() + ": " + candidatePattern.getPattern().vertexSet());

            PatternTreeNode patternTreeNode;
            patternTreeNode = this.patternTree.createNodeAtLevel(this.getCurrentVSpawnLevel(), candidatePattern);

            final long finalVspawnTime = System.currentTimeMillis() - vSpawnTime;
            this.addToTotalVSpawnTime(finalVspawnTime);
            TgfdDiscovery.printWithTime("vSpawn", finalVspawnTime);

            final long matchingStartTime = System.currentTimeMillis();
            if (!this.isGeneratek0Tgfds()) {
                if (this.isValidationSearch()) {
                    this.getMatchesForPatternUsingVF2(patternTreeNode);
                }
                else if (this.useChangeFile()) {
                    this.getMatchesUsingChangeFiles3(patternTreeNode);
                }
                else {
                    // TODO: Implement pattern support calculation here using entityURIs?
                    Map<String, List<Integer>> entityURIs = new HashMap<>();
//					ArrayList<ArrayList<DataVertex>> matchesOfThisCenterVertexPerTimestamp = extractListOfCenterVerticesAcrossAllSnapshots(graphs, patternVertexType, entityURIs);
//					patternTreeNode.setListOfCenterVertices(matchesOfThisCenterVertexPerTimestamp);
                    LocalizedVF2Matching localizedVF2Matching = new LocalizedVF2Matching(patternTreeNode.getPattern(), patternTreeNode.getCenterVertexParent(), this.getT(), this.isOnlyInterestingTGFDs(), this.getVertexTypesToActiveAttributesMap(), this.reUseMatches());
                    for (int t = 0; t < this.getT(); t++)
                        localizedVF2Matching.extractListOfCenterVerticesInSnapshot(patternTreeNode.getPattern().getCenterVertexType(), entityURIs, t, this.getGraphs().get(t));

                    System.out.println("Number of center vertex URIs found containing active attributes: " + entityURIs.size());
                    for (Map.Entry<String, List<Integer>> entry: entityURIs.entrySet()) {
                        System.out.println(entry);
                    }
                    if (this.reUseMatches())
                        patternTreeNode.setEntityURIs(entityURIs);
                    double S = this.getVertexHistogram().get(patternTreeNode.getPattern().getCenterVertexType());
                    double patternSupport = GfdSimulator.calculatePatternSupport(entityURIs, S, this.getT());
                    this.patternSupportsListForThisSnapshot.add(patternSupport);
                    patternTreeNode.setPatternSupport(patternSupport);
//					int numOfMatches = 0;
//					for (ArrayList<DataVertex> matchesOfThisCenterVertex : matchesOfThisCenterVertexPerTimestamp) {
//						numOfMatches += matchesOfThisCenterVertex.size();
//					}
//					System.out.println("Number of center vertex matches found containing active attributes: " + numOfMatches);
                }
                final long matchingEndTime = System.currentTimeMillis() - matchingStartTime;
                printWithTime("matchingTime", matchingEndTime);
                this.addToTotalMatchingTime(matchingEndTime);

                if (doesNotSatisfyTheta(patternTreeNode)) {
                    patternTreeNode.setIsPruned();
                }
            } else {
                List<Set<Set<ConstantLiteral>>> matchesPerTimestamps;
                if (this.isValidationSearch()) {
                    matchesPerTimestamps = this.getMatchesForPatternUsingVF2(patternTreeNode);
                }
                else if (this.useChangeFile()) {
                    matchesPerTimestamps = this.getMatchesUsingChangeFiles3(patternTreeNode);
                }
                else {
//					ArrayList<ArrayList<DataVertex>> matchesOfThisCenterVertexPerTimestamp = new ArrayList<>();
//					Map<String, List<Integer>> entityURIs = new HashMap<>();
//					findMatchesUsingCenterVerticesForVSpawnInit(this.getGraphs(), patternVertexType, patternTreeNode, matchesPerTimestamps, matchesOfThisCenterVertexPerTimestamp, entityURIs);
//					double patternSupport = this.calculatePatternSupport(entityURIs, patternTreeNode);
//					patternTreeNode.setPatternSupport(patternSupport);

                    LocalizedVF2Matching localizedVF2Matching;
                    if (this.isFastMatching())
                        if (patternTreeNode.getPattern().getPatternType() == PatternType.Windmill)
                            localizedVF2Matching = new WindmillMatching(patternTreeNode.getPattern(), patternTreeNode.getCenterVertexParent(), this.getT(), this.isOnlyInterestingTGFDs(), this.getVertexTypesToActiveAttributesMap(), this.reUseMatches());
                        else
                            localizedVF2Matching = new FastMatching(patternTreeNode.getPattern(), patternTreeNode.getCenterVertexParent(), this.getT(), this.isOnlyInterestingTGFDs(), this.getVertexTypesToActiveAttributesMap(), this.reUseMatches());
                    else
                        localizedVF2Matching = new LocalizedVF2Matching(patternTreeNode.getPattern(), patternTreeNode.getCenterVertexParent(), this.getT(), this.isOnlyInterestingTGFDs(), this.getVertexTypesToActiveAttributesMap(), this.reUseMatches());

                    localizedVF2Matching.findMatches(this.getGraphs(), this.getT());

                    Map<String, List<Integer>> entityURIs = localizedVF2Matching.getEntityURIs();
                    localizedVF2Matching.printEntityURIs();
                    if (this.reUseMatches())
                        patternTreeNode.setEntityURIs(entityURIs);

                    double S = this.getVertexHistogram().get(patternTreeNode.getPattern().getCenterVertexType());
                    double patternSupport = GfdSimulator.calculatePatternSupport(entityURIs, S, this.getT());
                    this.patternSupportsListForThisSnapshot.add(patternSupport);
                    patternTreeNode.setPatternSupport(patternSupport);
                    matchesPerTimestamps = localizedVF2Matching.getMatchesPerTimestamp();
                }

                final long matchingEndTime = System.currentTimeMillis() - matchingStartTime;
                printWithTime("matchingTime", matchingEndTime);
                this.addToTotalMatchingTime(matchingEndTime);

                if (doesNotSatisfyTheta(patternTreeNode)) {
                    patternTreeNode.setIsPruned();
                } else {
                    final long hSpawnStartTime = System.currentTimeMillis();
                    ArrayList<TGFD> tgfds = this.hSpawn(patternTreeNode, matchesPerTimestamps);
                    printWithTime("hSpawn", (System.currentTimeMillis() - hSpawnStartTime));
                    this.getDiscoveredTgfds().get(0).addAll(tgfds);
                }
            }
        }
        System.out.println("GenTree Level " + this.getCurrentVSpawnLevel() + " size: " + this.patternTree.getLevel(this.getCurrentVSpawnLevel()).size());
        for (PatternTreeNode node : this.patternTree.getLevel(this.getCurrentVSpawnLevel())) {
            System.out.println("Pattern: " + node.getPattern());
//			System.out.println("Pattern Support: " + node.getPatternSupport());
//			System.out.println("Dependency: " + node.getDependenciesSets());
        }

    }


    public static double calculatePatternSupport(Map<String, List<Integer>> entityURIs, double S, int T) {
        return calculateSupport(entityURIs.size(), S, T);
    }

    public List<Set<Set<ConstantLiteral>>> findMatchesUsingCenterVertices2(List<GraphLoader> graphs, PatternTreeNode patternTreeNode) {

        LocalizedVF2Matching localizedVF2Matching;
        if (this.isFastMatching())
            if (patternTreeNode.getPattern().getPatternType() == PatternType.Windmill)
                localizedVF2Matching = new WindmillMatching(patternTreeNode.getPattern(), patternTreeNode.getCenterVertexParent(), this.getT(), this.isOnlyInterestingTGFDs(), this.getVertexTypesToActiveAttributesMap(), this.reUseMatches());
            else
                localizedVF2Matching = new FastMatching(patternTreeNode.getPattern(), patternTreeNode.getCenterVertexParent(), this.getT(), this.isOnlyInterestingTGFDs(), this.getVertexTypesToActiveAttributesMap(), this.reUseMatches());
        else
            localizedVF2Matching = new LocalizedVF2Matching(patternTreeNode.getPattern(), patternTreeNode.getCenterVertexParent(), this.getT(), this.isOnlyInterestingTGFDs(), this.getVertexTypesToActiveAttributesMap(), this.reUseMatches());

        localizedVF2Matching.findMatches(graphs, this.getT());

        List<Set<Set<ConstantLiteral>>> matchesPerTimestamp = localizedVF2Matching.getMatchesPerTimestamp();
        this.countTotalNumberOfMatchesFound(matchesPerTimestamp);

        Map<String, List<Integer>> entityURIs = localizedVF2Matching.getEntityURIs();
        if (this.reUseMatches())
            patternTreeNode.setEntityURIs(entityURIs);

        localizedVF2Matching.printEntityURIs();

        return matchesPerTimestamp;
    }

    public List<Set<Set<ConstantLiteral>>> getMatchesForPatternUsingVF2(PatternTreeNode patternTreeNode) {
        // TODO: Potential speed up for single-edge/single-node patterns. Iterate through all edges/nodes in graph.
        Map<String, List<Integer>> entityURIs = new HashMap<>();
        List<Set<Set<ConstantLiteral>>> matchesPerTimestamps = new ArrayList<>();
        for (int timestamp = 0; timestamp < this.getNumOfSnapshots(); timestamp++) {
            matchesPerTimestamps.add(new HashSet<>());
        }

        patternTreeNode.getPattern().getCenterVertexType();

        for (int year = 0; year < this.getNumOfSnapshots(); year++) {
            long searchStartTime = System.currentTimeMillis();
            ArrayList<HashSet<ConstantLiteral>> matches = new ArrayList<>();
            int numOfMatchesInTimestamp = 0;
            VF2AbstractIsomorphismInspector<Vertex, RelationshipEdge> results = new VF2SubgraphIsomorphism().execute2(this.getGraphs().get(year).getGraph(), patternTreeNode.getPattern(), false);
            if (results.isomorphismExists()) {
                numOfMatchesInTimestamp = extractMatches(results.getMappings(), matches, patternTreeNode, entityURIs, year);
            }
            System.out.println("Number of matches found: " + numOfMatchesInTimestamp);
            System.out.println("Number of matches found that contain active attributes: " + matches.size());
            matchesPerTimestamps.get(year).addAll(matches);
            printWithTime("Search Cost", (System.currentTimeMillis() - searchStartTime));
        }

        // TODO: Should we implement pattern support here to weed out patterns with few matches in later iterations?
        // Is there an ideal pattern support threshold after which very few TGFDs are discovered?
        // How much does the real pattern differ from the estimate?
        int numberOfMatchesFound = 0;
        for (Set<Set<ConstantLiteral>> matchesInOneTimestamp : matchesPerTimestamps) {
            numberOfMatchesFound += matchesInOneTimestamp.size();
        }
        System.out.println("Total number of matches found across all snapshots:" + numberOfMatchesFound);

        for (Map.Entry<String, List<Integer>> entry: entityURIs.entrySet()) {
            System.out.println(entry);
        }

        return matchesPerTimestamps;
    }


    public List<Set<Set<ConstantLiteral>>> getMatchesUsingChangeFiles3(PatternTreeNode patternTreeNode) {
        LocalizedVF2Matching localizedVF2Matching;
        if (this.isFastMatching())
            if (patternTreeNode.getPattern().getPatternType() == PatternType.Windmill)
                localizedVF2Matching = new WindmillMatching(patternTreeNode.getPattern(), patternTreeNode.getCenterVertexParent(), this.getT(), this.isOnlyInterestingTGFDs(), this.getVertexTypesToActiveAttributesMap(), this.reUseMatches());
            else
                localizedVF2Matching = new FastMatching(patternTreeNode.getPattern(), patternTreeNode.getCenterVertexParent(), this.getT(), this.isOnlyInterestingTGFDs(), this.getVertexTypesToActiveAttributesMap(), this.reUseMatches());
        else
            localizedVF2Matching = new LocalizedVF2Matching(patternTreeNode.getPattern(), patternTreeNode.getCenterVertexParent(), this.getT(), this.isOnlyInterestingTGFDs(), this.getVertexTypesToActiveAttributesMap(), this.reUseMatches());

        GraphLoader graph = this.loadFirstSnapshot();
        localizedVF2Matching.findMatchesInSnapshot(graph, 0);
        for (int t = 1; t < this.getT(); t++) {
            System.out.println("-----------Snapshot (" + (t + 1) + ")-----------");
            String changeFilePath = "changes_t" + t + "_t" + (t + 1) + "_" + this.getGraphSize() + ".json";
            JSONArray jsonArray = this.isStoreInMemory() ? this.getChangeFilesMap().get(changeFilePath) : readJsonArrayFromFile(changeFilePath);
            ChangeLoader changeLoader = new ChangeLoader(jsonArray, true);
            IncUpdates incUpdatesOnDBpedia = new IncUpdates(graph.getGraph(), new ArrayList<>());
            sortChanges(changeLoader.getAllChanges());
            incUpdatesOnDBpedia.updateEntireGraph(changeLoader.getAllChanges());
            localizedVF2Matching.findMatchesInSnapshot(graph, t);
        }
        Map<String, List<Integer>> entityURIs = localizedVF2Matching.getEntityURIs();
        if (this.reUseMatches())
            patternTreeNode.setEntityURIs(entityURIs);

        System.out.println("-------------------------------------");
        System.out.println("Number of entity URIs found: "+entityURIs.size());
        localizedVF2Matching.printEntityURIs();

        return localizedVF2Matching.getMatchesPerTimestamp();
    }


    private ArrayList<TGFD> hSpawn(PatternTreeNode patternTreeNode, List<Set<Set<ConstantLiteral>>> matchesPerTimestamps) {
        ArrayList<TGFD> tgfds = new ArrayList<>();

        System.out.println("Performing HSpawn for " + patternTreeNode.getPattern());

        List<ConstantLiteral> activeAttributesInPattern = new ArrayList<>(getActiveAttributesInPattern(patternTreeNode.getGraph().vertexSet(), false));

        LiteralTree literalTree = new LiteralTree();
        int hSpawnLimit;
        if (this.isOnlyInterestingTGFDs()) {
            hSpawnLimit = Math.max(patternTreeNode.getGraph().vertexSet().size(), this.getMaxNumOfLiterals());
        } else {
            hSpawnLimit = this.getMaxNumOfLiterals();
        }
        for (int j = 0; j < hSpawnLimit; j++) {

            System.out.println("HSpawn level " + j + "/" + (hSpawnLimit-1));

            if (j == 0) {
                literalTree.addLevel();
                for (int index = 0; index < activeAttributesInPattern.size(); index++) {
                    ConstantLiteral literal = activeAttributesInPattern.get(index);
                    literalTree.createNodeAtLevel(j, literal, null);
                    System.out.println("Created root "+index+"/"+activeAttributesInPattern.size()+" of literal forest.");
                }
            } else {
                ArrayList<LiteralTreeNode> literalTreePreviousLevel = literalTree.getLevel(j - 1);
                if (literalTreePreviousLevel.size() == 0) {
                    System.out.println("Previous level of literal tree is empty. Nothing to expand. End HSpawn");
                    break;
                }
                literalTree.addLevel();
                HashSet<AttributeDependency> visitedPaths = new HashSet<>(); //TODO: Can this be implemented as HashSet to improve performance?
                ArrayList<TGFD> currentLevelTGFDs = new ArrayList<>();
                for (int literalTreePreviousLevelIndex = 0; literalTreePreviousLevelIndex < literalTreePreviousLevel.size(); literalTreePreviousLevelIndex++) {
                    System.out.println("Expanding previous level literal tree path " + (literalTreePreviousLevelIndex+1) + "/" + literalTreePreviousLevel.size()+"...");

                    LiteralTreeNode previousLevelLiteral = literalTreePreviousLevel.get(literalTreePreviousLevelIndex);
                    ArrayList<ConstantLiteral> parentsPathToRoot = previousLevelLiteral.getPathToRoot(); //TODO: Can this be implemented as HashSet to improve performance?
                    System.out.println("Literal path: "+parentsPathToRoot);

                    if (previousLevelLiteral.isPruned()) {
                        System.out.println("Could not expand pruned literal path.");
                        continue;
                    }
                    for (int index = 0; index < activeAttributesInPattern.size(); index++) {
                        ConstantLiteral literal = activeAttributesInPattern.get(index);
                        System.out.println("Adding active attribute "+(index+1)+"/"+activeAttributesInPattern.size()+" to path...");
                        System.out.println("Literal: "+literal);
                        if (this.isOnlyInterestingTGFDs() && j < patternTreeNode.getGraph().vertexSet().size()) { // Ensures all vertices are involved in dependency
                            if (isUsedVertexType(literal.getVertexType(), parentsPathToRoot)) continue;
                        }

                        if (parentsPathToRoot.contains(literal)) {
                            System.out.println("Skip. Literal already exists in path.");
                            continue;
                        }

                        // Check if path to candidate leaf node is unique
                        AttributeDependency newPath = new AttributeDependency(parentsPathToRoot,literal);
                        System.out.println("New candidate literal path: " + newPath);

                        long visitedPathCheckingTime = System.currentTimeMillis();
                        boolean isVistedPath = false;
                        if (visitedPaths.contains(newPath)) { // TODO: Is this relevant anymore?
                            System.out.println("Skip. Duplicate literal path.");
                            isVistedPath = true;
                        }
                        visitedPathCheckingTime = System.currentTimeMillis() - visitedPathCheckingTime;
                        printWithTime("visitedPathChecking", visitedPathCheckingTime);
                        addToTotalVisitedPathCheckingTime(visitedPathCheckingTime);

                        if (isVistedPath)
                            continue;

                        long supersetPathCheckingTime = System.currentTimeMillis();
                        boolean isSuperSetPath = false;
                        if (this.hasSupportPruning() && newPath.isSuperSetOfPath(patternTreeNode.getZeroEntityDependenciesOnThisPath())) { // Ensures we don't re-explore dependencies whose subsets have no entities
                            System.out.println("Skip. Candidate literal path is a superset of zero-entity dependency.");
                            isSuperSetPath = true;
                        }
                        else if (this.hasSupportPruning() && newPath.isSuperSetOfPath(patternTreeNode.getLowSupportDependenciesOnThisPath())) { // Ensures we don't re-explore dependencies whose subsets have no entities
                            System.out.println("Skip. Candidate literal path is a superset of low-support dependency.");
                            isSuperSetPath = true;
                        }
                        else if (this.hasMinimalityPruning() && newPath.isSuperSetOfPath(patternTreeNode.getAllMinimalDependenciesOnThisPath())) { // Ensures we don't re-explore dependencies whose subsets have already have a general dependency
                            System.out.println("Skip. Candidate literal path is a superset of minimal dependency.");
                            isSuperSetPath = true;
                        }
                        supersetPathCheckingTime = System.currentTimeMillis()-supersetPathCheckingTime;
                        printWithTime("supersetPathCheckingTime", supersetPathCheckingTime);
                        addToTotalSupersetPathCheckingTime(supersetPathCheckingTime);

                        if (isSuperSetPath)
                            continue;

                        // Add leaf node to tree
                        LiteralTreeNode literalTreeNode = literalTree.createNodeAtLevel(j, literal, previousLevelLiteral);
                        System.out.println("Added candidate literal path to tree.");

                        visitedPaths.add(newPath);

                        if (this.isOnlyInterestingTGFDs()) { // Ensures all vertices are involved in dependency
                            if (literalPathIsMissingTypesInPattern(literalTreeNode.getPathToRoot(), patternTreeNode.getGraph().vertexSet())) {
                                System.out.println("Skip Delta Discovery. Literal path does not involve all pattern vertices.");
                                continue;
                            }
                        }

                        System.out.println("Performing Delta Discovery at HSpawn level " + j);
                        final long deltaDiscoveryTime = System.currentTimeMillis();
                        ArrayList<TGFD> discoveredTGFDs = deltaDiscovery(patternTreeNode, literalTreeNode, newPath, matchesPerTimestamps);
                        printWithTime("deltaDiscovery", System.currentTimeMillis()-deltaDiscoveryTime);
                        currentLevelTGFDs.addAll(discoveredTGFDs);
                    }
                }
                System.out.println("TGFDs generated at HSpawn level " + j + ": " + currentLevelTGFDs.size());
                if (currentLevelTGFDs.size() > 0) {
                    tgfds.addAll(currentLevelTGFDs);
                }
            }
            System.out.println("Generated new literal tree nodes: "+ literalTree.getLevel(j).size());
        }
        System.out.println("For pattern " + patternTreeNode.getPattern());
        System.out.println("HSpawn TGFD count: " + tgfds.size());
        return tgfds;
    }

    public ArrayList<TGFD> deltaDiscovery(PatternTreeNode patternNode, LiteralTreeNode literalTreeNode, AttributeDependency literalPath, List<Set<Set<ConstantLiteral>>> matchesPerTimestamps) {
        ArrayList<TGFD> tgfds = new ArrayList<>();

        // Add dependency attributes to pattern
        // TODO: Fix - when multiple vertices in a pattern have the same type, attribute values get overwritten
        VF2PatternGraph patternForDependency = patternNode.getPattern().copy();
        Set<ConstantLiteral> attributesSetForDependency = new HashSet<>(literalPath.getLhs());
        attributesSetForDependency.add(literalPath.getRhs());
        for (Vertex v : patternForDependency.getGraph().vertexSet()) {
            String vType = new ArrayList<>(v.getTypes()).get(0);
            for (ConstantLiteral attribute : attributesSetForDependency) {
                if (vType.equals(attribute.getVertexType())) {
                    v.putAttributeIfAbsent(new Attribute(attribute.getAttrName()));
                }
            }
        }

        System.out.println("Pattern: " + patternForDependency);
        System.out.println("Dependency: " + "\n\tY=" + literalPath.getRhs() + ",\n\tX={" + literalPath.getLhs() + "\n\t}");

        System.out.println("Performing Entity Discovery");

        // Discover entities
        long findEntitiesTime = System.currentTimeMillis();
        Map<Set<ConstantLiteral>, ArrayList<Map.Entry<ConstantLiteral, List<Integer>>>> entities = findEntities(literalPath, matchesPerTimestamps);
        findEntitiesTime = System.currentTimeMillis() - findEntitiesTime;
        printWithTime("findEntitiesTime", findEntitiesTime);
        addToTotalFindEntitiesTime(findEntitiesTime);
        if (entities == null) {
            System.out.println("No entities found during entity discovery.");
            if (this.hasSupportPruning()) {
                literalTreeNode.setIsPruned();
                System.out.println("Marked as pruned. Literal path "+literalTreeNode.getPathToRoot());
                patternNode.addZeroEntityDependency(literalPath);
            }
            return tgfds;
        }
        System.out.println("Number of entities discovered: " + entities.size());

        System.out.println("Discovering constant TGFDs");

        // Find Constant TGFDs
//        Map<Pair,ArrayList<TreeSet<Pair>>> deltaToPairsMap = new HashMap<>();
//        ArrayList<TGFD> constantTGFDs = discoverConstantTGFDs(patternNode, literalPath.getRhs(), entities, deltaToPairsMap);
        List<AttributeDependency> constantPaths = new ArrayList<>();
        List<TGFD> constantTGFDs = discoverConstantTGFDs(patternNode, entities, constantPaths);

        // TODO: Try discover general TGFD even if no constant TGFD candidate met support threshold
        System.out.println("Constant TGFDs discovered: " + constantTGFDs.size());
        tgfds.addAll(constantTGFDs);

        System.out.println("Discovering general TGFDs");

        // Find general TGFDs
        if (constantPaths.size() > 0) {
            long discoverGeneralTGFDTime = System.currentTimeMillis();
            Dependency generalDependency = new Dependency();
            for (ConstantLiteral constantLiteral: literalPath.getLhs())
                generalDependency.addLiteralToX(new VariableLiteral(constantLiteral.getVertexType(),constantLiteral.getAttrName()));
            generalDependency.addLiteralToY(new VariableLiteral(literalPath.getRhs().getVertexType(),literalPath.getRhs().getAttrName()));
            double generalSupport = TgfdDiscovery.calculateSupport(constantPaths.size(), entities.size(), this.getT());
            TGFD generalTGFD = new TGFD(patternForDependency, null, generalDependency, generalSupport, patternNode.getPatternSupport(), "");
            discoverGeneralTGFDTime = System.currentTimeMillis() - discoverGeneralTGFDTime;
            printWithTime("discoverGeneralTGFDTime", discoverGeneralTGFDTime);
            addToTotalDiscoverGeneralTGFDTime(discoverGeneralTGFDTime);

            System.out.println("Converted constant TGFDs into a general TGFD.");
            System.out.println(generalTGFD);

            if (this.hasMinimalityPruning()) {
                literalTreeNode.setIsPruned();
                System.out.println("Marked as pruned. Literal path " + literalTreeNode.getPathToRoot());
                patternNode.addMinimalDependency(literalPath);
            }
            tgfds.add(generalTGFD);
        }

        return tgfds;
    }

    @NotNull
    private List<TGFD> discoverConstantTGFDs(PatternTreeNode patternNode, Map<Set<ConstantLiteral>, ArrayList<Map.Entry<ConstantLiteral, List<Integer>>>> entities, List<AttributeDependency> constantPaths) {
        List<TGFD> constantTGFDs = new ArrayList<>();
        long discoverConstantTGFDsTime = System.currentTimeMillis();
        long supersetPathCheckingTimeForThisTGFD = 0;
        for (Map.Entry<Set<ConstantLiteral>, ArrayList<Map.Entry<ConstantLiteral, List<Integer>>>> entityEntry : entities.entrySet()) {

            System.out.println("Candidate RHS values for entity...");
            ArrayList<Map.Entry<ConstantLiteral, List<Integer>>> rhsAttrValuesTimestampsSortedByFreq = entityEntry.getValue();
            for (Map.Entry<ConstantLiteral, List<Integer>> entry : rhsAttrValuesTimestampsSortedByFreq) {
                System.out.println(entry.getKey() + ":" + entry.getValue());
            }

            if (rhsAttrValuesTimestampsSortedByFreq.size() > 1)
                continue;

            VF2PatternGraph newPattern = patternNode.getPattern().copy();
            Dependency newDependency = new Dependency();
            AttributeDependency constantPath = new AttributeDependency();
            for (Vertex v : newPattern.getGraph().vertexSet()) {
                String vType = new ArrayList<>(v.getTypes()).get(0);
                for (ConstantLiteral xLiteral : entityEntry.getKey()) {
                    if (xLiteral.getVertexType().equalsIgnoreCase(vType)) {
                        v.putAttributeIfAbsent(new Attribute(xLiteral.getAttrName(), xLiteral.getAttrValue()));
                        ConstantLiteral newXLiteral = new ConstantLiteral(vType, xLiteral.getAttrName(), xLiteral.getAttrValue());
                        newDependency.addLiteralToX(newXLiteral);
                        constantPath.addToLhs(newXLiteral);
                    }
                }
            }

            ConstantLiteral rhsLiteral = rhsAttrValuesTimestampsSortedByFreq.get(0).getKey();
            System.out.println("Computing candidate delta for RHS value...\n" + rhsLiteral);
            int numOfMatches = rhsAttrValuesTimestampsSortedByFreq.get(0).getValue().get(0);
            ConstantLiteral newY = new ConstantLiteral(rhsLiteral.getVertexType(), rhsLiteral.getAttrName(), rhsLiteral.getAttrValue());
            newDependency.addLiteralToY(newY);
            constantPath.setRhs(newY);
            constantPaths.add(constantPath);

            long supersetPathCheckingTime = System.currentTimeMillis();
            boolean isNotMinimal = false;
            if (this.hasMinimalityPruning() && constantPath.isSuperSetOfPathAndSubsetOfDelta(patternNode.getAllMinimalConstantDependenciesOnThisPath())) { // Ensures we don't expand constant TGFDs from previous iterations
                System.out.println("Candidate constant TGFD " + constantPath + " is a superset of an existing minimal constant TGFD");
                isNotMinimal = true;
            }
            supersetPathCheckingTime = System.currentTimeMillis()-supersetPathCheckingTime;
            supersetPathCheckingTimeForThisTGFD += supersetPathCheckingTime;
            printWithTime("supersetPathCheckingTime", supersetPathCheckingTime);
            addToTotalSupersetPathCheckingTime(supersetPathCheckingTime);

            if (isNotMinimal)
                continue;

            double constantTgfdSupport = TgfdDiscovery.calculateSupport(numOfMatches, entities.size(), this.getT());
            this.constantTgfdSupportsListForThisSnapshot.add(constantTgfdSupport); // Statistics
            // Only output constant TGFDs that satisfy support
            if (constantTgfdSupport < this.getTgfdTheta()) {
                if (this.hasSupportPruning())
                    patternNode.addLowSupportDependency(new AttributeDependency(constantPath.getLhs(), constantPath.getRhs(), null));
                System.out.println("Could not satisfy TGFD support threshold for entity: " + entityEntry.getKey());
//				continue;
            } else {
                System.out.println("Creating new constant TGFD...");
                TGFD entityTGFD = new TGFD(newPattern, null, newDependency, constantTgfdSupport, patternNode.getPatternSupport(), "");
                System.out.println("TGFD: " + entityTGFD);
                constantTGFDs.add(entityTGFD);
                if (this.hasMinimalityPruning())
                    patternNode.addMinimalConstantDependency(constantPath);
            }
        }
        discoverConstantTGFDsTime = System.currentTimeMillis() - discoverConstantTGFDsTime - supersetPathCheckingTimeForThisTGFD;
        printWithTime("discoverConstantTGFDsTime", discoverConstantTGFDsTime);
        addToTotalDiscoverConstantTGFDsTime(discoverConstantTGFDsTime);
        return constantTGFDs;
    }


    private void loadGraphsAndComputeHistogram2(GraphLoader graph) {
        System.out.println("Computing Histogram...");
        Histogram histogram = new Histogram(this.getT(), Collections.singletonList(graph), this.getFrequentSetSize(), this.getGamma(), this.getInterestLabelsSet());
        Integer superVertexDegree = this.isDissolveSuperVerticesBasedOnCount() ? INDIVIDUAL_SUPER_VERTEX_INDEGREE_FLOOR : null;
        histogram.computeHistogramUsingGraphs(superVertexDegree);
        this.setGraphs(Collections.singletonList(graph));

        this.setVertexTypesToAvgInDegreeMap(histogram.getVertexTypesToMedianInDegreeMap());

        this.setActiveAttributesSet(histogram.getActiveAttributesSet());
        this.setVertexTypesToActiveAttributesMap(histogram.getVertexTypesToActiveAttributesMap());

        this.setSortedFrequentEdgesHistogram(histogram.getSortedFrequentEdgesHistogram());
        this.setSortedVertexHistogram(histogram.getSortedVertexTypesHistogram());
        this.setVertexHistogram(histogram.getVertexHistogram());

        this.setTotalHistogramTime(histogram.getTotalHistogramTime());
    }
}
