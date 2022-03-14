package TgfdDiscovery;

import IncrementalRunner.IncUpdates;
import IncrementalRunner.IncrementalChange;
import Infra.*;
import VF2Runner.VF2SubgraphIsomorphism;
import changeExploration.*;
import graphLoader.DBPediaLoader;
import graphLoader.GraphLoader;
import graphLoader.IMDBLoader;
import org.apache.commons.cli.*;
import org.apache.commons.math3.util.CombinatoricsUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jgrapht.Graph;
import org.jgrapht.GraphMapping;
import org.jgrapht.alg.isomorphism.VF2AbstractIsomorphismInspector;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.Duration;
import java.time.Period;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class TgfdDiscovery {
	public static final int DEFAULT_NUM_OF_SNAPSHOTS = 3;
	public static final String NO_REUSE_MATCHES_PARAMETER_TEXT = "noReuseMatches";
	public static final String CHANGEFILE_PARAMETER_TEXT = "changefile";
	private static final int DEFAULT_MAX_LITERALS_NUM = 0;
	public static final String FREQUENT_SIZE_SET_PARAM = "f";
	public static final String MAX_LIT_PARAM = "maxLit";
	protected static Integer INDIVIDUAL_VERTEX_INDEGREE_FLOOR = 25;
	public static double MEDIAN_SUPER_VERTEX_TYPE_INDEGREE_FLOOR = 25.0;
	public static final double DEFAULT_MAX_SUPER_VERTEX_DEGREE = 1500.0;
	public static final double DEFAULT_AVG_SUPER_VERTEX_DEGREE = 30.0;
	private boolean fastMatching = false;
	private int maxNumOfLiterals = DEFAULT_MAX_LITERALS_NUM;
	private int t = DEFAULT_NUM_OF_SNAPSHOTS;
	private boolean dissolveSuperVerticesBasedOnCount = false;
	private double superVertexDegree = MEDIAN_SUPER_VERTEX_TYPE_INDEGREE_FLOOR;
	private boolean useTypeChangeFile = false;
	private boolean dissolveSuperVertexTypes = false;
	private boolean validationSearch = false;
	private String path = ".";
	private int numOfSnapshots;
	public static final int DEFAULT_FREQUENT_SIZE_SET = Integer.MAX_VALUE;
	public static final int DEFAULT_GAMMA = 20;
	public static final int DEFAULT_K = 3;
	public static final double DEFAULT_TGFD_THETA = 0.25;
	public static final double DEFAULT_PATTERN_THETA = 0.05;
	private boolean reUseMatches = true;
	private boolean generatek0Tgfds = false;
    private boolean skipK1 = false;
    private Integer numOfEdgesInAllGraphs;
	private int numOfVerticesInAllGraphs;
	public Map<String, HashSet<String>> vertexTypesToAttributesMap; // freq attributes come from here
	public PatternTree patternTree;
	private boolean hasMinimalityPruning = true;
	private String graphSize = null;
	private boolean onlyInterestingTGFDs = true;
	private int k = DEFAULT_K;
	private double tgfdTheta = DEFAULT_TGFD_THETA;
	private double patternTheta = DEFAULT_PATTERN_THETA;
	private int gamma = DEFAULT_GAMMA;
	private int frequentSetSize = DEFAULT_FREQUENT_SIZE_SET;
	private final HashSet<String> activeAttributesSet = new HashSet<>();
	private int previousLevelNodeIndex = 0;
	private int candidateEdgeIndex = 0;
	private int currentVSpawnLevel = 0;
	private ArrayList<ArrayList<TGFD>> tgfds;
	private long startTime;
	private final ArrayList<Long> kRuntimes = new ArrayList<>();
	private String timeAndDateStamp = null;
	private boolean kExperiment = false;
	private boolean useChangeFile = false;
	private List<Entry<String, Integer>> sortedVertexHistogram; // freq nodes come from here
	private final List<Entry<String, Integer>> sortedFrequentEdgesHistogram = new ArrayList<>(); // freq edges come from here
	private final HashMap<String, Integer> vertexHistogram = new HashMap<>();
	private boolean hasSupportPruning = true;
	private final List<Double> medianPatternSupportsList = new ArrayList<>();
	private ArrayList<Double> patternSupportsListForThisSnapshot = new ArrayList<>();
	private final List<Double> medianConstantTgfdSupportsList = new ArrayList<>();
	private ArrayList<Double> constantTgfdSupportsListForThisSnapshot = new ArrayList<>();
	private final List<Double> medianGeneralTgfdSupportsList = new ArrayList<>();
	private ArrayList<Double> generalTgfdSupportsListForThisSnapshot = new ArrayList<>();
	private final ArrayList<Double> vertexFrequenciesList = new ArrayList<>();
	private final ArrayList<Double> edgeFrequenciesList = new ArrayList<>();
	private final List<Long> totalVisitedPathCheckingTime = new ArrayList<>();
	private final List<Long> totalMatchingTime = new ArrayList<>();
	private final List<Long> totalSupersetPathCheckingTime = new ArrayList<>();
	private final List<Long> totalFindEntitiesTime = new ArrayList<>();
	private final List<Long> totalVSpawnTime = new ArrayList<>();
	private final List<Long> totalDiscoverConstantTGFDsTime = new ArrayList<>();
	private final List<Long> totalDiscoverGeneralTGFDTime = new ArrayList<>();
	private String experimentName;
	private String loader;
	private List<Entry<String, List<String>>> timestampToFilesMap = new ArrayList<>();
	private HashMap<String, JSONArray> changeFilesMap;
	private List<GraphLoader> graphs;
	private boolean isStoreInMemory = true;
	private Map<String, Double> vertexTypesToAvgInDegreeMap = new HashMap<>();
	private Model firstSnapshotTypeModel = null;
	private Model firstSnapshotDataModel = null;
	private long totalHistogramTime = 0;
	private final Set<String> interestSet = new HashSet<>();

	public TgfdDiscovery() {
		this.setStartTime(System.currentTimeMillis());

		printInfo();

		this.initializeTgfdLists();
	}

	public TgfdDiscovery(String[] args) {

		String timeAndDateStamp = ZonedDateTime.now(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern("uuuu.MM.dd.HH.mm.ss"));
		this.setExperimentDateAndTimeStamp(timeAndDateStamp);
		this.setStartTime(System.currentTimeMillis());

		Options options = TgfdDiscovery.initializeCmdOptions();
		CommandLine cmd = TgfdDiscovery.parseArgs(options, args);

		if (cmd.hasOption("path")) {
			this.path = cmd.getOptionValue("path").replaceFirst("^~", System.getProperty("user.home"));
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

		// TO-DO: this is useless
//		this.setUseChangeFile(this.loader.equalsIgnoreCase("imdb"));

		this.setStoreInMemory(!cmd.hasOption("dontStore"));

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

		this.setMinimalityPruning(!cmd.hasOption("noMinimalityPruning"));
		this.setSupportPruning(!cmd.hasOption("noSupportPruning"));
		this.setOnlyInterestingTGFDs(!cmd.hasOption("uninteresting"));
		this.setUseChangeFile(false);
		boolean validationSearchTemp = false;
		boolean reUseMatchesTemp = !cmd.hasOption(NO_REUSE_MATCHES_PARAMETER_TEXT);
		if (cmd.hasOption("validation")) {
			validationSearchTemp = true;
			reUseMatchesTemp = false;
		} else if (cmd.hasOption(CHANGEFILE_PARAMETER_TEXT)) {
			this.setUseChangeFile(true);
			reUseMatchesTemp = false;
			if (cmd.getOptionValue(CHANGEFILE_PARAMETER_TEXT).equalsIgnoreCase("type")) {
				this.setUseTypeChangeFile(true);
			}
		}
		this.setReUseMatches(reUseMatchesTemp);
		this.setValidationSearch(validationSearchTemp);

		this.setGeneratek0Tgfds(cmd.hasOption("k0"));
		this.setSkipK1(cmd.hasOption("skipK1"));

		this.setT(cmd.getOptionValue("t") == null ? TgfdDiscovery.DEFAULT_NUM_OF_SNAPSHOTS : Integer.parseInt(cmd.getOptionValue("t")));
		this.setGamma(cmd.getOptionValue("a") == null ? TgfdDiscovery.DEFAULT_GAMMA : Integer.parseInt(cmd.getOptionValue("a")));
		this.setMaxNumOfLiterals(cmd.getOptionValue(MAX_LIT_PARAM) == null ? TgfdDiscovery.DEFAULT_MAX_LITERALS_NUM : Integer.parseInt(cmd.getOptionValue(MAX_LIT_PARAM)));
		this.setTgfdTheta(cmd.getOptionValue("theta") == null ? TgfdDiscovery.DEFAULT_TGFD_THETA : Double.parseDouble(cmd.getOptionValue("theta")));
		this.setPatternTheta(cmd.getOptionValue("pTheta") == null ? this.getTgfdTheta() : Double.parseDouble(cmd.getOptionValue("pTheta")));
		this.setK(cmd.getOptionValue("k") == null ? TgfdDiscovery.DEFAULT_K : Integer.parseInt(cmd.getOptionValue("k")));
		this.setFrequentSetSize(cmd.getOptionValue(FREQUENT_SIZE_SET_PARAM) == null ? TgfdDiscovery.DEFAULT_FREQUENT_SIZE_SET : Integer.parseInt(cmd.getOptionValue(FREQUENT_SIZE_SET_PARAM)));

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
				System.out.println("No valid loader specified.");
				System.exit(1);
			}
		}

		if (cmd.hasOption("interestLabels")) {
			String[] interestLabels = cmd.getOptionValue("interestLabels").split(",");
			this.getInterestSet().addAll(List.of(interestLabels));
		}

		this.loadGraphsAndComputeHistogram(this.getTimestampToFilesMap());

		if (cmd.hasOption("fast")) {
			this.setFastMatching(true);
		}

		printInfo();
	}

	protected void printInfo() {
		String[] info = {
				String.join("=", "loader", this.getGraphSize()),
				String.join("=", "|G|", this.getGraphSize()),
				String.join("=", "t", Integer.toString(this.getT())),
				String.join("=", "k", Integer.toString(this.getK())),
				String.join("=", "pTheta", Double.toString(this.getPatternTheta())),
				String.join("=", "theta", Double.toString(this.getTgfdTheta())),
				String.join("=", "gamma", Double.toString(this.getGamma())),
				String.join("=", "frequentSetSize", Double.toString(this.getFrequentSetSize())),
				String.join("=", "interesting", Boolean.toString(this.isOnlyInterestingTGFDs())),
				String.join("=", "literalMax", Integer.toString(this.getMaxNumOfLiterals())),
				String.join("=", "noMinimalityPruning", Boolean.toString(!this.hasMinimalityPruning())),
				String.join("=", "noSupportPruning", Boolean.toString(!this.hasSupportPruning())),
				String.join("=", "fastMatching", Boolean.toString(this.isFastMatching())),
				String.join("=", "interestLabels", String.join(",", this.getInterestSet())),
		};

		System.out.println(String.join(", ", info));
	}

	public static Options initializeCmdOptions() {
		Options options = new Options();
		options.addOption("name", true, "output files will be given the specified name");
		options.addOption("console", false, "print to console");
		options.addOption("noMinimalityPruning", false, "run algorithm without minimality pruning");
		options.addOption("noSupportPruning", false, "run algorithm without support pruning");
		options.addOption("uninteresting", false, "run algorithm and also consider uninteresting TGFDs");
		options.addOption("g", true, "run experiment on a specific graph size");
		options.addOption("t", true, "run experiment using t number of snapshots");
		options.addOption("k", true, "run experiment for k iterations");
		options.addOption("a", true, "run experiment for specified active attribute set size");
		options.addOption("theta", true, "run experiment using a specific support threshold");
		options.addOption("pTheta", true, "run experiment using a specific pattern support threshold");
		options.addOption("K", false, "run experiment for k = 1 to 5");
		options.addOption(FREQUENT_SIZE_SET_PARAM, true, "run experiment using frequent set of p vertices and p edges");
		options.addOption(CHANGEFILE_PARAMETER_TEXT, true, "run experiment using changefiles instead of snapshots");
		options.addOption(NO_REUSE_MATCHES_PARAMETER_TEXT, false, "run experiment without reusing matches between levels");
		options.addOption("k0", false, "run experiment and generate tgfds for single-node patterns");
		options.addOption("loader", true, "run experiment using specified loader");
		options.addOption("path", true, "path to dataset");
		options.addOption("skipK1", false, "run experiment and generate tgfds for k > 1");
		options.addOption("validation", false, "run experiment to test effectiveness of using changefiles");
		options.addOption("simplifySuperVertex", true, "run experiment by collapsing super vertices");
		options.addOption("simplifySuperVertexTypes", true, "run experiment by collapsing super vertex types");
		options.addOption("dontStore", false, "run experiment without storing changefiles in memory, read from disk");
		options.addOption(MAX_LIT_PARAM, true, "run experiment that outputs TGFDs with up n literals");
		options.addOption("fast", false, "run experiment using fast matching");
		options.addOption("interestLabels", true, "run experiment using frequent sets of vertices and edges that contain labels of interest");
		return options;
	}

	public static CommandLine parseArgs(Options options, String[] args) {
		CommandLineParser parser = new DefaultParser();
		CommandLine cmd = null;
		try {
			cmd = parser.parse(options, args);
		} catch (ParseException e) {
			e.printStackTrace();
		}
		assert cmd != null;
		return cmd;
	}

	public void initialize(List<GraphLoader> graphs) {

		vSpawnInit(graphs);

		if (this.isGeneratek0Tgfds()) {
			this.printTgfdsToFile(this.getExperimentName(), this.getTgfds().get(this.getCurrentVSpawnLevel()));
		}
		this.getkRuntimes().add(System.currentTimeMillis() - this.getStartTime());
		if (this.isGeneratek0Tgfds()) {
			this.printSupportStatisticsForThisSnapshot();
			this.printTimeStatisticsForThisSnapshot(this.getCurrentVSpawnLevel());
		}
		this.patternTree.addLevel();
		this.setCurrentVSpawnLevel(this.getCurrentVSpawnLevel() + 1);
	}

	@Override
	public String toString() {
		String[] info = {"G"+this.getGraphSize()
				, "t" + this.getT()
				, "k" + this.getCurrentVSpawnLevel()
				, "pTheta" + this.getPatternTheta()
				, "theta" + this.getTgfdTheta()
				, "gamma" + this.getGamma()
				, MAX_LIT_PARAM + this.getMaxNumOfLiterals()
				, "freqSet" + (this.getFrequentSetSize() == Integer.MAX_VALUE ? "All" : this.getFrequentSetSize())
				, (this.isFastMatching() ? "fast" : "")
				, (this.isValidationSearch() ? "validation" : "")
				, (this.useChangeFile() ? "changefile"+(this.isUseTypeChangeFile()?"Type":"All") : "")
				, (!this.reUseMatches() ? "noMatchesReUsed" : "")
				, (!this.isOnlyInterestingTGFDs() ? "uninteresting" : "")
				, (!this.hasMinimalityPruning() ? "noMinimalityPruning" : "")
				, (!this.hasSupportPruning() ? "noSupportPruning" : "")
				, (this.isDissolveSuperVertexTypes() ? "simplifySuperTypes"+(this.getSuperVertexDegree()) : "")
				, (this.isDissolveSuperVerticesBasedOnCount() ? "simplifySuperNodes"+(INDIVIDUAL_VERTEX_INDEGREE_FLOOR) : "")
				, (this.getTimeAndDateStamp() == null ? "" : this.getTimeAndDateStamp())
		};
		return String.join("-", info);
	}

	public void printTgfdsToFile(String experimentName, ArrayList<TGFD> tgfds) {
		tgfds.sort(new Comparator<TGFD>() {
			@Override
			public int compare(TGFD o1, TGFD o2) {
				return o2.getSupport().compareTo(o1.getSupport());
			}
		});
		System.out.println("Printing TGFDs to file for k = " + this.getCurrentVSpawnLevel());
		try {
			PrintStream printStream = new PrintStream(experimentName + "-tgfds" + this + ".txt");
			printStream.println("k = " + this.getCurrentVSpawnLevel());
			printStream.println("# of TGFDs generated = " + tgfds.size());
			for (TGFD tgfd : tgfds) {
				printStream.println(tgfd);
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
//		System.gc();
	}

	public void printExperimentRuntimestoFile() {
		try {
			PrintStream printStream = new PrintStream(this.getExperimentName() + "-runtimes-" + this.getTimeAndDateStamp() + ".txt");
			for (int i  = 0; i < this.getkRuntimes().size(); i++) {
				printStream.print("k = " + i);
				printStream.println(", execution time = " + this.getkRuntimes().get(i));
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		final long startTime = System.currentTimeMillis();

		TgfdDiscovery tgfdDiscovery = new TgfdDiscovery(args);

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
			vSpawnTime = System.currentTimeMillis()-vSpawnTime;

			if (tgfdDiscovery.getCurrentVSpawnLevel() > tgfdDiscovery.getK())
				break;

			TgfdDiscovery.printWithTime("vSpawn", vSpawnTime);
			tgfdDiscovery.addToTotalVSpawnTime(vSpawnTime);

			long matchingTime = System.currentTimeMillis();

			assert patternTreeNode != null;
			List<Set<Set<ConstantLiteral>>> matchesPerTimestamps;
			if (tgfdDiscovery.isValidationSearch()) {
				matchesPerTimestamps = tgfdDiscovery.getMatchesForPattern(tgfdDiscovery.getGraphs(), patternTreeNode);
				matchingTime = System.currentTimeMillis() - matchingTime;
				TgfdDiscovery.printWithTime("getMatchesForPattern", (matchingTime));
				tgfdDiscovery.addToTotalMatchingTime(matchingTime);
			}
			else if (tgfdDiscovery.useChangeFile()) {
				matchesPerTimestamps = tgfdDiscovery.getMatchesUsingChangeFiles(patternTreeNode);
				matchingTime = System.currentTimeMillis() - matchingTime;
				TgfdDiscovery.printWithTime("getMatchesUsingChangeFiles", (matchingTime));
				tgfdDiscovery.addToTotalMatchingTime(matchingTime);
			}
			else {
				matchesPerTimestamps = tgfdDiscovery.findMatchesUsingCenterVertices(tgfdDiscovery.getGraphs(), patternTreeNode);
				matchingTime = System.currentTimeMillis() - matchingTime;
				TgfdDiscovery.printWithTime("findMatchesUsingCenterVertices", (matchingTime));
				tgfdDiscovery.addToTotalMatchingTime(matchingTime);
			}

			if (tgfdDiscovery.doesNotSatisfyTheta(patternTreeNode)) {
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
		System.out.println("---------------------------------------------------------------");
		System.out.println("                          Summary                              ");
		System.out.println("---------------------------------------------------------------");
		for (int level = 0; level <= tgfdDiscovery.getK(); level++) {
			tgfdDiscovery.printSupportStatisticsForThisSnapshot(level);
			tgfdDiscovery.printTimeStatisticsForThisSnapshot(level);
		}
		tgfdDiscovery.printTimeStatistics();
		System.out.println("Total execution time: "+(System.currentTimeMillis() - startTime));
	}

	public void printSupportStatisticsForThisSnapshot(int level) {
		System.out.println("----------------Support Statistics for vSpawn level "+level+"-----------------");
		System.out.println("Median Pattern Support: " + this.getMedianPatternSupportsList(level));
		System.out.println("Median Constant TGFD Support: " + this.getMedianConstantTgfdSupportsList(level));
		System.out.println("Median General TGFD Support: " + this.getMedianGeneralTgfdSupportsList(level));
	}

	public void printTimeStatisticsForThisSnapshot(int level) {
		System.out.println("----------------Time Statistics for vSpawn level "+level+"-----------------");
		printWithTime("Total vSpawn", this.getTotalVSpawnTime(level));
		printWithTime("Total Matching", this.getTotalMatchingTime(level));
		printWithTime("Total Visited Path Checking", this.getTotalVisitedPathCheckingTime(level));
		printWithTime("Total Superset Path Checking Time", this.getTotalSupersetPathCheckingTime(level));
		printWithTime("Total Find Entities ", this.getTotalFindEntitiesTime(level));
		printWithTime("Total Discover Constant TGFDs", this.getTotalDiscoverConstantTGFDsTime(level));
		printWithTime("Total Discover General TGFD", this.getTotalDiscoverGeneralTGFDTime(level));
	}

	public void printTimeStatistics() {
		System.out.println("----------------Total Time Statistics-----------------");
		printWithTime("Total Histogram", this.getTotalHistogramTime());
		printWithTime("Total vSpawn", this.getTotalVSpawnTime().stream().reduce(0L, Long::sum));
		printWithTime("Total Matching", this.getTotalMatchingTime().stream().reduce(0L, Long::sum));
		printWithTime("Total Visited Path Checking", this.getTotalVisitedPathCheckingTime().stream().reduce(0L, Long::sum));
		printWithTime("Total Superset Path Checking", this.getTotalSupersetPathCheckingTime().stream().reduce(0L, Long::sum));
		printWithTime("Total Find Entities ", this.getTotalFindEntitiesTime().stream().reduce(0L, Long::sum));
		printWithTime("Total Discover Constant TGFDs", this.getTotalDiscoverConstantTGFDsTime().stream().reduce(0L, Long::sum));
		printWithTime("Total Discover General TGFD", this.getTotalDiscoverGeneralTGFDTime().stream().reduce(0L, Long::sum));
	}

	private int findAllMatchesOfK1patternInSnapshotUsingCenterVertices(PatternTreeNode patternTreeNode, Map<String, List<Integer>> entityURIs, GraphLoader currentSnapshot, Set<Set<ConstantLiteral>> matchesSet, ArrayList<DataVertex> matchesOfCenterVertexInCurrentSnapshot, DataVertex dataVertex, int timestamp) {
		String centerVertexType = patternTreeNode.getPattern().getCenterVertexType();
		Set<String> edgeLabels = patternTreeNode.getGraph().edgeSet().stream().map(RelationshipEdge::getLabel).collect(Collectors.toSet());
		String sourceType = patternTreeNode.getGraph().edgeSet().iterator().next().getSource().getTypes().iterator().next();
		String targetType = patternTreeNode.getGraph().edgeSet().iterator().next().getTarget().getTypes().iterator().next();

		Set<RelationshipEdge> edgeSet;
		if (centerVertexType.equals(sourceType)) {
			edgeSet = currentSnapshot.getGraph().getGraph().outgoingEdgesOf(dataVertex).stream().filter(e -> edgeLabels.contains(e.getLabel()) && e.getTarget().getTypes().contains(targetType)).collect(Collectors.toSet());
		} else {
			edgeSet = currentSnapshot.getGraph().getGraph().incomingEdgesOf(dataVertex).stream().filter(e -> edgeLabels.contains(e.getLabel()) && e.getSource().getTypes().contains(sourceType)).collect(Collectors.toSet());
		}

		ArrayList<HashSet<ConstantLiteral>> matches = new ArrayList<>();
		int numOfMatchesFound = extractMatches(edgeSet, matches, patternTreeNode, entityURIs, timestamp);
		// TO-DO: The following lines are common among all findMatch methods. Can they be combined?
		matchesSet.addAll(matches);
		if (this.isOnlyInterestingTGFDs() && matches.size() > 0) {
			matchesOfCenterVertexInCurrentSnapshot.add(dataVertex);
		} else if (!this.isOnlyInterestingTGFDs() && numOfMatchesFound > 0) {
			matchesOfCenterVertexInCurrentSnapshot.add(dataVertex);
		}
		return numOfMatchesFound;
	}

	private void countTotalNumberOfMatchesFound(List<Set<Set<ConstantLiteral>>> matchesPerTimestamps) {
		int numberOfMatchesFound = 0;
		for (Set<Set<ConstantLiteral>> matchesInOneTimestamp : matchesPerTimestamps) {
			numberOfMatchesFound += matchesInOneTimestamp.size();
		}
		System.out.println("Total number of matches found across all snapshots: " + numberOfMatchesFound);
	}

	private ArrayList<ArrayList<DataVertex>> getListOfMatchesOfCenterVerticesOfThisPattern(List<GraphLoader> graphs, PatternTreeNode patternTreeNode) {
		String centerVertexType = patternTreeNode.getPattern().getCenterVertexType();
		PatternTreeNode centerVertexParent = patternTreeNode.getCenterVertexParent();
		System.out.println("Center vertex type: "+centerVertexType);
		ArrayList<ArrayList<DataVertex>> matchesOfCenterVertex;
		if (centerVertexParent != null && centerVertexParent.getListOfCenterVertices() != null) {
			System.out.println("Found center vertex parent: "+centerVertexParent);
			matchesOfCenterVertex = centerVertexParent.getListOfCenterVertices();
		} else {
			// TO-DO: Can we reduce the possibility of this happening?
			System.out.println("Missing center vertex parent...");
			matchesOfCenterVertex = extractListOfCenterVertices(graphs, centerVertexType, null);
		}
		return matchesOfCenterVertex;
	}

	public List<Set<Set<ConstantLiteral>>> findMatchesUsingCenterVertices(List<GraphLoader> graphs, PatternTreeNode patternTreeNode) {

		Map<String, List<Integer>> entityURIs = new HashMap<>();

		List<Set<Set<ConstantLiteral>>> matchesPerTimestamps = new ArrayList<>();
		for (int timestamp = 0; timestamp < this.getNumOfSnapshots(); timestamp++) {
			matchesPerTimestamps.add(new HashSet<>());
		}

		this.extractMatchesAcrossSnapshots(graphs, patternTreeNode, matchesPerTimestamps, entityURIs);

		this.countTotalNumberOfMatchesFound(matchesPerTimestamps);

		for (Map.Entry<String, List<Integer>> entry: entityURIs.entrySet()) {
			System.out.println(entry);
		}
		this.setPatternSupport(entityURIs, patternTreeNode);

		return matchesPerTimestamps;
	}

	// TO-DO: Does this method contain duplicate code that is common with other findMatch methods?
	private void extractMatchesAcrossSnapshots(List<GraphLoader> graphs, PatternTreeNode patternTreeNode, List<Set<Set<ConstantLiteral>>> matchesPerTimestamps, Map<String, List<Integer>> entityURIs) {

		ArrayList<ArrayList<DataVertex>> matchesOfCenterVertex = getListOfMatchesOfCenterVerticesOfThisPattern(graphs, patternTreeNode);

		ArrayList<ArrayList<DataVertex>> newMatchesOfCenterVerticesInAllSnapshots = new ArrayList<>();

		int diameter = patternTreeNode.getPattern().getDiameter();

		System.out.println("Searching for patterns of diameter: " + diameter);
		for (int year = 0; year < graphs.size(); year++) {
			GraphLoader currentSnapshot = graphs.get(year);
			Set<Set<ConstantLiteral>> matchesSet = new HashSet<>();
			ArrayList<DataVertex> centerVertexMatchesInThisTimestamp = matchesOfCenterVertex.get(year);
			System.out.println("Number of center vertex matches in this timestamp from previous levels: "+centerVertexMatchesInThisTimestamp.size());
			newMatchesOfCenterVerticesInAllSnapshots.add(new ArrayList<>());
			ArrayList<DataVertex> newMatchesOfCenterVertexInCurrentSnapshot = newMatchesOfCenterVerticesInAllSnapshots.get(newMatchesOfCenterVerticesInAllSnapshots.size()-1);
			int numOfMatchesInTimestamp = 0; int processedCount = 0;
			for (DataVertex dataVertex: centerVertexMatchesInThisTimestamp) {
				int numOfMatchesFound;
				assert this.getCurrentVSpawnLevel() > 0;
				if (this.getCurrentVSpawnLevel() == 1) {
					numOfMatchesFound = findAllMatchesOfK1patternInSnapshotUsingCenterVertices(patternTreeNode, entityURIs, currentSnapshot, matchesSet, newMatchesOfCenterVertexInCurrentSnapshot, dataVertex, year);
				} else {
					if (!this.isFastMatching()) {
						numOfMatchesFound = findAllMatchesOfPatternInThisSnapshotUsingCenterVertices(patternTreeNode, entityURIs, currentSnapshot, matchesSet, newMatchesOfCenterVertexInCurrentSnapshot, dataVertex, year);
					} else {
						if (this.getCurrentVSpawnLevel() == 2) {
							numOfMatchesFound = findAllMatchesOfK2PatternInSnapshotUsingCenterVertices(patternTreeNode, entityURIs, currentSnapshot, matchesSet, newMatchesOfCenterVertexInCurrentSnapshot, dataVertex, year);
						} else if (this.getCurrentVSpawnLevel() > 2 && patternTreeNode.getGraph().edgesOf(patternTreeNode.getPattern().getCenterVertex()).size() == this.getCurrentVSpawnLevel()) {
							numOfMatchesFound = findAllMatchesOfStarPatternInSnapshotUsingCenterVertices(patternTreeNode, entityURIs, currentSnapshot, matchesSet, newMatchesOfCenterVertexInCurrentSnapshot, dataVertex, year);
						}
						else if (this.getCurrentVSpawnLevel() > 2 && patternTreeNode.getGraph().vertexSet().size() == patternTreeNode.getGraph().edgeSet().size()+1) {
							numOfMatchesFound = findAllMatchesOfLinePatternInSnapshotUsingCenterVertices(patternTreeNode, entityURIs, currentSnapshot, matchesSet, newMatchesOfCenterVertexInCurrentSnapshot, dataVertex, year, false);
						}
						else if (this.getCurrentVSpawnLevel() > 2 && patternTreeNode.getGraph().vertexSet().size() == patternTreeNode.getGraph().edgeSet().size()) {
							numOfMatchesFound = findAllMatchesOfLinePatternInSnapshotUsingCenterVertices(patternTreeNode, entityURIs, currentSnapshot, matchesSet, newMatchesOfCenterVertexInCurrentSnapshot, dataVertex, year, true);
						}
						else {
							numOfMatchesFound = findAllMatchesOfPatternInThisSnapshotUsingCenterVertices(patternTreeNode, entityURIs, currentSnapshot, matchesSet, newMatchesOfCenterVertexInCurrentSnapshot, dataVertex, year);
						}
					}
				}
				numOfMatchesInTimestamp += numOfMatchesFound;

				processedCount++;
				if (centerVertexMatchesInThisTimestamp.size() >= 100000 && processedCount % 100000 == 0) {
					System.out.println("Processed " + processedCount + "/" + centerVertexMatchesInThisTimestamp.size());
				}
			}
			System.out.println("Number of matches found: " + numOfMatchesInTimestamp);
			System.out.println("Number of matches found that contain active attributes: " + matchesSet.size());
			System.out.println("Number of center vertex matches in this timestamp in this level: " + newMatchesOfCenterVertexInCurrentSnapshot.size());
			matchesPerTimestamps.get(year).addAll(matchesSet);
		}
		if (this.reUseMatches()) {
			patternTreeNode.setListOfCenterVertices(newMatchesOfCenterVerticesInAllSnapshots);
		}
		System.out.println("Number of entity URIs found: "+entityURIs.size());
	}

	// Use this for k=2 instead of findAllMatchesOfStarPattern to avoid overhead of creating a MappingTree
	private int findAllMatchesOfK2PatternInSnapshotUsingCenterVertices(PatternTreeNode patternTreeNode, Map<String, List<Integer>> entityURIs, GraphLoader currentSnapshot, Set<Set<ConstantLiteral>> matchesSet, ArrayList<DataVertex> newMatchesOfCenterVertexInCurrentSnapshot, DataVertex dataVertex, int year) {
		Set<Vertex> patternVertexSet = patternTreeNode.getGraph().vertexSet();

		Map<PatternVertex, Set<DataVertex>> patternVertexToDataVerticesMap = getPatternVertextoDataVerticesMap(patternTreeNode, currentSnapshot, dataVertex);

		Set<Set<ConstantLiteral>> matches = new HashSet<>();
		Set<ConstantLiteral> centerDataVertexLiterals = getCenterDataVertexLiterals(patternTreeNode, dataVertex, patternVertexSet);
		int numOfMatchesForCenterVertex = 0;
		ArrayList<Entry<PatternVertex, Set<DataVertex>>> vertexSets = new ArrayList<>(patternVertexToDataVerticesMap.entrySet());

		Entry<PatternVertex, Set<DataVertex>> nonCenterVertexEntry1 = vertexSets.get(0);
		String nonCenterPatternVertex1Type = nonCenterVertexEntry1.getKey().getTypes().iterator().next();
		for (DataVertex nonCenterDataVertex1: nonCenterVertexEntry1.getValue()) {
			Entry<PatternVertex, Set<DataVertex>> nonCenterVertexEntry2 = vertexSets.get(1);
			String nonCenterPatternVertex2Type = nonCenterVertexEntry2.getKey().getTypes().iterator().next();
			for (DataVertex nonCenterDataVertex2: nonCenterVertexEntry2.getValue()) {
				numOfMatchesForCenterVertex++;
				Set<ConstantLiteral> match = new HashSet<>();
				int[] interestingnessMap = {0, 0, 0};
				for (String matchedAttrName: nonCenterDataVertex1.getAllAttributesNames()) {
					for (ConstantLiteral activeAttribute : getActiveAttributesInPattern(patternVertexSet, true)) {
						if (!nonCenterPatternVertex1Type.equals(activeAttribute.getVertexType())) continue;
						if (!matchedAttrName.equals(activeAttribute.getAttrName())) continue;
						String matchedAttrValue = nonCenterDataVertex1.getAttributeValueByName(matchedAttrName);
						ConstantLiteral literal = new ConstantLiteral(nonCenterPatternVertex1Type, matchedAttrName, matchedAttrValue);
						match.add(literal);
						interestingnessMap[0] += 1;
					}
				}
				for (String matchedAttrName: nonCenterDataVertex2.getAllAttributesNames()) {
					for (ConstantLiteral activeAttribute : getActiveAttributesInPattern(patternVertexSet, true)) {
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
				if (this.isOnlyInterestingTGFDs()) {
					boolean skip = false;
					for (int count: interestingnessMap) {
						if (count < 2) {
							skip = true;
							break;
						}
					}
					if (skip) continue;
				} else if (!this.isOnlyInterestingTGFDs() && match.size() <= patternVertexSet.size()) {
					continue;
				}
				matches.add(match);
			}
		}
		String entityURI = dataVertex.getVertexURI();
		// TO-DO: The following lines are common among all findMatch methods. Can they be combined?
		if (matches.size() > 0) { // equivalent to entityURI != null
			matchesSet.addAll(matches);
			entityURIs.putIfAbsent(entityURI, createEmptyArrayListOfSize(this.getNumOfSnapshots()));
			entityURIs.get(entityURI).set(year, entityURIs.get(entityURI).get(year)+matches.size());
		}
		if (this.isOnlyInterestingTGFDs() && matches.size() > 0) {
			newMatchesOfCenterVertexInCurrentSnapshot.add(dataVertex);
		} else if (!this.isOnlyInterestingTGFDs() && numOfMatchesForCenterVertex > 0) {
			newMatchesOfCenterVertexInCurrentSnapshot.add(dataVertex);
		}
		return numOfMatchesForCenterVertex;
	}

	// TO-DO: Does this method contain duplicate code that is common with other findMatch methods?
	private int findAllMatchesOfLinePatternInSnapshotUsingCenterVertices(PatternTreeNode patternTreeNode, Map<String, List<Integer>> entityURIs, GraphLoader currentSnapshot, Set<Set<ConstantLiteral>> matchesSet, ArrayList<DataVertex> newMatchesOfCenterVertexInCurrentSnapshot, DataVertex startDataVertex, int year, boolean isCyclic) {

		Vertex startVertex = patternTreeNode.getPattern().getCenterVertex();
		String startVertexType = startVertex.getTypes().iterator().next();

		Vertex currentPatternVertex = startVertex;
		Set<RelationshipEdge> visited = new HashSet<>();
		List<RelationshipEdge> patternEdgePath = new ArrayList<>();
		while (visited.size() < patternTreeNode.getGraph().edgeSet().size()) {
			for (RelationshipEdge patternEdge : patternTreeNode.getGraph().edgesOf(currentPatternVertex)) {
				if (!visited.contains(patternEdge)) {
					boolean outgoing = patternEdge.getSource().equals(currentPatternVertex);
					currentPatternVertex = outgoing ? patternEdge.getTarget() : patternEdge.getSource();
					patternEdgePath.add(patternEdge);
					visited.add(patternEdge);
				}
			}
		}

		MappingTree mappingTree = new MappingTree();
		mappingTree.addLevel();
		mappingTree.getLevel(0).add(new MappingTreeNode(startDataVertex, startVertexType, null));

		for (int index = 0; index < patternTreeNode.getGraph().edgeSet().size(); index++) {

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
						if (isCyclic && index == patternTreeNode.getGraph().edgeSet().size()-1) {
							if (!newMappingTreeNode.getDataVertex().getVertexURI().equals(startDataVertex.getVertexURI())) {
								newMappingTreeNode.setPruned(true);
							}
						}
						mappingTree.getLevel(index + 1).add(newMappingTreeNode);
					}
				}
				for (RelationshipEdge dataEdge : currentSnapshot.getGraph().getGraph().incomingEdgesOf(currentDataVertex)) {
					if (dataEdge.getLabel().equals(currentPatternEdgeLabel)
							&& dataEdge.getSource().getTypes().contains(currentPatternSourceVertexLabel)
							&& dataEdge.getTarget().getTypes().contains(currentPatternTargetVertexLabel)) {
						DataVertex otherVertex = (DataVertex) dataEdge.getSource();
						MappingTreeNode newMappingTreeNode = new MappingTreeNode(otherVertex, currentPatternSourceVertexLabel, currentMappingTreeNode);
						if (isCyclic && index == patternTreeNode.getGraph().edgeSet().size()-1) {
							if (!newMappingTreeNode.getDataVertex().getVertexURI().equals(startDataVertex.getVertexURI())) {
								newMappingTreeNode.setPruned(true);
							}
						}
						mappingTree.getLevel(index + 1).add(newMappingTreeNode);
					}
				}
			}
		}

		if (mappingTree.getTree().size() == patternTreeNode.getGraph().vertexSet().size()) {
			return extractMatchesFromMappingTree(patternTreeNode, entityURIs, matchesSet, newMatchesOfCenterVertexInCurrentSnapshot, startDataVertex, year, mappingTree);
		} else {
			return 0;
		}
	}

	// TO-DO: Does this method contain duplicate code that is common with other findMatch methods?
	private int findAllMatchesOfStarPatternInSnapshotUsingCenterVertices(PatternTreeNode patternTreeNode, Map<String, List<Integer>> entityURIs, GraphLoader currentSnapshot, Set<Set<ConstantLiteral>> matchesSet, ArrayList<DataVertex> newMatchesOfCenterVertexInCurrentSnapshot, DataVertex centerDataVertex, int year) {

		Map<PatternVertex, Set<DataVertex>> patternVertexToDataVerticesMap = getPatternVertextoDataVerticesMap(patternTreeNode, currentSnapshot, centerDataVertex);

		ArrayList<Entry<PatternVertex, Set<DataVertex>>> vertexSets = new ArrayList<>(patternVertexToDataVerticesMap.entrySet());
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

		return extractMatchesFromMappingTree(patternTreeNode, entityURIs, matchesSet, newMatchesOfCenterVertexInCurrentSnapshot, centerDataVertex, year, mappingTree);
	}

	private int extractMatchesFromMappingTree(PatternTreeNode patternTreeNode, Map<String, List<Integer>> entityURIs, Set<Set<ConstantLiteral>> matchesSet, ArrayList<DataVertex> newMatchesOfCenterVertexInCurrentSnapshot, DataVertex centerDataVertex, int year, MappingTree mappingTree) {
		Set<Vertex> patternVertexSet = patternTreeNode.getGraph().vertexSet();
		Set<Set<ConstantLiteral>> matches = new HashSet<>();
		Set<ConstantLiteral> centerDataVertexLiterals = getCenterDataVertexLiterals(patternTreeNode, centerDataVertex, patternVertexSet);
		int numOfMatchesForCenterVertex = 0;
		for (MappingTreeNode leafNode: mappingTree.getLevel(mappingTree.getTree().size()-1).stream().filter(mappingTreeNode -> !mappingTreeNode.isPruned()).collect(Collectors.toList())) {
			numOfMatchesForCenterVertex++;
			Set<MappingTreeNode> mapping = leafNode.getPathToRoot();
			List<Integer> interestingnessMap = new ArrayList<>();
			Set<ConstantLiteral> match = new HashSet<>(centerDataVertexLiterals);
			interestingnessMap.add(centerDataVertexLiterals.size());
			for (MappingTreeNode mappingTreeNode: mapping) {
				DataVertex dv = mappingTreeNode.getDataVertex();
				String patternVertexType = mappingTreeNode.getPatternVertexType();
				interestingnessMap.add(0);
				for (String matchedAttrName: dv.getAllAttributesNames()) {
					for (ConstantLiteral activeAttribute : getActiveAttributesInPattern(patternVertexSet, true)) {
						if (!patternVertexType.equals(activeAttribute.getVertexType())) continue;
						if (!matchedAttrName.equals(activeAttribute.getAttrName())) continue;
						String matchedAttrValue = dv.getAttributeValueByName(matchedAttrName);
						ConstantLiteral literal = new ConstantLiteral(patternVertexType, matchedAttrName, matchedAttrValue);
						match.add(literal);
						interestingnessMap.set(interestingnessMap.size()-1, interestingnessMap.get(interestingnessMap.size()-1)+1);
					}
				}
			}
			if (this.isOnlyInterestingTGFDs()) {
				boolean skip = false;
				for (int count: interestingnessMap) {
					if (count < 2) {
						skip = true;
						break;
					}
				}
				if (skip) continue;
			} else if (!this.isOnlyInterestingTGFDs() && match.size() <= patternVertexSet.size()) {
				continue;
			}
			matches.add(match);
		}

		String entityURI = centerDataVertex.getVertexURI();
		// TO-DO: The following lines are common among all findMatch methods. Can they be combined?
		if (matches.size() > 0) { // equivalent to entityURI != null
			matchesSet.addAll(matches);
			entityURIs.putIfAbsent(entityURI, createEmptyArrayListOfSize(this.getNumOfSnapshots()));
			entityURIs.get(entityURI).set(year, entityURIs.get(entityURI).get(year)+matches.size());
		}
		if (this.isOnlyInterestingTGFDs() && matches.size() > 0) {
			newMatchesOfCenterVertexInCurrentSnapshot.add(centerDataVertex);
		} else if (!this.isOnlyInterestingTGFDs() && numOfMatchesForCenterVertex > 0) {
			newMatchesOfCenterVertexInCurrentSnapshot.add(centerDataVertex);
		}
		return numOfMatchesForCenterVertex;
	}

	@NotNull
	private Set<ConstantLiteral> getCenterDataVertexLiterals(PatternTreeNode patternTreeNode, DataVertex dataVertex, Set<Vertex> patternVertexSet) {
		Set<ConstantLiteral> centerDataVertexLiterals = new HashSet<>();
		String centerVertexType = patternTreeNode.getPattern().getCenterVertexType();
		for (String matchedAttrName: dataVertex.getAllAttributesNames()) {
			for (ConstantLiteral activeAttribute : getActiveAttributesInPattern(patternVertexSet, true)) {
				if (!centerVertexType.equals(activeAttribute.getVertexType())) continue;
				if (!matchedAttrName.equals(activeAttribute.getAttrName())) continue;
				String matchedAttrValue = dataVertex.getAttributeValueByName(matchedAttrName);
				ConstantLiteral literal = new ConstantLiteral(centerVertexType, matchedAttrName, matchedAttrValue);
				centerDataVertexLiterals.add(literal);
			}
		}
		return centerDataVertexLiterals;
	}

	@NotNull
	private Map<PatternVertex, Set<DataVertex>> getPatternVertextoDataVerticesMap(PatternTreeNode patternTreeNode, GraphLoader currentSnapshot, DataVertex dataVertex) {
		Map<PatternVertex, Set<DataVertex>> patternVertexToDataVerticesMap = new HashMap<>();
		Vertex centerPatternVertex = patternTreeNode.getPattern().getCenterVertex();
		for (RelationshipEdge patternEdge: patternTreeNode.getPattern().getPattern().incomingEdgesOf(centerPatternVertex)) {
			PatternVertex nonCenterPatternVertex = (PatternVertex) patternEdge.getSource();
			patternVertexToDataVerticesMap.put(nonCenterPatternVertex, new HashSet<>());
			for (RelationshipEdge dataEdge: currentSnapshot.getGraph().getGraph().incomingEdgesOf(dataVertex)) {
				if (dataEdge.getLabel().equals(patternEdge.getLabel())
						&& dataEdge.getSource().getTypes().contains(nonCenterPatternVertex.getTypes().iterator().next())) {
					patternVertexToDataVerticesMap.get(nonCenterPatternVertex).add((DataVertex) dataEdge.getSource());
				}
			}
		}
		for (RelationshipEdge patternEdge: patternTreeNode.getPattern().getPattern().outgoingEdgesOf(centerPatternVertex)) {
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

	// TO-DO: Does this method contain duplicate code that is common with other findMatch methods?
	private int findAllMatchesOfPatternInThisSnapshotUsingCenterVertices(PatternTreeNode patternTreeNode, Map<String, List<Integer>> entityURIs, GraphLoader currentSnapshot, Set<Set<ConstantLiteral>> matchesSet, ArrayList<DataVertex> matchesOfCenterVertexInCurrentSnapshot, DataVertex dataVertex, int timestamp) {
		Set<String> edgeLabels = patternTreeNode.getGraph().edgeSet().stream().map(RelationshipEdge::getLabel).collect(Collectors.toSet());
		int diameter = patternTreeNode.getPattern().getRadius();
		Graph<Vertex, RelationshipEdge> subgraph = currentSnapshot.getGraph().getSubGraphWithinDiameter(dataVertex, diameter, edgeLabels, patternTreeNode.getGraph().edgeSet());
		VF2AbstractIsomorphismInspector<Vertex, RelationshipEdge> results = new VF2SubgraphIsomorphism().execute2(subgraph, patternTreeNode.getPattern(), false);

		int numOfMatchesForCenterVertex = 0;
		ArrayList<HashSet<ConstantLiteral>> matches = new ArrayList<>();
		if (results.isomorphismExists()) {
			numOfMatchesForCenterVertex = extractMatches(results.getMappings(), matches, patternTreeNode, entityURIs, timestamp);
			matchesSet.addAll(matches);
			// TO-DO: The following lines are common among all findMatch methods. Can they be combined?
			if (this.isOnlyInterestingTGFDs() && matches.size() > 0) {
				matchesOfCenterVertexInCurrentSnapshot.add(dataVertex);
			} else if (!this.isOnlyInterestingTGFDs() && numOfMatchesForCenterVertex > 0) {
				matchesOfCenterVertexInCurrentSnapshot.add(dataVertex);
			}
		}

		return numOfMatchesForCenterVertex;
	}

	protected void printHistogramStatistics() {
		System.out.println("----------------Statistics for Histogram-----------------");
		Collections.sort(this.vertexFrequenciesList);
		Collections.sort(this.edgeFrequenciesList);
		double medianVertexSupport = 0;
		if (this.vertexFrequenciesList.size() > 0) {
			medianVertexSupport = this.vertexFrequenciesList.size() % 2 != 0 ? this.vertexFrequenciesList.get(this.vertexFrequenciesList.size() / 2) : ((this.vertexFrequenciesList.get(this.vertexFrequenciesList.size() / 2) + this.vertexFrequenciesList.get(this.vertexFrequenciesList.size() / 2 - 1)) / 2);
		}
		double medianEdgeSupport = 0;
		if (this.edgeFrequenciesList.size() > 0) {
			medianEdgeSupport = this.edgeFrequenciesList.size() % 2 != 0 ? this.edgeFrequenciesList.get(this.edgeFrequenciesList.size() / 2) : ((this.edgeFrequenciesList.get(this.edgeFrequenciesList.size() / 2) + this.edgeFrequenciesList.get(this.edgeFrequenciesList.size() / 2 - 1)) / 2);
		}
		System.out.println("Median Vertex Support: " + medianVertexSupport);
		System.out.println("Median Edge Support: " + medianEdgeSupport);
	}

	private void printSupportStatisticsForThisSnapshot() {
		System.out.println("----------------Support Statistics for vSpawn level "+this.getCurrentVSpawnLevel()+"-----------------");

		Collections.sort(this.patternSupportsListForThisSnapshot);
		Collections.sort(this.constantTgfdSupportsListForThisSnapshot);
		Collections.sort(this.generalTgfdSupportsListForThisSnapshot);

		double medianPatternSupport = 0;
		if (this.patternSupportsListForThisSnapshot.size() > 0) {
			medianPatternSupport = this.patternSupportsListForThisSnapshot.size() % 2 != 0 ? this.patternSupportsListForThisSnapshot.get(this.patternSupportsListForThisSnapshot.size() / 2) : ((this.patternSupportsListForThisSnapshot.get(this.patternSupportsListForThisSnapshot.size() / 2) + this.patternSupportsListForThisSnapshot.get(this.patternSupportsListForThisSnapshot.size() / 2 - 1)) / 2);
		}
		this.addToMedianPatternSupportsList(medianPatternSupport);
		double medianConstantTgfdSupport = 0;
		if (this.constantTgfdSupportsListForThisSnapshot.size() > 0) {
			medianConstantTgfdSupport = this.constantTgfdSupportsListForThisSnapshot.size() % 2 != 0 ? this.constantTgfdSupportsListForThisSnapshot.get(this.constantTgfdSupportsListForThisSnapshot.size() / 2) : ((this.constantTgfdSupportsListForThisSnapshot.get(this.constantTgfdSupportsListForThisSnapshot.size() / 2) + this.constantTgfdSupportsListForThisSnapshot.get(this.constantTgfdSupportsListForThisSnapshot.size() / 2 - 1)) / 2);
		}
		this.addToMedianConstantTgfdSupportsList(medianConstantTgfdSupport);
		double medianGeneralTgfdSupport = 0;
		if (this.generalTgfdSupportsListForThisSnapshot.size() > 0) {
			medianGeneralTgfdSupport = this.generalTgfdSupportsListForThisSnapshot.size() % 2 != 0 ? this.generalTgfdSupportsListForThisSnapshot.get(this.generalTgfdSupportsListForThisSnapshot.size() / 2) : ((this.generalTgfdSupportsListForThisSnapshot.get(this.generalTgfdSupportsListForThisSnapshot.size() / 2) + this.generalTgfdSupportsListForThisSnapshot.get(this.generalTgfdSupportsListForThisSnapshot.size() / 2 - 1)) / 2);
		}
		this.addToMedianGeneralTgfdSupportsList(medianGeneralTgfdSupport);

		System.out.println("Median Pattern Support: " + medianPatternSupport);
		System.out.println("Median Constant TGFD Support: " + medianConstantTgfdSupport);
		System.out.println("Median General TGFD Support: " + medianGeneralTgfdSupport);
		// Reset for each level of vSpawn
		this.patternSupportsListForThisSnapshot = new ArrayList<>();
		this.constantTgfdSupportsListForThisSnapshot = new ArrayList<>();
		this.generalTgfdSupportsListForThisSnapshot = new ArrayList<>();
	}

	public void markAsKexperiment() {
		this.setExperimentName("vary-k");
		this.setkExperiment(true);
	}

	public void setExperimentDateAndTimeStamp(String timeAndDateStamp) {
		this.setTimeAndDateStamp(timeAndDateStamp);
	}

	protected void printVertexAndEdgeStatisticsForEntireTemporalGraph(List<?> graphs, Map<String, Integer> vertexTypesHistogram) {
		System.out.println("Number of vertices across all graphs: " + this.getNumOfVerticesInAllGraphs());
		System.out.println("Number of vertex types across all graphs: " + vertexTypesHistogram.size());
		if (graphs.stream().allMatch(c -> c instanceof GraphLoader)) {
			System.out.println("Number of edges in each snapshot before collapsing super vertices...");
			long totalEdgeCount = 0;
			for (GraphLoader graph : (List<GraphLoader>) graphs) {
				long edgeCount = graph.getGraph().getGraph().edgeSet().size();
				totalEdgeCount += edgeCount;
				System.out.println(edgeCount);
			}
			System.out.println("Number of edges across all graphs: " + totalEdgeCount);
		}
	}

	private void calculateAverageInDegree(Map<String, List<Integer>> vertexTypesToInDegreesMap) {
		System.out.println("Average in-degrees of vertex types...");
		List<Double> avgInDegrees = new ArrayList<>();
		for (Entry<String, List<Integer>> entry: vertexTypesToInDegreesMap.entrySet()) {
			if (entry.getValue().size() == 0) continue;
			entry.getValue().sort(Comparator.naturalOrder());
			double avgInDegree = (double) entry.getValue().stream().mapToInt(Integer::intValue).sum() / (double) entry.getValue().size();
			System.out.println(entry.getKey()+": "+avgInDegree);
			avgInDegrees.add(avgInDegree);
			this.getVertexTypesToAvgInDegreeMap().put(entry.getKey(), avgInDegree);
		}
//		double avgInDegree = avgInDegrees.stream().mapToDouble(Double::doubleValue).sum() / (double) avgInDegrees.size();
		double avgInDegree = this.getHighOutlierThreshold(avgInDegrees);
		this.setSuperVertexDegree(Math.max(avgInDegree, DEFAULT_AVG_SUPER_VERTEX_DEGREE));
		System.out.println("Super vertex degree is "+ this.getSuperVertexDegree());
	}

	protected void calculateMedianInDegree(Map<String, List<Integer>> vertexTypesToInDegreesMap) {
		System.out.println("Median in-degrees of vertex types...");
		List<Double> medianInDegrees = new ArrayList<>();
		for (Entry<String, List<Integer>> entry: vertexTypesToInDegreesMap.entrySet()) {
			if (entry.getValue().size() == 0) {
				this.getVertexTypesToAvgInDegreeMap().put(entry.getKey(), 0.0);
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
			this.getVertexTypesToAvgInDegreeMap().put(entry.getKey(), medianInDegree);
		}
//		double medianInDegree;
//		if (medianInDegrees.size() % 2 == 0) {
//			medianInDegree = (medianInDegrees.get(medianInDegrees.size()/2) + medianInDegrees.get(medianInDegrees.size()/2-1))/2.0;
//		} else {
//			medianInDegree = medianInDegrees.get(medianInDegrees.size()/2);
//		}
        double medianInDegree = this.getHighOutlierThreshold(medianInDegrees);
		this.setSuperVertexDegree(Math.max(medianInDegree, MEDIAN_SUPER_VERTEX_TYPE_INDEGREE_FLOOR));
		System.out.println("Super vertex degree is "+ this.getSuperVertexDegree());
	}

	private void calculateMaxInDegree(Map<String, List<Integer>> vertexTypesToInDegreesMap) {
		System.out.println("Max in-degrees of vertex types...");
		List<Double> maxInDegrees = new ArrayList<>();
		for (Entry<String, List<Integer>> entry: vertexTypesToInDegreesMap.entrySet()) {
			if (entry.getValue().size() == 0) continue;
			double maxInDegree = Collections.max(entry.getValue()).doubleValue();
			System.out.println(entry.getKey()+": "+maxInDegree);
			maxInDegrees.add(maxInDegree);
			this.getVertexTypesToAvgInDegreeMap().put(entry.getKey(), maxInDegree);
		}
		double maxInDegree = getHighOutlierThreshold(maxInDegrees);
		System.out.println("Based on histogram, high outlier threshold for in-degree is "+maxInDegree);
		this.setSuperVertexDegree(Math.max(maxInDegree, DEFAULT_MAX_SUPER_VERTEX_DEGREE));
		System.out.println("Super vertex degree is "+ this.getSuperVertexDegree());
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

	protected void dissolveSuperVerticesAndUpdateHistograms(Map<String, Set<String>> tempVertexAttrFreqMap, Map<String, Set<String>> attrDistributionMap, Map<String, List<Integer>> vertexTypesToInDegreesMap, Map<String, Integer> edgeTypesHistogram) {
		int numOfCollapsedSuperVertices = 0;
		this.setNumOfEdgesInAllGraphs(0);
		this.setNumOfVerticesInAllGraphs(0);
		for (Entry<String, List<Integer>> entry: vertexTypesToInDegreesMap.entrySet()) {
			String superVertexType = entry.getKey();
			if (entry.getValue().size() == 0) continue;
			double medianDegree = this.getVertexTypesToAvgInDegreeMap().get(entry.getKey());
			if (medianDegree > this.getSuperVertexDegree()) {
				System.out.println("Collapsing super vertex "+superVertexType+" with...");
				System.out.println("Degree = "+medianDegree+", Vertex Count = "+this.vertexHistogram.get(superVertexType));
				numOfCollapsedSuperVertices++;
				for (GraphLoader graph: this.getGraphs()) {
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
								tempVertexAttrFreqMap.putIfAbsent(vertexType, new HashSet<>());
								for (String attrName: v.getAllAttributesNames()) {
									if (attrName.equals("uri")) continue;
									numOfAttributes++;
									if (tempVertexAttrFreqMap.containsKey(vertexType)) {
										tempVertexAttrFreqMap.get(vertexType).add(attrName);
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
					this.setNumOfVerticesInAllGraphs(this.getNumOfVerticesInAllGraphs()+numOfVertices);

					System.out.println("Number of attributes added to graph: " + numOfAttributesAdded);
					System.out.println("Updated count of attributes in graph: " + numOfAttributes);

					System.out.println("Number of edges deleted from graph: " + numOfEdgesDeleted);
					int newEdgeCount = graph.getGraph().getGraph().edgeSet().size();
					System.out.println("Updated count of edges in graph: " + newEdgeCount);
					this.setNumOfEdgesInAllGraphs(this.getNumOfEdgesInAllGraphs()+newEdgeCount);
				}
			}
		}
		System.out.println("Number of super vertices collapsed: "+numOfCollapsedSuperVertices);
	}

	// TO-DO: Can this be merged with the code in histogram?
	protected void dissolveSuperVerticesBasedOnCount(GraphLoader graph) {
		System.out.println("Dissolving super vertices based on count...");
		System.out.println("Initial edge count of first snapshot: "+graph.getGraph().getGraph().edgeSet().size());
		for (Vertex v: graph.getGraph().getGraph().vertexSet()) {
			int inDegree = graph.getGraph().getGraph().incomingEdgesOf(v).size();
			if (inDegree > INDIVIDUAL_VERTEX_INDEGREE_FLOOR) {
				List<RelationshipEdge> edgesToDelete = new ArrayList<>(graph.getGraph().getGraph().incomingEdgesOf(v));
				for (RelationshipEdge e : edgesToDelete) {
					Vertex sourceVertex = e.getSource();
					Map<String, Attribute> sourceVertexAttrMap = sourceVertex.getAllAttributesHashMap();
					String newAttrName = e.getLabel();
					if (sourceVertexAttrMap.containsKey(newAttrName)) {
						newAttrName = e.getLabel() + "value";
						if (!sourceVertexAttrMap.containsKey(newAttrName)) {
							sourceVertex.putAttributeIfAbsent(new Attribute(newAttrName, v.getAttributeValueByName("uri")));
						}
					}
					graph.getGraph().getGraph().removeEdge(e);
				}
			}
		}
		System.out.println("Updated edge count of first snapshot: "+graph.getGraph().getGraph().edgeSet().size());
	}

	protected int readVertexTypesAndAttributeNamesFromGraph(Map<String, Integer> vertexTypesHistogram, Map<String, Set<String>> tempVertexAttrFreqMap, Map<String, Set<String>> attrDistributionMap, Map<String, List<Integer>> vertexTypesToInDegreesMap, GraphLoader graph, Map<String, Integer> edgeTypesHistogram) {
		int initialEdgeCount = graph.getGraph().getGraph().edgeSet().size();
		System.out.println("Initial count of edges in graph: " + initialEdgeCount);

		int numOfAttributesAdded = 0;
		int numOfEdgesDeleted = 0;
		int numOfVerticesInGraph = 0;
		int numOfAttributesInGraph = 0;
		for (Vertex v: graph.getGraph().getGraph().vertexSet()) {
			numOfVerticesInGraph++;
			for (String vertexType : v.getTypes()) {
				vertexTypesHistogram.merge(vertexType, 1, Integer::sum);
				tempVertexAttrFreqMap.putIfAbsent(vertexType, new HashSet<>());

				for (String attrName: v.getAllAttributesNames()) {
					if (attrName.equals("uri")) continue;
					numOfAttributesInGraph++;
					if (tempVertexAttrFreqMap.containsKey(vertexType)) {
						tempVertexAttrFreqMap.get(vertexType).add(attrName);
					}
					if (!attrDistributionMap.containsKey(attrName)) {
						attrDistributionMap.put(attrName, new HashSet<>());
					}
					attrDistributionMap.get(attrName).add(vertexType);
				}
			}

			int inDegree = graph.getGraph().getGraph().incomingEdgesOf(v).size();
			if (this.isDissolveSuperVerticesBasedOnCount() && inDegree > INDIVIDUAL_VERTEX_INDEGREE_FLOOR) {
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
							edgeTypesHistogram.merge(uniqueEdge, 1, Integer::sum);
						}
					}
				}
				// Update all attribute related histograms
				for (String vertexType : v.getTypes()) {
					tempVertexAttrFreqMap.putIfAbsent(vertexType, new HashSet<>());
					for (String attrName: v.getAllAttributesNames()) {
						if (attrName.equals("uri")) continue;
						if (tempVertexAttrFreqMap.containsKey(vertexType)) {
							tempVertexAttrFreqMap.get(vertexType).add(attrName);
						}
						if (!attrDistributionMap.containsKey(attrName)) {
							attrDistributionMap.put(attrName, new HashSet<>());
						}
						attrDistributionMap.get(attrName).add(vertexType);
					}
				}
			}
			for (String vertexType : v.getTypes()) {
				if (!vertexTypesToInDegreesMap.containsKey(vertexType)) {
					vertexTypesToInDegreesMap.put(vertexType, new ArrayList<>());
				}
				if (inDegree > 0) {
					vertexTypesToInDegreesMap.get(vertexType).add(inDegree);
				}
			}
		}
		for (Map.Entry<String, List<Integer>> entry: vertexTypesToInDegreesMap.entrySet()) {
			entry.getValue().sort(Comparator.naturalOrder());
		}
		System.out.println("Number of vertices in graph: " + numOfVerticesInGraph);
		System.out.println("Number of attributes in graph: " + numOfAttributesInGraph);

		System.out.println("Number of attributes added to graph: " + numOfAttributesAdded);
		System.out.println("Updated count of attributes in graph: " + (numOfAttributesInGraph+numOfAttributesAdded));

		System.out.println("Number of edges deleted from graph: " + numOfEdgesDeleted);
		int newEdgeCount = graph.getGraph().getGraph().edgeSet().size();
		System.out.println("Updated count of edges in graph: " + newEdgeCount);

		return numOfVerticesInGraph;
	}

	private void calculateSuperVertexDegreeThreshold(Map<String, List<Integer>> vertexTypesToInDegreesMap) {
		List<Long> listOfAverageDegreesAbove1 = new ArrayList<>();
		System.out.println("Average in-degree of each vertex type...");
		for (Entry<String, List<Integer>> entry: vertexTypesToInDegreesMap.entrySet()) {
			long averageDegree = Math.round(entry.getValue().stream().reduce(0, Integer::sum).doubleValue() / (double) entry.getValue().size());
			System.out.println(entry.getKey()+":"+averageDegree);
			listOfAverageDegreesAbove1.add(averageDegree);
		}
		this.setSuperVertexDegree(Math.max(getSuperVertexDegree(), Math.round(listOfAverageDegreesAbove1.stream().reduce(0L, Long::sum).doubleValue() / (double) listOfAverageDegreesAbove1.size())));
	}

	protected void setVertexTypesToAttributesMap(Map<String, Set<String>> tempVertexAttrFreqMap) {
		Map<String, HashSet<String>> vertexTypesToAttributesMap = new HashMap<>();
		for (String vertexType : tempVertexAttrFreqMap.keySet()) {
			Set<String> attrNameSet = tempVertexAttrFreqMap.get(vertexType);
			vertexTypesToAttributesMap.put(vertexType, new HashSet<>());
			for (String attrName : attrNameSet) {
				if (this.activeAttributesSet.contains(attrName)) { // Filters non-active attributes
					vertexTypesToAttributesMap.get(vertexType).add(attrName);
				}
			}
		}

		this.vertexTypesToAttributesMap = vertexTypesToAttributesMap;
	}

	protected void setActiveAttributeSet(Map<String, Set<String>> attrDistributionMap) {
		ArrayList<Entry<String,Set<String>>> sortedAttrDistributionMap = new ArrayList<>(attrDistributionMap.entrySet());
		sortedAttrDistributionMap.sort((o1, o2) -> o2.getValue().size() - o1.getValue().size());
		int exclusiveIndex = Math.min(this.getGamma(), sortedAttrDistributionMap.size());
		for (Entry<String, Set<String>> attrNameEntry : sortedAttrDistributionMap.subList(0, exclusiveIndex)) {
			this.activeAttributesSet.add(attrNameEntry.getKey());
		}
		if (this.getInterestSet().size() > 0) {
			for (Entry<String, Set<String>> attrNameEntry : sortedAttrDistributionMap.subList(exclusiveIndex, sortedAttrDistributionMap.size())) {
				if (this.getInterestSet().contains(attrNameEntry.getKey()))
					this.activeAttributesSet.add(attrNameEntry.getKey());
			}
		}
	}

	protected int readEdgesInfoFromGraph(Map<String, Integer> edgeTypesHistogram, GraphLoader graph) {
		int numOfEdges = 0;
		for (RelationshipEdge e: graph.getGraph().getGraph().edgeSet()) {
			numOfEdges++;
			Vertex sourceVertex = e.getSource();
			String predicateName = e.getLabel();
			Vertex objectVertex = e.getTarget();
			for (String subjectVertexType: sourceVertex.getTypes()) {
				for (String objectVertexType: objectVertex.getTypes()) {
					String uniqueEdge = subjectVertexType + " " + predicateName + " " + objectVertexType;
					edgeTypesHistogram.merge(uniqueEdge, 1, Integer::sum);
				}
			}
		}
		System.out.println("Number of edges in graph: " + numOfEdges);
		return numOfEdges;
	}

	public void setSortedFrequentVertexTypesHistogram(Map<String, Integer> vertexTypesHistogram) {
		ArrayList<Entry<String, Integer>> sortedVertexTypesHistogram = new ArrayList<>(vertexTypesHistogram.entrySet());
		sortedVertexTypesHistogram.sort((o1, o2) -> o2.getValue() - o1.getValue());
		for (Entry<String, Integer> entry : sortedVertexTypesHistogram) {
			this.vertexHistogram.put(entry.getKey(), entry.getValue());
			double vertexFrequency = (double) entry.getValue() / this.getNumOfVerticesInAllGraphs();
			this.vertexFrequenciesList.add(vertexFrequency);
		}
		this.sortedVertexHistogram = sortedVertexTypesHistogram;
	}

	public void setSortedFrequentEdgeHistogram(Map<String, Integer> edgeTypesHist, Map<String, Integer> vertexTypesHistogram) {
		ArrayList<Entry<String, Integer>> finalEdgesHist = new ArrayList<>();
		for (Entry<String, Integer> entry : edgeTypesHist.entrySet()) {
			String[] edgeString = entry.getKey().split(" ");
			String sourceType = edgeString[0];
			String targetType = edgeString[2];
			this.vertexHistogram.put(sourceType, vertexTypesHistogram.get(sourceType));
			this.vertexHistogram.put(targetType, vertexTypesHistogram.get(targetType));
			double edgeFrequency = (double) entry.getValue() / (double) this.getNumOfEdgesInAllGraphs();
			this.edgeFrequenciesList.add(edgeFrequency);
			if (this.vertexTypesToAttributesMap.get(sourceType).size() > 0 && this.vertexTypesToAttributesMap.get(targetType).size() > 0) {
				finalEdgesHist.add(entry);
			}
		}
		finalEdgesHist.sort((o1, o2) -> o2.getValue() - o1.getValue());
		int exclusiveIndex = Math.min(finalEdgesHist.size(), this.getFrequentSetSize());
		this.sortedFrequentEdgesHistogram.addAll(finalEdgesHist.subList(0, exclusiveIndex));
		if (this.getInterestSet().size() > 0) {
			for (Entry<String, Integer> entry : finalEdgesHist.subList(exclusiveIndex, finalEdgesHist.size())) {
				String[] edgeString = entry.getKey().split(" ");
				String sourceType = edgeString[0];
				if (this.getInterestSet().contains(sourceType)) {
					this.sortedFrequentEdgesHistogram.add(entry);
					continue;
				}
				String edgeLabel = edgeString[1];
				if (this.getInterestSet().contains(edgeLabel)) {
					this.sortedFrequentEdgesHistogram.add(entry);
					continue;
				}
				String targetType = edgeString[2];
				if (this.getInterestSet().contains(targetType)) {
					this.sortedFrequentEdgesHistogram.add(entry);
					continue;
				}
			}
		}

		Set<String> relevantFrequentVertexTypes = new HashSet<>();
		for (Entry<String, Integer> entry : sortedFrequentEdgesHistogram) {
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
		this.setSortedFrequentVertexTypesHistogram(relevantVertexTypesHistogram);
	}

	public void loadGraphsAndComputeHistogram(List<Entry<String, List<String>>> timestampToPathsMap) {
		System.out.println("Computing Histogram...");

		final long histogramTime = System.currentTimeMillis();

		Map<String, Integer> vertexTypesHistogram = new HashMap<>();
		Map<String, Set<String>> tempVertexAttrFreqMap = new HashMap<>();
		Map<String, Set<String>> attrDistributionMap = new HashMap<>();
		Map<String, List<Integer>> vertexTypesToInDegreesMap = new HashMap<>();
		Map<String, Integer> edgeTypesHistogram = new HashMap<>();

		this.setGraphs(new ArrayList<>());

		int numOfVerticesAcrossAllGraphs = 0;
		int numOfEdgesAcrossAllGraphs = 0;
		for (Map.Entry<String, List<String>> timestampToPathEntry: timestampToPathsMap) {
			final long graphLoadTime = System.currentTimeMillis();
			GraphLoader graphLoader;
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
			if (this.getLoader().equals("dbpedia")) {
				graphLoader = new DBPediaLoader(new ArrayList<>(), Collections.singletonList(model), Collections.singletonList(dataModel));
			} else {
				graphLoader = new IMDBLoader(new ArrayList<>(), Collections.singletonList(dataModel));
			}
			if (this.isStoreInMemory() || this.getLoader().equalsIgnoreCase("dbpedia")) {
				if (this.useChangeFile()) {
					if (this.getGraphs().size() == 0) {
						this.getGraphs().add(graphLoader);
						this.loadChangeFilesIntoMemory();
					}
				} else {
					this.getGraphs().add(graphLoader);
				}
			}

			printWithTime("Single graph load", (System.currentTimeMillis() - graphLoadTime));
			final long graphReadTime = System.currentTimeMillis();
			numOfVerticesAcrossAllGraphs += readVertexTypesAndAttributeNamesFromGraph(vertexTypesHistogram, tempVertexAttrFreqMap, attrDistributionMap, vertexTypesToInDegreesMap, graphLoader, edgeTypesHistogram);
			numOfEdgesAcrossAllGraphs += readEdgesInfoFromGraph(edgeTypesHistogram, graphLoader);
			printWithTime("Single graph read", (System.currentTimeMillis() - graphReadTime));
		}

		this.setNumOfVerticesInAllGraphs(numOfVerticesAcrossAllGraphs);
		this.setNumOfEdgesInAllGraphs(numOfEdgesAcrossAllGraphs);

		this.printVertexAndEdgeStatisticsForEntireTemporalGraph(timestampToPathsMap, vertexTypesHistogram);

		final long superVertexHandlingTime = System.currentTimeMillis();
		// TO-DO: What is the best way to estimate a good value for SUPER_VERTEX_DEGREE for each run?
//		this.calculateAverageInDegree(vertexTypesToInDegreesMap);
		this.calculateMedianInDegree(vertexTypesToInDegreesMap);
//		this.calculateMaxInDegree(vertexTypesToInDegreesMap);
		if (this.isDissolveSuperVertexTypes()) {
			if (this.getGraphs().size() > 0) {
				System.out.println("Collapsing vertices with an in-degree above " + this.getSuperVertexDegree());
				this.dissolveSuperVerticesAndUpdateHistograms(tempVertexAttrFreqMap, attrDistributionMap, vertexTypesToInDegreesMap, edgeTypesHistogram);
				printWithTime("Super vertex dissolution", (System.currentTimeMillis() - superVertexHandlingTime));
			}
		}

		this.setActiveAttributeSet(attrDistributionMap);

		this.setVertexTypesToAttributesMap(tempVertexAttrFreqMap);

		System.out.println("Number of edges across all graphs: " + this.getNumOfEdgesInAllGraphs());
		System.out.println("Number of edges labels across all graphs: " + edgeTypesHistogram.size());
		this.setSortedFrequentEdgeHistogram(edgeTypesHistogram, vertexTypesHistogram);

		this.findAndSetNumOfSnapshots();

		this.setTotalHistogramTime(System.currentTimeMillis() - histogramTime);
		printWithTime("All snapshots histogram", getTotalHistogramTime());
		printHistogram();
		printHistogramStatistics();
	}

	public void printHistogram() {

		System.out.println("Number of vertex types: " + this.getSortedVertexHistogram().size());
		System.out.println("Frequent Vertices:");
		for (Entry<String, Integer> entry : this.getSortedVertexHistogram()) {
			String vertexType = entry.getKey();
			Set<String> attributes = this.vertexTypesToAttributesMap.get(vertexType);
			System.out.println(vertexType + "={count=" + entry.getValue() + ", support=" + (1.0 * entry.getValue() / this.getNumOfVerticesInAllGraphs()) + ", attributes=" + attributes + "}");
		}

		System.out.println();
		System.out.println("Size of active attributes set: " + this.activeAttributesSet.size());
		System.out.println("Attributes:");
		for (String attrName : this.activeAttributesSet) {
			System.out.println(attrName);
		}
		System.out.println();
		System.out.println("Number of edge types: " + this.sortedFrequentEdgesHistogram.size());
		System.out.println("Frequent Edges:");
		for (Entry<String, Integer> entry : this.sortedFrequentEdgesHistogram) {
			System.out.println("edge=\"" + entry.getKey() + "\", count=" + entry.getValue() + ", support=" +(1.0 * entry.getValue() / this.getNumOfEdgesInAllGraphs()));
		}
		System.out.println();
	}

	public static Map<Set<ConstantLiteral>, ArrayList<Entry<ConstantLiteral, List<Integer>>>> findEntities(AttributeDependency attributes, List<Set<Set<ConstantLiteral>>> matchesPerTimestamps) {
		String yVertexType = attributes.getRhs().getVertexType();
		String yAttrName = attributes.getRhs().getAttrName();
		Set<ConstantLiteral> xAttributes = attributes.getLhs();
		Map<Set<ConstantLiteral>, Map<ConstantLiteral, List<Integer>>> entitiesWithRHSvalues = new HashMap<>();

		for (int timestamp = 0; timestamp < matchesPerTimestamps.size(); timestamp++) {
			Set<Set<ConstantLiteral>> matchesInOneTimeStamp = matchesPerTimestamps.get(timestamp);
			System.out.println("---------- Attribute values in t = " + timestamp + " ---------- ");
			int numOfMatches = 0;
			if (matchesInOneTimeStamp.size() > 0) {
				for(Set<ConstantLiteral> match : matchesInOneTimeStamp) {
					if (match.size() < attributes.size()) continue;
					Set<ConstantLiteral> entity = new HashSet<>();
					ConstantLiteral rhs = null;
					for (ConstantLiteral literalInMatch : match) {
						if (literalInMatch.getVertexType().equals(yVertexType) && literalInMatch.getAttrName().equals(yAttrName)) {
							rhs = new ConstantLiteral(literalInMatch.getVertexType(), literalInMatch.getAttrName(), literalInMatch.getAttrValue());
							continue;
						}
						for (ConstantLiteral attribute : xAttributes) {
							if (literalInMatch.getVertexType().equals(attribute.getVertexType()) && literalInMatch.getAttrName().equals(attribute.getAttrName())) {
								entity.add(new ConstantLiteral(literalInMatch.getVertexType(), literalInMatch.getAttrName(), literalInMatch.getAttrValue()));
							}
						}
					}
					if (entity.size() < xAttributes.size() || rhs == null) continue;

					if (!entitiesWithRHSvalues.containsKey(entity)) {
						entitiesWithRHSvalues.put(entity, new HashMap<>());
					}
					if (!entitiesWithRHSvalues.get(entity).containsKey(rhs)) {
						entitiesWithRHSvalues.get(entity).put(rhs, createEmptyArrayListOfSize(matchesPerTimestamps.size()));
					}
					entitiesWithRHSvalues.get(entity).get(rhs).set(timestamp, entitiesWithRHSvalues.get(entity).get(rhs).get(timestamp)+1);
					numOfMatches++;
				}
			}
			System.out.println("Number of matches: " + numOfMatches);
		}
		if (entitiesWithRHSvalues.size() == 0) return null;

		Comparator<Entry<ConstantLiteral, List<Integer>>> comparator = new Comparator<Entry<ConstantLiteral, List<Integer>>>() {
			@Override
			public int compare(Entry<ConstantLiteral, List<Integer>> o1, Entry<ConstantLiteral, List<Integer>> o2) {
				return o2.getValue().stream().reduce(0, Integer::sum) - o1.getValue().stream().reduce(0, Integer::sum);
			}
		};

		Map<Set<ConstantLiteral>, ArrayList<Map.Entry<ConstantLiteral, List<Integer>>>> entitiesWithSortedRHSvalues = new HashMap<>();
		for (Set<ConstantLiteral> entity : entitiesWithRHSvalues.keySet()) {
			Map<ConstantLiteral, List<Integer>> rhsMapOfEntity = entitiesWithRHSvalues.get(entity);
			ArrayList<Map.Entry<ConstantLiteral, List<Integer>>> sortedRhsMapOfEntity = new ArrayList<>(rhsMapOfEntity.entrySet());
			sortedRhsMapOfEntity.sort(comparator);
			entitiesWithSortedRHSvalues.put(entity, sortedRhsMapOfEntity);
		}

		return entitiesWithSortedRHSvalues;
	}

	public HashSet<ConstantLiteral> getActiveAttributesInPattern(Set<Vertex> vertexSet, boolean considerURI) {
		HashMap<String, HashSet<String>> patternVerticesAttributes = new HashMap<>();
		for (Vertex vertex : vertexSet) {
			for (String vertexType : vertex.getTypes()) {
				patternVerticesAttributes.put(vertexType, new HashSet<>());
				Set<String> attrNameSet = this.vertexTypesToAttributesMap.get(vertexType);
				for (String attrName : attrNameSet) {
					patternVerticesAttributes.get(vertexType).add(attrName);
				}
			}
		}
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

	public boolean isPathVisited(AttributeDependency path, HashSet<AttributeDependency> visitedPaths) {
		long visitedPathCheckingTime = System.currentTimeMillis();
		boolean pathAlreadyVisited = visitedPaths.contains(path);
		visitedPathCheckingTime = System.currentTimeMillis() - visitedPathCheckingTime;
		printWithTime("visitedPathChecking", visitedPathCheckingTime);
		addToTotalVisitedPathCheckingTime(visitedPathCheckingTime);
		return pathAlreadyVisited;
	}

	public ArrayList<TGFD> deltaDiscovery(PatternTreeNode patternNode, LiteralTreeNode literalTreeNode, AttributeDependency literalPath, List<Set<Set<ConstantLiteral>>> matchesPerTimestamps) {
		ArrayList<TGFD> tgfds = new ArrayList<>();

		// Add dependency attributes to pattern
		// TO-DO: Fix - when multiple vertices in a pattern have the same type, attribute values get overwritten
		VF2PatternGraph patternForDependency = patternNode.getPattern().copy();
		Set<ConstantLiteral> attributesSetForDependency = new HashSet<>(literalPath.getLhs());
		attributesSetForDependency.add(literalPath.getRhs());
		for (Vertex v : patternForDependency.getPattern().vertexSet()) {
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
		Map<Set<ConstantLiteral>, ArrayList<Entry<ConstantLiteral, List<Integer>>>> entities = findEntities(literalPath, matchesPerTimestamps);
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
		Map<Pair,ArrayList<TreeSet<Pair>>> deltaToPairsMap = new HashMap<>();
		ArrayList<TGFD> constantTGFDs = discoverConstantTGFDs(patternNode, literalPath.getRhs(), entities, deltaToPairsMap);
		// TO-DO: Try discover general TGFD even if no constant TGFD candidate met support threshold
		System.out.println("Constant TGFDs discovered: " + constantTGFDs.size());
		tgfds.addAll(constantTGFDs);

		System.out.println("Discovering general TGFDs");

		// Find general TGFDs
		if (!deltaToPairsMap.isEmpty()) {
			long discoverGeneralTGFDTime = System.currentTimeMillis();
			ArrayList<TGFD> generalTGFDs = discoverGeneralTGFD(patternNode, patternNode.getPatternSupport(), literalPath, entities.size(), deltaToPairsMap, literalTreeNode);
			discoverGeneralTGFDTime = System.currentTimeMillis() - discoverGeneralTGFDTime;
			printWithTime("discoverGeneralTGFDTime", discoverGeneralTGFDTime);
			addToTotalDiscoverGeneralTGFDTime(discoverGeneralTGFDTime);
			if (generalTGFDs.size() > 0) {
				System.out.println("Discovered " + generalTGFDs.size() + " general TGFDs for this dependency.");
				if (this.hasMinimalityPruning()) {
					literalTreeNode.setIsPruned();
					System.out.println("Marked as pruned. Literal path " + literalTreeNode.getPathToRoot());
					patternNode.addMinimalDependency(literalPath);
				}
			}
			tgfds.addAll(generalTGFDs);
		}

		return tgfds;
	}

	private ArrayList<TGFD> discoverGeneralTGFD(PatternTreeNode patternTreeNode, double patternSupport, AttributeDependency literalPath, int entitiesSize, Map<Pair, ArrayList<TreeSet<Pair>>> deltaToPairsMap, LiteralTreeNode literalTreeNode) {

		ArrayList<TGFD> tgfds = new ArrayList<>();

		System.out.println("Number of delta: " + deltaToPairsMap.keySet().size());
		for (Pair deltaPair : deltaToPairsMap.keySet()) {
			System.out.println("constant delta: " + deltaPair);
		}

		System.out.println("Delta to Pairs map...");
		int numOfEntitiesWithDeltas = 0;
		int numOfPairs = 0;
		for (Entry<Pair, ArrayList<TreeSet<Pair>>> deltaToPairsEntry : deltaToPairsMap.entrySet()) {
			numOfEntitiesWithDeltas += deltaToPairsEntry.getValue().size();
			for (TreeSet<Pair> pairSet : deltaToPairsEntry.getValue()) {
				System.out.println(deltaToPairsEntry.getKey()+":"+pairSet);
				numOfPairs += pairSet.size();
			}
		}
		System.out.println("Number of entities with deltas: " + numOfEntitiesWithDeltas);
		System.out.println("Number of pairs: " + numOfPairs);


		// Find intersection delta
		HashMap<Pair, ArrayList<Pair>> intersections = new HashMap<>();
		int currMin = 0;
		int currMax = this.getNumOfSnapshots() - 1;
		// TO-DO: Verify if TreeSet<Pair> is being sorted correctly
		// TO-DO: Does this method only produce intervals (x,y), where x == y ?
		ArrayList<Pair> currSatisfyingAttrValues = new ArrayList<>();
		for (Pair deltaPair: deltaToPairsMap.keySet()) {
			if (Math.max(currMin, deltaPair.min()) <= Math.min(currMax, deltaPair.max())) {
				currMin = Math.max(currMin, deltaPair.min());
				currMax = Math.min(currMax, deltaPair.max());
//				currSatisfyingAttrValues.add(satisfyingPairsSet.get(index)); // By axiom 4
				continue;
			}
			for (Entry<Pair, ArrayList<TreeSet<Pair>>> deltaToPairsEntry : deltaToPairsMap.entrySet()) {
				for (TreeSet<Pair> satisfyingPairSet : deltaToPairsEntry.getValue()) {
					for (Pair satisfyingPair : satisfyingPairSet) {
						if (satisfyingPair.max() - satisfyingPair.min() >= currMin && satisfyingPair.max() - satisfyingPair.min() <= currMax) {
							currSatisfyingAttrValues.add(new Pair(satisfyingPair.min(), satisfyingPair.max()));
						}
					}
				}
			}
			intersections.putIfAbsent(new Pair(currMin, currMax), currSatisfyingAttrValues);
			currSatisfyingAttrValues = new ArrayList<>();
			currMin = 0;
			currMax = this.getNumOfSnapshots() - 1;
			if (Math.max(currMin, deltaPair.min()) <= Math.min(currMax, deltaPair.max())) {
				currMin = Math.max(currMin, deltaPair.min());
				currMax = Math.min(currMax, deltaPair.max());
//				currSatisfyingAttrValues.add(satisfyingPairsSet.get(index));
			}
		}
		for (Entry<Pair, ArrayList<TreeSet<Pair>>> deltaToPairsEntry : deltaToPairsMap.entrySet()) {
			for (TreeSet<Pair> satisfyingPairSet : deltaToPairsEntry.getValue()) {
				for (Pair satisfyingPair : satisfyingPairSet) {
					if (satisfyingPair.max() - satisfyingPair.min() >= currMin && satisfyingPair.max() - satisfyingPair.min() <= currMax) {
						currSatisfyingAttrValues.add(new Pair(satisfyingPair.min(), satisfyingPair.max()));
					}
				}
			}
		}
		intersections.putIfAbsent(new Pair(currMin, currMax), currSatisfyingAttrValues);

		ArrayList<Entry<Pair, ArrayList<Pair>>> sortedIntersections = new ArrayList<>(intersections.entrySet());
		sortedIntersections.sort(new Comparator<Entry<Pair, ArrayList<Pair>>>() {
			@Override
			public int compare(Entry<Pair, ArrayList<Pair>> o1, Entry<Pair, ArrayList<Pair>> o2) {
				return o2.getValue().size() - o1.getValue().size();
			}
		});

		System.out.println("Candidate deltas for general TGFD:");
		for (Entry<Pair, ArrayList<Pair>> intersection : sortedIntersections) {
			System.out.println(intersection.getKey());
		}

		System.out.println("Evaluating candidate deltas for general TGFD...");
		for (Entry<Pair, ArrayList<Pair>> intersection : sortedIntersections) {
			Pair candidateDelta = intersection.getKey();
			int generalMin = candidateDelta.min();
			int generalMax = candidateDelta.max();
			System.out.println("Calculating support for candidate general TGFD candidate delta: " + intersection.getKey());

			// Compute general support
			int numberOfSatisfyingPairs = intersection.getValue().size();

			System.out.println("Number of satisfying pairs: " + numberOfSatisfyingPairs);
			System.out.println("Satisfying pairs: " + intersection.getValue());
			double support = this.calculateSupport(numberOfSatisfyingPairs, entitiesSize);
			System.out.println("Candidate general TGFD support: " + support);
			this.generalTgfdSupportsListForThisSnapshot.add(support);

			Delta delta = new Delta(Period.ofYears(generalMin), Period.ofYears(generalMax), Duration.ofDays(365));

			Dependency generalDependency = new Dependency();
			String yVertexType = literalPath.getRhs().getVertexType();
			String yAttrName = literalPath.getRhs().getAttrName();
			VariableLiteral y = new VariableLiteral(yVertexType, yAttrName, yVertexType, yAttrName);
			generalDependency.addLiteralToY(y);
			for (ConstantLiteral x : literalPath.getLhs()) {
				String xVertexType = x.getVertexType();
				String xAttrName = x.getAttrName();
				VariableLiteral varX = new VariableLiteral(xVertexType, xAttrName, xVertexType, xAttrName);
				generalDependency.addLiteralToX(varX);
			}

			if (support < this.getTgfdTheta()) {
				if (this.hasSupportPruning() && delta.getMin().getYears() == 0 && delta.getMax().getYears() == this.getNumOfSnapshots()-1) {
					literalTreeNode.setIsPruned();
					patternTreeNode.addLowSupportDependency(new AttributeDependency(literalPath.getLhs(), literalPath.getRhs(), delta));
				}
				System.out.println("Support for candidate general TGFD is below support threshold");
//				continue;
			} else {
				System.out.println("Creating new general TGFD...");
				TGFD tgfd = new TGFD(patternTreeNode.getPattern(), delta, generalDependency, support, patternSupport, "");
				System.out.println("TGFD: " + tgfd);
				tgfds.add(tgfd);
			}
		}
		return tgfds;
	}

	private boolean isSupersetPath(AttributeDependency literalPath, Pair candidateDelta, HashMap<AttributeDependency, ArrayList<Pair>> lowSupportGeneralTgfdList) {
		for (Map.Entry<AttributeDependency, ArrayList<Pair>> lowSupportGeneralTgfdEntry: lowSupportGeneralTgfdList.entrySet()) {
			AttributeDependency lowSupportGeneralTgfdDependencyPath = lowSupportGeneralTgfdEntry.getKey();
			ArrayList<Pair> lowSupportGeneralTgfdDeltaPairList = lowSupportGeneralTgfdEntry.getValue();
			for (Pair lowSupportGeneralTgfdDeltaPair : lowSupportGeneralTgfdDeltaPairList) {
				if (literalPath.getRhs().equals(lowSupportGeneralTgfdDependencyPath.getRhs()) && literalPath.getLhs().containsAll(lowSupportGeneralTgfdDependencyPath.getLhs())) {
					System.out.println("Candidate path " + literalPath + " is a superset of pruned path " + lowSupportGeneralTgfdDependencyPath);
					if (candidateDelta.min() >= lowSupportGeneralTgfdDeltaPair.min() &&  candidateDelta.max() <= lowSupportGeneralTgfdDeltaPair.max()) {
						System.out.println("The candidate general dependency " + literalPath
								+ "\n with candidate delta " + candidateDelta
								+ "\n is a superset of a minimal dependency " + lowSupportGeneralTgfdDependencyPath
								+ "\n with minimal delta " + lowSupportGeneralTgfdDeltaPair
								+ ".");
						return true;
					}
				}
			}
		}
		return false;
	}

	private ArrayList<TGFD> discoverConstantTGFDs(PatternTreeNode patternNode, ConstantLiteral yLiteral, Map<Set<ConstantLiteral>, ArrayList<Entry<ConstantLiteral, List<Integer>>>> entities, Map<Pair, ArrayList<TreeSet<Pair>>> deltaToPairsMap) {
		long discoverConstantTGFDsTime = System.currentTimeMillis();
		ArrayList<TGFD> tgfds = new ArrayList<>();
		String yVertexType = yLiteral.getVertexType();
		String yAttrName = yLiteral.getAttrName();
		long supersetPathCheckingTimeForThisTGFD = 0;
		for (Entry<Set<ConstantLiteral>, ArrayList<Entry<ConstantLiteral, List<Integer>>>> entityEntry : entities.entrySet()) {
			VF2PatternGraph newPattern = patternNode.getPattern().copy();
			Dependency newDependency = new Dependency();
			AttributeDependency constantPath = new AttributeDependency();
			for (Vertex v : newPattern.getPattern().vertexSet()) {
				String vType = new ArrayList<>(v.getTypes()).get(0);
				if (vType.equalsIgnoreCase(yVertexType)) { // TO-DO: What if our pattern has duplicate vertex types?
					v.putAttributeIfAbsent(new Attribute(yAttrName));
					if (newDependency.getY().size() == 0) {
						VariableLiteral newY = new VariableLiteral(yVertexType, yAttrName, yVertexType, yAttrName);
						newDependency.addLiteralToY(newY);
					}
				}
				for (ConstantLiteral xLiteral : entityEntry.getKey()) {
					if (xLiteral.getVertexType().equalsIgnoreCase(vType)) {
						v.putAttributeIfAbsent(new Attribute(xLiteral.getAttrName(), xLiteral.getAttrValue()));
						ConstantLiteral newXLiteral = new ConstantLiteral(vType, xLiteral.getAttrName(), xLiteral.getAttrValue());
						newDependency.addLiteralToX(newXLiteral);
						constantPath.addToLhs(newXLiteral);
					}
				}
			}
			constantPath.setRhs(new ConstantLiteral(yVertexType, yAttrName, null));

			System.out.println("Performing Constant TGFD discovery");
			System.out.println("Pattern: " + newPattern);
			System.out.println("Entity: " + newDependency);

			System.out.println("Candidate RHS values for entity...");
			ArrayList<Entry<ConstantLiteral, List<Integer>>> rhsAttrValuesTimestampsSortedByFreq = entityEntry.getValue();
			for (Map.Entry<ConstantLiteral, List<Integer>> entry : rhsAttrValuesTimestampsSortedByFreq) {
				System.out.println(entry.getKey() + ":" + entry.getValue());
			}

            System.out.println("Computing candidate delta for RHS value...\n" + rhsAttrValuesTimestampsSortedByFreq.get(0).getKey());
            ArrayList<Pair> candidateDeltas = new ArrayList<>();
            if (rhsAttrValuesTimestampsSortedByFreq.size() == 1) {
				List<Integer> timestampCounts = rhsAttrValuesTimestampsSortedByFreq.get(0).getValue();
				Pair candidateDelta = getMinMaxPair(timestampCounts);
				if (candidateDelta == null) continue;
				candidateDeltas.add(candidateDelta);
			} else if (rhsAttrValuesTimestampsSortedByFreq.size() > 1) {
				findCandidateDeltasForMostFreqRHS(rhsAttrValuesTimestampsSortedByFreq, candidateDeltas);
			}
			if (candidateDeltas.size() == 0) {
				System.out.println("Could not find any deltas for entity: " + entityEntry.getKey());
				continue;
			}

			// Compute TGFD support
			Delta candidateTGFDdelta;
			double candidateTGFDsupport = 0;
			Pair mostSupportedDelta = null;
			TreeSet<Pair> mostSupportedSatisfyingPairs = null;
			for (Pair candidateDelta : candidateDeltas) {
				int minDistance = candidateDelta.min();
				int maxDistance = candidateDelta.max();
				if (minDistance <= maxDistance) {
					System.out.println("Calculating support for candidate delta ("+minDistance+","+maxDistance+")");
					double numerator;
					List<Integer> timestampCounts = rhsAttrValuesTimestampsSortedByFreq.get(0).getValue();
					TreeSet<Pair> satisfyingPairs = new TreeSet<>();
					for (int index = 0; index < timestampCounts.size(); index++) {
						if (timestampCounts.get(index) == 0)
							continue;
						else if (timestampCounts.get(index) > 1 && 0 >= minDistance && 0 <= maxDistance)
							satisfyingPairs.add(new Pair(index, index));
						for (int j = index + 1; j < timestampCounts.size(); j++) {
							if (timestampCounts.get(j) > 0) {
								if (j - index >= minDistance && j - index <= maxDistance) {
									satisfyingPairs.add(new Pair(index, j));
								}
							}
						}
					}

					System.out.println("Satisfying pairs: " + satisfyingPairs);

					numerator = satisfyingPairs.size();
					double candidateSupport = this.calculateSupport(numerator, entities.size());

					if (candidateSupport > candidateTGFDsupport) {
						candidateTGFDsupport = candidateSupport;
						mostSupportedDelta = candidateDelta;
						mostSupportedSatisfyingPairs = satisfyingPairs;
					}
				}
			}
			if (mostSupportedDelta == null) {
				System.out.println("Could not come up with mostSupportedDelta for entity: " + entityEntry.getKey());
				continue;
			}
			System.out.println("Entity satisfying attributes:" + mostSupportedSatisfyingPairs);
			System.out.println("Entity delta = " + mostSupportedDelta);
			System.out.println("Entity support = " + candidateTGFDsupport);

			// All entities are considered in general TGFD, regardless of their support
			if (!deltaToPairsMap.containsKey(mostSupportedDelta)) {
				deltaToPairsMap.put(mostSupportedDelta, new ArrayList<>());
			}
			deltaToPairsMap.get(mostSupportedDelta).add(mostSupportedSatisfyingPairs);

			this.constantTgfdSupportsListForThisSnapshot.add(candidateTGFDsupport); // Statistics

			int minDistance = mostSupportedDelta.min();
			int maxDistance = mostSupportedDelta.max();
			candidateTGFDdelta = new Delta(Period.ofYears(minDistance), Period.ofYears(maxDistance), Duration.ofDays(365));
			System.out.println("Constant TGFD delta: "+candidateTGFDdelta);
			constantPath.setDelta(candidateTGFDdelta);

			long supersetPathCheckingTime = System.currentTimeMillis();
			if (this.hasMinimalityPruning() && constantPath.isSuperSetOfPathAndSubsetOfDelta(patternNode.getAllMinimalConstantDependenciesOnThisPath())) { // Ensures we don't expand constant TGFDs from previous iterations
				System.out.println("Candidate constant TGFD " + constantPath + " is a superset of an existing minimal constant TGFD");
				continue;
			}
			supersetPathCheckingTime = System.currentTimeMillis()-supersetPathCheckingTime;
			supersetPathCheckingTimeForThisTGFD += supersetPathCheckingTime;
			printWithTime("supersetPathCheckingTime", supersetPathCheckingTime);
			addToTotalSupersetPathCheckingTime(supersetPathCheckingTime);

			// Only output constant TGFDs that satisfy support
			if (candidateTGFDsupport < this.getTgfdTheta()) {
				if (this.hasSupportPruning() && candidateTGFDdelta.getMin().getYears() == 0 && candidateTGFDdelta.getMax().getYears() == this.getNumOfSnapshots()-1)
					patternNode.addLowSupportDependency(new AttributeDependency(constantPath.getLhs(), constantPath.getRhs(), candidateTGFDdelta));
				System.out.println("Could not satisfy TGFD support threshold for entity: " + entityEntry.getKey());
//				continue;
			} else {
				System.out.println("Creating new constant TGFD...");
				TGFD entityTGFD = new TGFD(newPattern, candidateTGFDdelta, newDependency, candidateTGFDsupport, patternNode.getPatternSupport(), "");
				System.out.println("TGFD: " + entityTGFD);
				tgfds.add(entityTGFD);
				if (this.hasMinimalityPruning()) patternNode.addMinimalConstantDependency(constantPath);
			}
		}

		discoverConstantTGFDsTime = System.currentTimeMillis() - discoverConstantTGFDsTime - supersetPathCheckingTimeForThisTGFD;
		printWithTime("discoverConstantTGFDsTime", discoverConstantTGFDsTime);
		addToTotalDiscoverConstantTGFDsTime(discoverConstantTGFDsTime);

		return tgfds;
	}

	private void findCandidateDeltasForMostFreqRHS(ArrayList<Entry<ConstantLiteral, List<Integer>>> rhsAttrValuesTimestampsSortedByFreq, ArrayList<Pair> candidateDeltas) {
		List<Integer> timestampCountOfMostFreqRHS = rhsAttrValuesTimestampsSortedByFreq.get(0).getValue();
		Integer minExclusionDistance = null;
		Integer maxExclusionDistance = null;
		for (Entry<ConstantLiteral, List<Integer>> timestampCountEntryOfOtherRHS : rhsAttrValuesTimestampsSortedByFreq.subList(1, rhsAttrValuesTimestampsSortedByFreq.size())) {
			List<Integer> timestampCountOfOtherRHS = timestampCountEntryOfOtherRHS.getValue();
			for (int i = 0; i < timestampCountOfOtherRHS.size(); i++) {
				int otherTimestampCount = timestampCountOfOtherRHS.get(i);
				if (otherTimestampCount == 0)
					continue;
				for (int j = 0; j < timestampCountOfMostFreqRHS.size(); j++) {
					Integer refTimestampCount = timestampCountOfMostFreqRHS.get(j);
					if (refTimestampCount == 0)
						continue;
					int distance = Math.abs(i - j);
					minExclusionDistance = minExclusionDistance != null ? Math.min(minExclusionDistance, distance) : distance;
					maxExclusionDistance = maxExclusionDistance != null ? Math.max(maxExclusionDistance, distance) : distance;
				}
			}
			if (minExclusionDistance != null && minExclusionDistance == 0
					&& maxExclusionDistance != null && maxExclusionDistance == (this.getNumOfSnapshots()-1))
				break;
		}
		if (minExclusionDistance != null && minExclusionDistance > 0) {
			candidateDeltas.add(new Pair(0, minExclusionDistance-1));
		}
		if (maxExclusionDistance != null && maxExclusionDistance < this.getNumOfSnapshots()-1) {
			candidateDeltas.add(new Pair(maxExclusionDistance+1, this.getNumOfSnapshots()-1));
		}
	}

	@Nullable
	private Pair getMinMaxPair(List<Integer> timestampCounts) {
		Integer minDistance = null;
		Integer maxDistance = null;
		if (timestampCounts.stream().anyMatch(count -> count > 1)) {
			minDistance = 0;
		} else {
			Integer indexOfPreviousOccurence = null;
			for (int index = 0; index < timestampCounts.size(); index++) {
				if (timestampCounts.get(index) > 0) {
					if (indexOfPreviousOccurence == null) {
						indexOfPreviousOccurence = index;
					} else {
						minDistance = minDistance != null ? Math.min(minDistance, index - indexOfPreviousOccurence) : (index - indexOfPreviousOccurence);
						if (minDistance == 0) break;
					}
				}
			}
		}
		if (minDistance == null)
			return null;
		Integer indexOfFirstOccurence = null;
		for (int index = 0; index < timestampCounts.size(); index++) {
			if (timestampCounts.get(index) > 0) {
				indexOfFirstOccurence = index;
				break;
			}
		}
		Integer indexOfFinalOccurence = null;
		for (int index = timestampCounts.size()-1; index >= 0; index--) {
			if (timestampCounts.get(index) > 0) {
				indexOfFinalOccurence = index;
				break;
			}
		}
		if (indexOfFirstOccurence != null && indexOfFinalOccurence != null) {
			if (indexOfFirstOccurence.equals(indexOfFinalOccurence) && timestampCounts.get(indexOfFirstOccurence) > 1) {
				maxDistance = 0;
			} else {
				maxDistance = indexOfFinalOccurence - indexOfFirstOccurence;
			}
		}
		if (maxDistance == null)
			return null;
		if (minDistance > maxDistance) {
			System.out.println("Not enough timestamped matches found for entity.");
			return null;
		}
		return new Pair(minDistance, maxDistance);
	}

	public ArrayList<TGFD> getDummyVertexTypeTGFDs() {
		ArrayList<TGFD> dummyTGFDs = new ArrayList<>();
		for (Map.Entry<String,Integer> frequentVertexTypeEntry : this.getSortedVertexHistogram()) {
			String frequentVertexType = frequentVertexTypeEntry.getKey();
			VF2PatternGraph patternGraph = new VF2PatternGraph();
			PatternVertex patternVertex = new PatternVertex(frequentVertexType);
			patternGraph.addVertex(patternVertex);
//			HashSet<ConstantLiteral> activeAttributes = getActiveAttributesInPattern(patternGraph.getPattern().vertexSet());
//			for (ConstantLiteral activeAttribute: activeAttributes) {
				TGFD dummyTGFD = new TGFD();
				dummyTGFD.setName(frequentVertexType);
				dummyTGFD.setPattern(patternGraph);
//				Dependency dependency = new Dependency();
//				dependency.addLiteralToY(activeAttribute);
				dummyTGFDs.add(dummyTGFD);
//			}
		}
		return dummyTGFDs;
	}

	public ArrayList<TGFD> getDummyEdgeTypeTGFDs() {
		ArrayList<TGFD> dummyTGFDs = new ArrayList<>();

		for (Map.Entry<String,Integer> frequentEdgeEntry : this.sortedFrequentEdgesHistogram) {
			String frequentEdge = frequentEdgeEntry.getKey();
			String[] info = frequentEdge.split(" ");
			String sourceVertexType = info[0];
			String edgeType = info[1];
			String targetVertexType = info[2];
			VF2PatternGraph patternGraph = new VF2PatternGraph();
			PatternVertex sourceVertex = new PatternVertex(sourceVertexType);
			patternGraph.addVertex(sourceVertex);
			PatternVertex targetVertex = new PatternVertex(targetVertexType);
			patternGraph.addVertex(targetVertex);
			RelationshipEdge edge = new RelationshipEdge(edgeType);
			patternGraph.addEdge(sourceVertex, targetVertex, edge);
//			HashSet<ConstantLiteral> activeAttributes = getActiveAttributesInPattern(patternGraph.getPattern().vertexSet());
//			for (ConstantLiteral activeAttribute: activeAttributes) {
				TGFD dummyTGFD = new TGFD();
				dummyTGFD.setName(frequentEdge.replaceAll(" ","_"));
				dummyTGFD.setPattern(patternGraph);
//				Dependency dependency = new Dependency();
//				dependency.addLiteralToY(activeAttribute);
				dummyTGFDs.add(dummyTGFD);
//			}
		}
		return dummyTGFDs;
	}

	public void setImdbTimestampToFilesMapFromPath(String path) {
		HashMap<String, List<String>> timestampToFilesMap = generateImdbTimestampToFilesMapFromPath(path);
		this.setTimestampToFilesMap(new ArrayList<>(timestampToFilesMap.entrySet()));
	}

	@NotNull
	public static HashMap<String, List<String>> generateImdbTimestampToFilesMapFromPath(String path) {
		System.out.println("Searching for IMDB snapshots in path: "+ path);
		List<File> allFilesInDirectory = new ArrayList<>(List.of(Objects.requireNonNull(new File(path).listFiles(File::isFile))));
		System.out.println("Found files: "+allFilesInDirectory);
		List<File> ntFilesInDirectory = new ArrayList<>();
		for (File ntFile: allFilesInDirectory) {
			System.out.println("Is this an .nt file? "+ntFile.getName());
			if (ntFile.getName().endsWith(".nt")) {
				System.out.println("Found .nt file: "+ntFile.getPath());
				ntFilesInDirectory.add(ntFile);
			}
		}
		ntFilesInDirectory.sort(Comparator.comparing(File::getName));
		System.out.println("Found .nt files: "+ntFilesInDirectory);
		HashMap<String,List<String>> timestampToFilesMap = new HashMap<>();
		for (File ntFile: ntFilesInDirectory) {
			String regex = "^imdb-([0-9]+)\\.nt$";
			Pattern pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(ntFile.getName());
			if (matcher.find()) {
				String timestamp = matcher.group(1);
				timestampToFilesMap.putIfAbsent(timestamp, new ArrayList<>());
				timestampToFilesMap.get(timestamp).add(ntFile.getPath());
			}
		}
		System.out.println("TimestampToFilesMap...");
		System.out.println(timestampToFilesMap);
		return timestampToFilesMap;
	}

	protected void loadChangeFilesIntoMemory() {
		HashMap<String, org.json.simple.JSONArray> changeFilesMap = new HashMap<>();
		if (this.isUseTypeChangeFile()) {
			for (Map.Entry<String,Integer> frequentVertexTypeEntry : this.vertexHistogram.entrySet()) {
				for (int i = 0; i < this.getTimestampToFilesMap().size() - 1; i++) {
					System.out.println("-----------Snapshot (" + (i + 2) + ")-----------");
					String changeFilePath = "changes_t" + (i + 1) + "_t" + (i + 2) + "_" + frequentVertexTypeEntry.getKey() + ".json";
					JSONArray jsonArray = readJsonArrayFromFile(changeFilePath);
					System.out.println("Storing " + changeFilePath + " in memory");
					changeFilesMap.put(changeFilePath, jsonArray);
				}
			}
		} else {
			for (int i = 0; i < this.getTimestampToFilesMap().size() - 1; i++) {
				System.out.println("-----------Snapshot (" + (i + 2) + ")-----------");
				String changeFilePath = "changes_t" + (i + 1) + "_t" + (i + 2) + "_" + this.getGraphSize() + ".json";
				JSONArray jsonArray = readJsonArrayFromFile(changeFilePath);
				System.out.println("Storing " + changeFilePath + " in memory");
				changeFilesMap.put(changeFilePath, jsonArray);
			}
		}
		this.setChangeFilesMap(changeFilesMap);
	}

	private JSONArray readJsonArrayFromFile(String changeFilePath) {
		System.out.println("Reading JSON array from file "+changeFilePath);
		JSONParser parser = new JSONParser();
		Object json;
		JSONArray jsonArray = new JSONArray();
		try {
			json = parser.parse(new FileReader(changeFilePath));
			jsonArray = (JSONArray) json;
		} catch (Exception e) {
			e.printStackTrace();
		}
		return jsonArray;
	}

	public void setDBpediaTimestampsAndFilePaths(String path) {
		Map<String, List<String>> timestampToFilesMap = generateDbpediaTimestampToFilesMap(path);
		this.setTimestampToFilesMap(new ArrayList<>(timestampToFilesMap.entrySet()));
	}

	@NotNull
	public static Map<String, List<String>> generateDbpediaTimestampToFilesMap(String path) {
		ArrayList<File> directories = new ArrayList<>(List.of(Objects.requireNonNull(new File(path).listFiles(File::isDirectory))));
		directories.sort(Comparator.comparing(File::getName));
		Map<String, List<String>> timestampToFilesMap = new HashMap<>();
		for (File directory: directories) {
			ArrayList<File> files = new ArrayList<>(List.of(Objects.requireNonNull(new File(directory.getPath()).listFiles(File::isFile))));
			List<String> paths = files.stream().map(File::getPath).collect(Collectors.toList());
			timestampToFilesMap.put(directory.getName(),paths);
		}
		return timestampToFilesMap;
	}

	public void setCitationTimestampsAndFilePaths() {
		ArrayList<String> filePaths = new ArrayList<>();
		filePaths.add("dblp_papers_v11.txt");
		filePaths.add("dblp.v12.json");
		filePaths.add("dblpv13.json");
		Map<String,List<String>> timestampstoFilePathsMap = new HashMap<>();
		int timestampName = 11;
		for (String filePath: filePaths) {
			timestampstoFilePathsMap.put(String.valueOf(timestampName), Collections.singletonList(filePath));
		}
		this.setTimestampToFilesMap(new ArrayList<>(timestampstoFilePathsMap.entrySet()));
	}

	public ArrayList<TGFD> hSpawn(PatternTreeNode patternTreeNode, List<Set<Set<ConstantLiteral>>> matchesPerTimestamps) {
		ArrayList<TGFD> tgfds = new ArrayList<>();

		System.out.println("Performing HSpawn for " + patternTreeNode.getPattern());

		HashSet<ConstantLiteral> activeAttributes = getActiveAttributesInPattern(patternTreeNode.getGraph().vertexSet(), false);

		LiteralTree literalTree = new LiteralTree();
		int hSpawnLimit;
		if (this.isOnlyInterestingTGFDs()) {
			hSpawnLimit = Math.max(patternTreeNode.getGraph().vertexSet().size(), this.getMaxNumOfLiterals());
		} else {
			hSpawnLimit = this.getMaxNumOfLiterals();
		}
		for (int j = 0; j < hSpawnLimit; j++) {

			System.out.println("HSpawn level " + j + "/" + hSpawnLimit);

			if (j == 0) {
				literalTree.addLevel();
				for (ConstantLiteral literal: activeAttributes) {
					literalTree.createNodeAtLevel(j, literal, null);
				}
			} else {
				ArrayList<LiteralTreeNode> literalTreePreviousLevel = literalTree.getLevel(j - 1);
				if (literalTreePreviousLevel.size() == 0) {
					System.out.println("Previous level of literal tree is empty. Nothing to expand. End HSpawn");
					break;
				}
				literalTree.addLevel();
				HashSet<AttributeDependency> visitedPaths = new HashSet<>(); //TO-DO: Can this be implemented as HashSet to improve performance?
				ArrayList<TGFD> currentLevelTGFDs = new ArrayList<>();
				for (LiteralTreeNode previousLevelLiteral : literalTreePreviousLevel) {
					System.out.println("Creating literal tree node " + literalTree.getLevel(j).size() + "/" + (literalTreePreviousLevel.size() * (literalTreePreviousLevel.size()-1)));
					if (previousLevelLiteral.isPruned()) {
						System.out.println("Could not expand pruned literal path "+previousLevelLiteral.getPathToRoot());
						continue;
					}
					for (ConstantLiteral literal: activeAttributes) {
						ArrayList<ConstantLiteral> parentsPathToRoot = previousLevelLiteral.getPathToRoot(); //TO-DO: Can this be implemented as HashSet to improve performance?
						if (this.isOnlyInterestingTGFDs() && j < patternTreeNode.getGraph().vertexSet().size()) { // Ensures all vertices are involved in dependency
							if (isUsedVertexType(literal.getVertexType(), parentsPathToRoot)) continue;
						}

						if (parentsPathToRoot.contains(literal)) continue;

						// Check if path to candidate leaf node is unique
						AttributeDependency newPath = new AttributeDependency(previousLevelLiteral.getPathToRoot(),literal);
						System.out.println("New candidate literal path: " + newPath);
						if (isPathVisited(newPath, visitedPaths)) { // TO-DO: Is this relevant anymore?
							System.out.println("Skip. Duplicate literal path.");
							continue;
						}

						long supersetPathCheckingTime = System.currentTimeMillis();
						if (this.hasSupportPruning() && newPath.isSuperSetOfPath(patternTreeNode.getZeroEntityDependenciesOnThisPath())) { // Ensures we don't re-explore dependencies whose subsets have no entities
							System.out.println("Skip. Candidate literal path is a superset of zero-entity dependency.");
							continue;
						}
						if (this.hasSupportPruning() && newPath.isSuperSetOfPath(patternTreeNode.getLowSupportDependenciesOnThisPath())) { // Ensures we don't re-explore dependencies whose subsets have no entities
							System.out.println("Skip. Candidate literal path is a superset of low-support dependency.");
							continue;
						}
						if (this.hasMinimalityPruning() && newPath.isSuperSetOfPath(patternTreeNode.getAllMinimalDependenciesOnThisPath())) { // Ensures we don't re-explore dependencies whose subsets have already have a general dependency
							System.out.println("Skip. Candidate literal path is a superset of minimal dependency.");
							continue;
						}
						supersetPathCheckingTime = System.currentTimeMillis()-supersetPathCheckingTime;
						printWithTime("supersetPathCheckingTime", supersetPathCheckingTime);
						addToTotalSupersetPathCheckingTime(supersetPathCheckingTime);

						System.out.println("Newly created unique literal path: " + newPath);

						// Add leaf node to tree
						LiteralTreeNode literalTreeNode = literalTree.createNodeAtLevel(j, literal, previousLevelLiteral);

						visitedPaths.add(newPath);

						if (this.isOnlyInterestingTGFDs()) { // Ensures all vertices are involved in dependency
                            if (literalPathIsMissingTypesInPattern(literalTreeNode.getPathToRoot(), patternTreeNode.getGraph().vertexSet()))
								continue;
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

	private static boolean isUsedVertexType(String vertexType, ArrayList<ConstantLiteral> parentsPathToRoot) {
		for (ConstantLiteral literal : parentsPathToRoot) {
			if (literal.getVertexType().equals(vertexType)) {
				return true;
			}
		}
		return false;
	}

	private static boolean literalPathIsMissingTypesInPattern(ArrayList<ConstantLiteral> parentsPathToRoot, Set<Vertex> patternVertexSet) {
		for (Vertex v : patternVertexSet) {
			boolean missingType = true;
			for (ConstantLiteral literal : parentsPathToRoot) {
				if (literal.getVertexType().equals(v.getTypes().iterator().next())) {
					missingType = false;
				}
			}
			if (missingType) return true;
		}
		return false;
	}

	public int getCurrentVSpawnLevel() {
		return currentVSpawnLevel;
	}

	public void setCurrentVSpawnLevel(int currentVSpawnLevel) {
		this.currentVSpawnLevel = currentVSpawnLevel;
	}

	public int getK() {
		return k;
	}

	public int getNumOfSnapshots() {
		return numOfSnapshots;
	}

	public double getTgfdTheta() {
		return tgfdTheta;
	}

	public List<Long> getTotalVSpawnTime() {
		return totalVSpawnTime;
	}

	public Long getTotalVSpawnTime(int index) {
		return index < this.getTotalVSpawnTime().size() ? this.getTotalVSpawnTime().get(index) : 0;
	}

	public void addToTotalVSpawnTime(long vSpawnTime) {
		addToValueInListAtIndex(this.getTotalVSpawnTime(), this.getCurrentVSpawnLevel(), vSpawnTime);
	}

	public List<Long> getTotalMatchingTime() {
		return totalMatchingTime;
	}

	public Long getTotalMatchingTime(int index) {
		return index < this.getTotalMatchingTime().size() ? this.getTotalMatchingTime().get(index) : 0;
	}

	public void addToTotalMatchingTime(long matchingTime) {
		addToValueInListAtIndex(this.getTotalMatchingTime(), this.getCurrentVSpawnLevel(), matchingTime);
	}

	public static void addToValueInListAtIndex(List<Long> list, int index, long valueToAdd) {
		while (list.size() <= index)
			list.add(0L);
		list.set(index, list.get(index)+valueToAdd);
	}

	public boolean hasSupportPruning() {
		return hasSupportPruning;
	}

	public void setSupportPruning(boolean hasSupportPruning) {
		this.hasSupportPruning = hasSupportPruning;
	}

	public boolean isSkipK1() {
		return skipK1;
	}

	public ArrayList<ArrayList<TGFD>> getTgfds() {
		return tgfds;
	}

	public void initializeTgfdLists() {
		this.tgfds = new ArrayList<>();
		for (int vSpawnLevel = 0; vSpawnLevel <= this.getK(); vSpawnLevel++) {
			getTgfds().add(new ArrayList<>());
		}
	}

	public boolean reUseMatches() {
		return reUseMatches;
	}

	public boolean isGeneratek0Tgfds() {
		return generatek0Tgfds;
	}

	public boolean useChangeFile() {
		return useChangeFile;
	}

	public void setUseChangeFile(boolean useChangeFile) {
		this.useChangeFile = useChangeFile;
	}

	public int getPreviousLevelNodeIndex() {
		return previousLevelNodeIndex;
	}

	public void setPreviousLevelNodeIndex(int previousLevelNodeIndex) {
		this.previousLevelNodeIndex = previousLevelNodeIndex;
	}

	public int getCandidateEdgeIndex() {
		return candidateEdgeIndex;
	}

	public void setCandidateEdgeIndex(int candidateEdgeIndex) {
		this.candidateEdgeIndex = candidateEdgeIndex;
	}

	public void findAndSetNumOfSnapshots() {
		if (this.useChangeFile()) {
			this.setNumOfSnapshots(this.getTimestampToFilesMap().size());
		} else {
			this.setNumOfSnapshots(this.getGraphs().size());
		}
		System.out.println("Number of "+this.getLoader()+" snapshots: "+this.getNumOfSnapshots());
	}

	public String getLoader() {
		return loader;
	}

	public void setLoader(String loader) {
		this.loader = loader;
	}

	public List<Entry<String, List<String>>> getTimestampToFilesMap() {
		return timestampToFilesMap;
	}

	public void setTimestampToFilesMap(List<Entry<String, List<String>>> timestampToFilesMap) {
		timestampToFilesMap.sort(Entry.comparingByKey());
		this.timestampToFilesMap = timestampToFilesMap.subList(0,Math.min(timestampToFilesMap.size(),this.getT()));
	}

	public int getT() {
		return t;
	}

	public void setT(int t) {
		this.t = t;
	}


	public boolean isValidationSearch() {
		return validationSearch;
	}

	public String getPath() {
		return path;
	}

	public void setPath(String path) {
		this.path = path;
	}

	public void setReUseMatches(boolean reUseMatches) {
		this.reUseMatches = reUseMatches;
	}

	public void setValidationSearch(boolean validationSearch) {
		this.validationSearch = validationSearch;
	}

	public long getStartTime() {
		return startTime;
	}

	public void setStartTime(long startTime) {
		this.startTime = startTime;
	}

	public boolean isDissolveSuperVertexTypes() {
		return dissolveSuperVertexTypes;
	}

	public void setDissolveSuperVertexTypes(boolean dissolveSuperVertexTypes) {
		this.dissolveSuperVertexTypes = dissolveSuperVertexTypes;
	}

	public boolean hasMinimalityPruning() {
		return hasMinimalityPruning;
	}

	public void setMinimalityPruning(boolean hasMinimalityPruning) {
		this.hasMinimalityPruning = hasMinimalityPruning;
	}

	public void setGeneratek0Tgfds(boolean generatek0Tgfds) {
		this.generatek0Tgfds = generatek0Tgfds;
	}

	public void setSkipK1(boolean skipK1) {
		this.skipK1 = skipK1;
	}

	public boolean isOnlyInterestingTGFDs() {
		return onlyInterestingTGFDs;
	}

	public void setOnlyInterestingTGFDs(boolean onlyInterestingTGFDs) {
		this.onlyInterestingTGFDs = onlyInterestingTGFDs;
	}

	public boolean iskExperiment() {
		return kExperiment;
	}

	public void setkExperiment(boolean kExperiment) {
		this.kExperiment = kExperiment;
	}

	public List<GraphLoader> getGraphs() {
		return graphs;
	}

	public void setGraphs(List<GraphLoader> graphs) {
		this.graphs = graphs;
	}

	public boolean isStoreInMemory() {
		return isStoreInMemory;
	}

	public void setStoreInMemory(boolean storeInMemory) {
		isStoreInMemory = storeInMemory;
	}

	public String getExperimentName() {
		return experimentName;
	}

	public void setExperimentName(String experimentName) {
		this.experimentName = experimentName;
	}

	public void setK(int k) {
		this.k = k;
	}

	public void setTgfdTheta(double tgfdTheta) {
		this.tgfdTheta = tgfdTheta;
	}

	public int getGamma() {
		return gamma;
	}

	public void setGamma(int gamma) {
		this.gamma = gamma;
	}

	public int getFrequentSetSize() {
		return frequentSetSize;
	}

	public void setFrequentSetSize(int frequentSetSize) {
		this.frequentSetSize = frequentSetSize;
	}

	public void setNumOfSnapshots(int numOfSnapshots) {
		this.numOfSnapshots = numOfSnapshots;
	}

	public HashMap<String, JSONArray> getChangeFilesMap() {
		return changeFilesMap;
	}

	public void setChangeFilesMap(HashMap<String, JSONArray> changeFilesMap) {
		this.changeFilesMap = changeFilesMap;
	}

	public String getGraphSize() {
		return graphSize;
	}

	public void setGraphSize(String graphSize) {
		this.graphSize = graphSize;
	}

	public ArrayList<Long> getkRuntimes() {
		return kRuntimes;
	}

	public String getTimeAndDateStamp() {
		return timeAndDateStamp;
	}

	public void setTimeAndDateStamp(String timeAndDateStamp) {
		this.timeAndDateStamp = timeAndDateStamp;
	}

	public boolean isUseTypeChangeFile() {
		return useTypeChangeFile;
	}

	public void setUseTypeChangeFile(boolean useTypeChangeFile) {
		this.useTypeChangeFile = useTypeChangeFile;
	}

	public List<Entry<String, Integer>> getSortedVertexHistogram() {
		return sortedVertexHistogram;
	}

	public Integer getNumOfEdgesInAllGraphs() {
		return numOfEdgesInAllGraphs;
	}

	public void setNumOfEdgesInAllGraphs(Integer numOfEdgesInAllGraphs) {
		this.numOfEdgesInAllGraphs = numOfEdgesInAllGraphs;
	}

	public int getNumOfVerticesInAllGraphs() {
		return numOfVerticesInAllGraphs;
	}

	public void setNumOfVerticesInAllGraphs(int numOfVerticesInAllGraphs) {
		this.numOfVerticesInAllGraphs = numOfVerticesInAllGraphs;
	}

	public double getSuperVertexDegree() {
		return superVertexDegree;
	}

	public void setSuperVertexDegree(double superVertexDegree) {
		this.superVertexDegree = superVertexDegree;
	}

	public Map<String, Double> getVertexTypesToAvgInDegreeMap() {
		return vertexTypesToAvgInDegreeMap;
	}

	public void setVertexTypesToAvgInDegreeMap(Map<String, Double> vertexTypesToAvgInDegreeMap) {
		this.vertexTypesToAvgInDegreeMap = vertexTypesToAvgInDegreeMap;
	}

	public boolean isDissolveSuperVerticesBasedOnCount() {
		return dissolveSuperVerticesBasedOnCount;
	}

	public void setDissolveSuperVerticesBasedOnCount(boolean dissolveSuperVerticesBasedOnCount) {
		this.dissolveSuperVerticesBasedOnCount = dissolveSuperVerticesBasedOnCount;
	}

	public Model getFirstSnapshotTypeModel() {
		return firstSnapshotTypeModel;
	}

	public void setFirstSnapshotTypeModel(Model firstSnapshotTypeModel) {
		this.firstSnapshotTypeModel = firstSnapshotTypeModel;
	}

	public Model getFirstSnapshotDataModel() {
		return firstSnapshotDataModel;
	}

	public void setFirstSnapshotDataModel(Model firstSnapshotDataModel) {
		this.firstSnapshotDataModel = firstSnapshotDataModel;
	}

	public int getMaxNumOfLiterals() {
		return maxNumOfLiterals;
	}

	public void setMaxNumOfLiterals(int maxNumOfLiterals) {
		this.maxNumOfLiterals = maxNumOfLiterals;
	}

	public List<Long> getTotalVisitedPathCheckingTime() {
		return totalVisitedPathCheckingTime;
	}

	public Long getTotalVisitedPathCheckingTime(int index) {
		return returnLongAtIndexIfExistsElseZero(this.getTotalVisitedPathCheckingTime(), index);
	}

	public void addToTotalVisitedPathCheckingTime(long totalVisitedPathCheckingTime) {
		addToValueInListAtIndex(this.getTotalVisitedPathCheckingTime(), this.getCurrentVSpawnLevel(), totalVisitedPathCheckingTime);
	}

	public static void setValueInListAtIndex(List<Double> list, int index, Double value) {
		while (list.size() <= index) {
			list.add(0.0);
		}
		list.set(index, value);
	}

	public List<Long> getTotalSupersetPathCheckingTime() {
		return totalSupersetPathCheckingTime;
	}

	public Long getTotalSupersetPathCheckingTime(int index) {
		return returnLongAtIndexIfExistsElseZero(this.getTotalSupersetPathCheckingTime(), index);
	}

	public void addToTotalSupersetPathCheckingTime(long totalSupersetPathCheckingTime) {
		addToValueInListAtIndex(this.getTotalSupersetPathCheckingTime(), this.getCurrentVSpawnLevel(), totalSupersetPathCheckingTime);
	}

	public List<Long> getTotalFindEntitiesTime() {
		return totalFindEntitiesTime;
	}

	public Long getTotalFindEntitiesTime(int index) {
		return returnLongAtIndexIfExistsElseZero(this.getTotalFindEntitiesTime(), index);
	}

	public void addToTotalFindEntitiesTime(long totalFindEntitiesTime) {
		addToValueInListAtIndex(this.getTotalFindEntitiesTime(), this.getCurrentVSpawnLevel(), totalFindEntitiesTime);
	}

	public List<Long> getTotalDiscoverConstantTGFDsTime() {
		return totalDiscoverConstantTGFDsTime;
	}

	public Long getTotalDiscoverConstantTGFDsTime(int index) {
		return returnLongAtIndexIfExistsElseZero(this.getTotalDiscoverConstantTGFDsTime(), index);
	}

	public void addToTotalDiscoverConstantTGFDsTime(long totalDiscoverConstantTGFDsTime) {
		addToValueInListAtIndex(this.getTotalDiscoverConstantTGFDsTime(), this.getCurrentVSpawnLevel(), totalDiscoverConstantTGFDsTime);
	}

	public List<Long> getTotalDiscoverGeneralTGFDTime() {
		return totalDiscoverGeneralTGFDTime;
	}

	public Long getTotalDiscoverGeneralTGFDTime(int index) {
		return returnLongAtIndexIfExistsElseZero(this.getTotalDiscoverGeneralTGFDTime(), index);
	}

	public void addToTotalDiscoverGeneralTGFDTime(long totalDiscoverGeneralTGFDTime) {
		addToValueInListAtIndex(this.getTotalDiscoverGeneralTGFDTime(), this.getCurrentVSpawnLevel(), totalDiscoverGeneralTGFDTime);
	}

	public static Long returnLongAtIndexIfExistsElseZero(List<Long> list, int index) {
		return index < list.size() ? list.get(index) : 0;
	}

	public double getPatternTheta() {
		return patternTheta;
	}

	public void setPatternTheta(double patternTheta) {
		this.patternTheta = patternTheta;
	}

	public long getTotalHistogramTime() {
		return totalHistogramTime;
	}

	public void setTotalHistogramTime(long totalHistogramTime) {
		this.totalHistogramTime = totalHistogramTime;
	}

	public boolean isFastMatching() {
		return fastMatching;
	}

	public void setFastMatching(boolean fastMatching) {
		this.fastMatching = fastMatching;
	}

	public List<Double> getMedianPatternSupportsList() {
		return medianPatternSupportsList;
	}

	public Double getMedianPatternSupportsList(int index) {
		return returnDoubleAtIndexIfExistsElseZero(this.getMedianPatternSupportsList(), index);
	}

	public void addToMedianPatternSupportsList(double medianPatternSupport) {
		setValueInListAtIndex(this.getMedianPatternSupportsList(), this.getCurrentVSpawnLevel(), medianPatternSupport);
	}

	public List<Double> getMedianConstantTgfdSupportsList() {
		return medianConstantTgfdSupportsList;
	}

	public Double getMedianConstantTgfdSupportsList(int index) {
		return returnDoubleAtIndexIfExistsElseZero(this.getMedianConstantTgfdSupportsList(), index);
	}

	public void addToMedianConstantTgfdSupportsList(double medianConstantTgfdSupport) {
		setValueInListAtIndex(this.getMedianConstantTgfdSupportsList(), this.getCurrentVSpawnLevel(), medianConstantTgfdSupport);
	}

	public List<Double> getMedianGeneralTgfdSupportsList() {
		return medianGeneralTgfdSupportsList;
	}

	public Double getMedianGeneralTgfdSupportsList(int index) {
		return returnDoubleAtIndexIfExistsElseZero(this.getMedianGeneralTgfdSupportsList(), index);
	}

	public void addToMedianGeneralTgfdSupportsList(double medianGeneralTgfdSupport) {
		setValueInListAtIndex(this.getMedianGeneralTgfdSupportsList(), this.getCurrentVSpawnLevel(), medianGeneralTgfdSupport);
	}

	public static Double returnDoubleAtIndexIfExistsElseZero(List<Double> list, int index) {
		return index < list.size() ? list.get(index) : 0;
	}

	public Set<String> getInterestSet() {
		return interestSet;
	}

	public static class Pair implements Comparable<Pair> {
		private final Integer min;
		private final Integer max;

		public Pair(int min, int max) {
			this.min = min;
			this.max = max;
		}

		public Integer min() {
			return min;
		}

		public Integer max() {
			return max;
		}

		@Override
		public int compareTo(@NotNull TgfdDiscovery.Pair o) {
			if (this.min.equals(o.min)) {
				return this.max.compareTo(o.max);
			} else {
				return this.min.compareTo(o.min);
			}
		}

		@Override
		public String toString() {
			return "(" + min +
					", " + max +
					')';
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) return true;
			if (o == null || getClass() != o.getClass()) return false;
			Pair pair = (Pair) o;
			return min.equals(pair.min) && max.equals(pair.max);
		}

		@Override
		public int hashCode() {
			return Objects.hash(min, max);
		}
	}

	public void vSpawnInit(List<GraphLoader> graphs) {
		this.patternTree = new PatternTree();
		this.patternTree.addLevel();

		System.out.println("VSpawn Level 0");
		for (int i = 0; i < this.getSortedVertexHistogram().size(); i++) {
			long vSpawnTime = System.currentTimeMillis();
			System.out.println("VSpawnInit with single-node pattern " + (i+1) + "/" + this.getSortedVertexHistogram().size());
			String patternVertexType = this.getSortedVertexHistogram().get(i).getKey();

			if (this.vertexTypesToAttributesMap.get(patternVertexType).size() == 0)
				continue; // TO-DO: Should these frequent types without active attribute be filtered out much earlier?

			int numOfInstancesOfVertexType = this.getSortedVertexHistogram().get(i).getValue();
			int numOfInstancesOfAllVertexTypes = this.getNumOfVerticesInAllGraphs();

			double frequency = (double) numOfInstancesOfVertexType / (double) numOfInstancesOfAllVertexTypes;
			System.out.println("Frequency of vertex type: " + numOfInstancesOfVertexType + " / " + numOfInstancesOfAllVertexTypes + " = " + frequency);

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

			if (!this.isGeneratek0Tgfds()) {
				final long extractListOfCenterVerticesTime = System.currentTimeMillis();
				ArrayList<ArrayList<DataVertex>> matchesOfThisCenterVertexPerTimestamp;
				if (this.reUseMatches()) {
					// TO-DO: Implement pattern support calculation here using entityURIs?
					Map<String, List<Integer>> entityURIs = new HashMap<>();
					matchesOfThisCenterVertexPerTimestamp = extractListOfCenterVertices(graphs, patternVertexType, entityURIs);
					patternTreeNode.setListOfCenterVertices(matchesOfThisCenterVertexPerTimestamp);
					for (Map.Entry<String, List<Integer>> entry: entityURIs.entrySet()) {
						System.out.println(entry);
					}
					this.setPatternSupport(entityURIs, patternTreeNode);
					printWithTime("extractListOfCenterVerticesTime", (System.currentTimeMillis() - extractListOfCenterVerticesTime));
					int numOfMatches = 0;
					for (ArrayList<DataVertex> matchesOfThisCenterVertex : matchesOfThisCenterVertexPerTimestamp) {
						numOfMatches += matchesOfThisCenterVertex.size();
					}
					System.out.println("Number of center vertex matches found containing active attributes: " + numOfMatches);
					if (doesNotSatisfyTheta(patternTreeNode)) {
						patternTreeNode.setIsPruned();
					}
				}
			} else {
				final long matchingTime = System.currentTimeMillis();
				List<Set<Set<ConstantLiteral>>> matchesPerTimestamps = new ArrayList<>();
				if (this.isValidationSearch()) {
					matchesPerTimestamps = this.getMatchesForPattern(this.getGraphs(), patternTreeNode);
				} else {
					ArrayList<ArrayList<DataVertex>> matchesOfThisCenterVertexPerTimestamp = new ArrayList<>();
					HashMap<String, List<Integer>> entityURIs = new HashMap<>();
					for (int timestamp = 0; timestamp < graphs.size(); timestamp++) {
						int numOfMatches = 0;
						GraphLoader graph = graphs.get(timestamp);
						Set<Set<ConstantLiteral>> matchesInThisTimestamp = new HashSet<>(graphs.size());
						ArrayList<DataVertex> matchesOfThisCenterVertexInThisTimestamp = new ArrayList<>();
						for (Vertex v : graph.getGraph().getGraph().vertexSet()) {
							if (v.getTypes().contains(patternVertexType)) {
								numOfMatches++;
								DataVertex dataVertex = (DataVertex) v;
								HashSet<ConstantLiteral> match = new HashSet<>();
								Map<String, Integer> interestingnessMap = new HashMap<>();
								String entityURI = extractAttributes(patternTreeNode, patternVertexType, match, dataVertex, interestingnessMap);
								if (this.isOnlyInterestingTGFDs() && interestingnessMap.values().stream().anyMatch(n -> n < 2)) {
									continue;
								} else if (!this.isOnlyInterestingTGFDs() && match.size() < patternTreeNode.getGraph().vertexSet().size()) {
									continue;
								}
								if (entityURI != null) {
									entityURIs.putIfAbsent(entityURI, createEmptyArrayListOfSize(this.getNumOfSnapshots()));
									entityURIs.get(entityURI).set(timestamp, entityURIs.get(entityURI).get(timestamp) + 1);
								}
								matchesInThisTimestamp.add(match);
								matchesOfThisCenterVertexInThisTimestamp.add(dataVertex);
							}
						}
						System.out.println("Number of center vertex matches found: " + numOfMatches);
						System.out.println("Number of center vertex matches found containing active attributes: " + matchesInThisTimestamp.size());
						matchesPerTimestamps.add(matchesInThisTimestamp);
						matchesOfThisCenterVertexPerTimestamp.add(matchesOfThisCenterVertexInThisTimestamp);
					}
					patternTreeNode.setListOfCenterVertices(matchesOfThisCenterVertexPerTimestamp);
					this.setPatternSupport(entityURIs, patternTreeNode);
				}
				if (doesNotSatisfyTheta(patternTreeNode)) {
					patternTreeNode.setIsPruned();
				} else {
					final long hSpawnStartTime = System.currentTimeMillis();
					ArrayList<TGFD> tgfds = this.hSpawn(patternTreeNode, matchesPerTimestamps);
					printWithTime("hSpawn", (System.currentTimeMillis() - hSpawnStartTime));
					this.getTgfds().get(0).addAll(tgfds);
				}
				final long finalMatchingTime = System.currentTimeMillis() - matchingTime;
				printWithTime("matchingTime", finalMatchingTime);
				this.addToTotalMatchingTime(finalMatchingTime);
			}
		}
		System.out.println("GenTree Level " + this.getCurrentVSpawnLevel() + " size: " + this.patternTree.getLevel(this.getCurrentVSpawnLevel()).size());
		for (PatternTreeNode node : this.patternTree.getLevel(this.getCurrentVSpawnLevel())) {
			System.out.println("Pattern: " + node.getPattern());
//			System.out.println("Pattern Support: " + node.getPatternSupport());
//			System.out.println("Dependency: " + node.getDependenciesSets());
		}

	}

	public void setPatternSupport(Map<String, List<Integer>> entityURIsMap, PatternTreeNode patternTreeNode) {
		System.out.println("Calculating pattern support...");
		String centerVertexType = patternTreeNode.getPattern().getCenterVertexType();
		System.out.println("Center vertex type: " + centerVertexType);
		int numOfPossiblePairs = 0;
		for (Entry<String, List<Integer>> entityUriEntry : entityURIsMap.entrySet()) {
			int numberOfAcrossMatchesOfEntity = (int) entityUriEntry.getValue().stream().filter(x -> x > 0).count();
			int k = 2;
			if (numberOfAcrossMatchesOfEntity >= k) {
				numOfPossiblePairs += CombinatoricsUtils.binomialCoefficient(numberOfAcrossMatchesOfEntity, k);
			}
			int numberOfWithinMatchesOfEntity = (int) entityUriEntry.getValue().stream().filter(x -> x > 1).count();
			numOfPossiblePairs += numberOfWithinMatchesOfEntity;
		}
		int S = this.vertexHistogram.get(centerVertexType);
		double patternSupport = calculateSupport(numOfPossiblePairs, S);
		patternTreeNode.setPatternSupport(patternSupport);
		this.patternSupportsListForThisSnapshot.add(patternSupport);
	}

	private double calculateSupport(double numerator, double S) {
		System.out.println("S = "+S);
		double denominator = S * CombinatoricsUtils.binomialCoefficient(this.getNumOfSnapshots()+1,2);
		assert numerator <= denominator;
		double support = numerator / denominator;
		System.out.println("Support: " + numerator + " / " + denominator + " = " + support);
		return support;
	}

	private boolean doesNotSatisfyTheta(PatternTreeNode patternTreeNode) {
		assert patternTreeNode.getPatternSupport() != null;
		return patternTreeNode.getPatternSupport() < this.getPatternTheta();
	}

	public static boolean isDuplicateEdge(VF2PatternGraph pattern, String edgeType, String sourceType, String targetType) {
		for (RelationshipEdge edge : pattern.getPattern().edgeSet()) {
			if (edge.getLabel().equalsIgnoreCase(edgeType)) {
				if (edge.getSource().getTypes().contains(sourceType) && edge.getTarget().getTypes().contains(targetType)) {
					return true;
				}
			}
		}
		return false;
	}

	public static boolean isMultipleEdge(VF2PatternGraph pattern, String sourceType, String targetType) {
		for (RelationshipEdge edge : pattern.getPattern().edgeSet()) {
			if (edge.getSource().getTypes().contains(sourceType) && edge.getTarget().getTypes().contains(targetType)) {
				return true;
			}
		}
		return false;
	}

	public PatternTreeNode vSpawn() {

		if (this.getCandidateEdgeIndex() > this.sortedFrequentEdgesHistogram.size()-1) {
			this.setCandidateEdgeIndex(0);
			this.setPreviousLevelNodeIndex(this.getPreviousLevelNodeIndex() + 1);
		}

		if (this.getPreviousLevelNodeIndex() >= this.patternTree.getLevel(this.getCurrentVSpawnLevel() -1).size()) {
			this.getkRuntimes().add(System.currentTimeMillis() - this.getStartTime());
			this.printTgfdsToFile(this.getExperimentName(), this.getTgfds().get(this.getCurrentVSpawnLevel()));
			if (this.iskExperiment()) this.printExperimentRuntimestoFile();
			this.printSupportStatisticsForThisSnapshot();
			this.printTimeStatisticsForThisSnapshot(this.getCurrentVSpawnLevel());
			this.setCurrentVSpawnLevel(this.getCurrentVSpawnLevel() + 1);
			if (this.getCurrentVSpawnLevel() > this.getK()) {
				return null;
			}
			this.patternTree.addLevel();
			this.setPreviousLevelNodeIndex(0);
			this.setCandidateEdgeIndex(0);
		}

		System.out.println("Performing VSpawn");
		System.out.println("VSpawn Level " + this.getCurrentVSpawnLevel());
//		System.gc();

		ArrayList<PatternTreeNode> previousLevel = this.patternTree.getLevel(this.getCurrentVSpawnLevel() - 1);
		if (previousLevel.size() == 0) {
			this.setPreviousLevelNodeIndex(this.getPreviousLevelNodeIndex() + 1);
			return null;
		}
		PatternTreeNode previousLevelNode = previousLevel.get(this.getPreviousLevelNodeIndex());
		System.out.println("Processing previous level node " + this.getPreviousLevelNodeIndex() + "/" + previousLevel.size());
		System.out.println("Performing VSpawn on pattern: " + previousLevelNode.getPattern());

		System.out.println("Level " + (this.getCurrentVSpawnLevel() - 1) + " pattern: " + previousLevelNode.getPattern());
		if (this.hasSupportPruning() && previousLevelNode.isPruned()) {
			System.out.println("Marked as pruned. Skip.");
			this.setPreviousLevelNodeIndex(this.getPreviousLevelNodeIndex() + 1);
			return null;
		}

		System.out.println("Processing candidate edge " + this.getCandidateEdgeIndex() + "/" + this.sortedFrequentEdgesHistogram.size());
		Map.Entry<String, Integer> candidateEdge = this.sortedFrequentEdgesHistogram.get(this.getCandidateEdgeIndex());
		String candidateEdgeString = candidateEdge.getKey();
		System.out.println("Candidate edge:" + candidateEdgeString);


		String sourceVertexType = candidateEdgeString.split(" ")[0];
		String targetVertexType = candidateEdgeString.split(" ")[2];

		if (this.vertexTypesToAttributesMap.get(targetVertexType).size() == 0) {
			System.out.println("Target vertex in candidate edge does not contain active attributes");
			this.setCandidateEdgeIndex(this.getCandidateEdgeIndex() + 1);
			return null;
		}

		// TO-DO: We should add support for duplicate vertex types in the future
		if (sourceVertexType.equals(targetVertexType)) {
			System.out.println("Candidate edge contains duplicate vertex types. Skip.");
			this.setCandidateEdgeIndex(this.getCandidateEdgeIndex() + 1);
			return null;
		}
		String edgeType = candidateEdgeString.split(" ")[1];

		// Check if candidate edge already exists in pattern
		if (isDuplicateEdge(previousLevelNode.getPattern(), edgeType, sourceVertexType, targetVertexType)) {
			System.out.println("Candidate edge: " + candidateEdge.getKey());
			System.out.println("already exists in pattern");
			this.setCandidateEdgeIndex(this.getCandidateEdgeIndex() + 1);
			return null;
		}

		if (isMultipleEdge(previousLevelNode.getPattern(), sourceVertexType, targetVertexType)) {
			System.out.println("We do not support multiple edges between existing vertices.");
			this.setCandidateEdgeIndex(this.getCandidateEdgeIndex() + 1);
			return null;
		}

		// Checks if candidate edge extends pattern
		PatternVertex sourceVertex = isDuplicateVertex(previousLevelNode.getPattern(), sourceVertexType);
		PatternVertex targetVertex = isDuplicateVertex(previousLevelNode.getPattern(), targetVertexType);
		if (sourceVertex == null && targetVertex == null) {
			System.out.println("Candidate edge: " + candidateEdge.getKey());
			System.out.println("does not extend from pattern");
			this.setCandidateEdgeIndex(this.getCandidateEdgeIndex() + 1);
			return null;
		}

		PatternTreeNode patternTreeNode = null;
		// TO-DO: FIX label conflict. What if an edge has same vertex type on both sides?
		for (Vertex v : previousLevelNode.getGraph().vertexSet()) {
			System.out.println("Looking to add candidate edge to vertex: " + v.getTypes());
			PatternVertex pv = (PatternVertex) v;
			if (pv.isMarked()) {
				System.out.println("Skip vertex. Already added candidate edge to vertex: " + pv.getTypes());
				continue;
			}
			if (!pv.getTypes().contains(sourceVertexType) && !pv.getTypes().contains(targetVertexType)) {
				System.out.println("Skip vertex. Candidate edge does not connect to vertex: " + pv.getTypes());
				pv.setMarked(true);
				continue;
			}

			// Create unmarked copy of k-1 pattern
			VF2PatternGraph newPattern = previousLevelNode.getPattern().copy();
			if (targetVertex == null) {
				targetVertex = new PatternVertex(targetVertexType);
				newPattern.addVertex(targetVertex);
			} else {
				for (Vertex vertex : newPattern.getPattern().vertexSet()) {
					if (vertex.getTypes().contains(targetVertexType)) {
						targetVertex.setMarked(true);
						targetVertex = (PatternVertex) vertex;
						break;
					}
				}
			}
			RelationshipEdge newEdge = new RelationshipEdge(edgeType);
			if (sourceVertex == null) {
				sourceVertex = new PatternVertex(sourceVertexType);
				newPattern.addVertex(sourceVertex);
			} else {
				for (Vertex vertex : newPattern.getPattern().vertexSet()) {
					if (vertex.getTypes().contains(sourceVertexType)) {
						sourceVertex.setMarked(true);
						sourceVertex = (PatternVertex) vertex;
						break;
					}
				}
			}
			newPattern.addEdge(sourceVertex, targetVertex, newEdge);

			System.out.println("Created new pattern: " + newPattern);

			// TO-DO: Debug - Why does this work with strings but not subgraph isomorphism???
			if (isIsomorphicPattern(newPattern, this.patternTree)) {
				pv.setMarked(true);
				System.out.println("Skip. Candidate pattern is an isomorph of existing pattern");
				continue;
			}

			if (this.hasSupportPruning() && isSuperGraphOfPrunedPattern(newPattern, this.patternTree)) {
				pv.setMarked(true);
				System.out.println("Skip. Candidate pattern is a supergraph of pruned pattern");
				continue;
			}
			if (this.getCurrentVSpawnLevel() == 1) {
				patternTreeNode = new PatternTreeNode(newPattern, previousLevelNode, candidateEdgeString);
				for (RelationshipEdge e : newPattern.getPattern().edgeSet()) {
					Vertex source = e.getSource();
					String sourceType = source.getTypes().iterator().next();
					Vertex target = e.getTarget();
					String targetType = target.getTypes().iterator().next();
					Vertex centerVertex = this.getVertexTypesToAvgInDegreeMap().get(sourceType) > this.getVertexTypesToAvgInDegreeMap().get(targetType) ? source : target;
					newPattern.setCenterVertex(centerVertex);
					newPattern.setRadius(1);
					newPattern.setDiameter(1);
				}
				this.patternTree.getTree().get(this.getCurrentVSpawnLevel()).add(patternTreeNode);
				this.patternTree.findSubgraphParents(this.getCurrentVSpawnLevel()-1, patternTreeNode);
				this.patternTree.findCenterVertexParent(this.getCurrentVSpawnLevel()-1, patternTreeNode, true);
			} else {
				boolean considerAlternativeParents = true;
				newPattern.getCenterVertexType();
				if (this.isFastMatching() && this.getCurrentVSpawnLevel() > 2) {
					if (newPattern.getPattern().edgesOf(newPattern.getCenterVertex()).size() != this.currentVSpawnLevel
							&& newPattern.getPattern().vertexSet().size() == newPattern.getPattern().edgeSet().size()+1) {
						newPattern.setCenterVertex(newPattern.getFirstNode());
						considerAlternativeParents = false;
					}
				} else {
					int minRadius = newPattern.getPattern().vertexSet().size();
					for (Vertex newV : newPattern.getPattern().vertexSet()) {
						minRadius = Math.min(minRadius, newPattern.calculateRadiusForGivenVertex(newV));
					}
					Map<Vertex, Double> maxDegreeTypes = new HashMap<>();
					for (Vertex newV : newPattern.getPattern().vertexSet()) {
						if (minRadius == newPattern.calculateRadiusForGivenVertex(newV)) {
							String type = newV.getTypes().iterator().next();
							maxDegreeTypes.put(newV, this.getVertexTypesToAvgInDegreeMap().get(type));
						}
					}
					assert maxDegreeTypes.size() > 0;
					List<Entry<Vertex, Double>> entries = new ArrayList<>(maxDegreeTypes.entrySet());
					entries.sort(new Comparator<Entry<Vertex, Double>>() {
						@Override
						public int compare(Entry<Vertex, Double> o1, Entry<Vertex, Double> o2) {
							return o2.getValue().compareTo(o1.getValue());
						}
					});
					Vertex centerVertex = entries.get(0).getKey();
					newPattern.setCenterVertex(centerVertex);
				}
				patternTreeNode = this.patternTree.createNodeAtLevel(this.getCurrentVSpawnLevel(), newPattern, previousLevelNode, candidateEdgeString, considerAlternativeParents);
			}
			System.out.println("Marking vertex " + pv.getTypes() + "as expanded.");
			break;
		}
		if (patternTreeNode == null) {
			for (Vertex v : previousLevelNode.getGraph().vertexSet()) {
				System.out.println("Unmarking all vertices in current pattern for the next candidate edge");
				((PatternVertex)v).setMarked(false);
			}
			this.setCandidateEdgeIndex(this.getCandidateEdgeIndex() + 1);
		}
		return patternTreeNode;
	}

	private boolean isIsomorphicPattern(VF2PatternGraph newPattern, PatternTree patternTree) {
		final long isIsomorphicPatternCheckStartTime = System.currentTimeMillis();
	    System.out.println("Checking if the pattern is isomorphic...");
	    ArrayList<String> newPatternEdges = new ArrayList<>();
        newPattern.getPattern().edgeSet().forEach((edge) -> {newPatternEdges.add(edge.toString());});
        boolean isIsomorphic = false;
		for (PatternTreeNode otherPattern: patternTree.getLevel(this.getCurrentVSpawnLevel())) {
            ArrayList<String> otherPatternEdges = new ArrayList<>();
            otherPattern.getGraph().edgeSet().forEach((edge) -> {otherPatternEdges.add(edge.toString());});
            if (newPatternEdges.containsAll(otherPatternEdges)) {
				System.out.println("Candidate pattern: " + newPattern);
				System.out.println("is an isomorph of current VSpawn level pattern: " + otherPattern.getPattern());
				isIsomorphic = true;
				break;
			}
		}
		final long isomorphicCheckingTime = System.currentTimeMillis() - isIsomorphicPatternCheckStartTime;
		printWithTime("isIsomorphicPatternCheck", isomorphicCheckingTime);
		addToTotalSupergraphCheckingTime(isomorphicCheckingTime);
		return isIsomorphic;
	}

	// TO-DO: Should this be done using real subgraph isomorphism instead of strings?
	private boolean isSuperGraphOfPrunedPattern(VF2PatternGraph newPattern, PatternTree patternTree) {
		final long supergraphCheckingStartTime = System.currentTimeMillis();
        ArrayList<String> newPatternEdges = new ArrayList<>();
        newPattern.getPattern().edgeSet().forEach((edge) -> {newPatternEdges.add(edge.toString());});
		int i = this.getCurrentVSpawnLevel();
		boolean isSupergraph = false;
		while (i >= 0) {
			for (PatternTreeNode treeNode : patternTree.getLevel(i)) {
				if (treeNode.isPruned()) {
					if (treeNode.getPattern().getCenterVertexType().equals(newPattern.getCenterVertexType())) {
						if (i == 0) {
							isSupergraph = true;
						} else {
							ArrayList<String> otherPatternEdges = new ArrayList<>();
							treeNode.getGraph().edgeSet().forEach((edge) -> {
								otherPatternEdges.add(edge.toString());
							});
							if (newPatternEdges.containsAll(otherPatternEdges)) {
								isSupergraph = true;
							}
						}
						if (isSupergraph) {
							System.out.println("Candidate pattern: " + newPattern);
							System.out.println("is a supergraph of pruned subgraph pattern: " + treeNode.getPattern());
							break;
						}
					}
				}
			}
			if (isSupergraph) break;
			i--;
		}
		final long supergraphCheckingTime = System.currentTimeMillis() - supergraphCheckingStartTime;
		printWithTime("Supergraph checking", supergraphCheckingTime);
		addToTotalSupergraphCheckingTime(supergraphCheckingTime);
		return isSupergraph;
	}

	private PatternVertex isDuplicateVertex(VF2PatternGraph newPattern, String vertexType) {
		for (Vertex v: newPattern.getPattern().vertexSet()) {
			if (v.getTypes().contains(vertexType)) {
				return (PatternVertex) v;
			}
		}
		return null;
	}

	public ArrayList<ArrayList<DataVertex>> extractListOfCenterVertices(List<GraphLoader> graphs, String patternVertexType, Map<String, List<Integer>> entityURIs) {
		System.out.println("Extracting list of center vertices for vertex type "+patternVertexType);
		ArrayList<ArrayList<DataVertex>> matchesOfThisCenterVertex = new ArrayList<>(graphs.size());
		for (int timestamp = 0; timestamp < graphs.size(); timestamp++) {
			GraphLoader graph = graphs.get(timestamp);
			ArrayList<DataVertex> matchesInThisTimestamp = new ArrayList<>();
			for (Vertex vertex : graph.getGraph().getGraph().vertexSet()) {
				if (vertex.getTypes().contains(patternVertexType)) {
					DataVertex dataVertex = (DataVertex) vertex;
					if (this.isOnlyInterestingTGFDs()) {
						// Check if vertex contains at least one active attribute
						boolean containsActiveAttributes = false;
						for (String attrName : vertex.getAllAttributesNames()) {
							if (this.activeAttributesSet.contains(attrName)) {
								containsActiveAttributes = true;
								break;
							}
						}
						if (!containsActiveAttributes) {
							continue;
						}
						matchesInThisTimestamp.add(dataVertex);
					} else {
						matchesInThisTimestamp.add(dataVertex);
					}
					if (entityURIs != null) {
						String entityURI = dataVertex.getVertexURI();
						entityURIs.putIfAbsent(entityURI, createEmptyArrayListOfSize(this.getNumOfSnapshots()));
						entityURIs.get(entityURI).set(timestamp, entityURIs.get(entityURI).get(timestamp) + 1);
					}
				}
			}
			System.out.println("Number of matches found: " + matchesInThisTimestamp.size());
			matchesOfThisCenterVertex.add(matchesInThisTimestamp);
		}
		return matchesOfThisCenterVertex;
	}

	public int extractMatches(Set<RelationshipEdge> edgeSet, ArrayList<HashSet<ConstantLiteral>> matches, PatternTreeNode patternTreeNode, Map<String, List<Integer>> entityURIs, int timestamp) {
		String patternEdgeLabel = patternTreeNode.getGraph().edgeSet().iterator().next().getLabel();
		String sourceVertexType = patternTreeNode.getGraph().edgeSet().iterator().next().getSource().getTypes().iterator().next();
		String targetVertexType = patternTreeNode.getGraph().edgeSet().iterator().next().getTarget().getTypes().iterator().next();
		int numOfMatches = 0;
		for (RelationshipEdge edge: edgeSet) {
			String matchedEdgeLabel = edge.getLabel();
			Set<String> matchedSourceVertexType = edge.getSource().getTypes();
			Set<String> matchedTargetVertexType = edge.getTarget().getTypes();
			if (matchedEdgeLabel.equals(patternEdgeLabel) && matchedSourceVertexType.contains(sourceVertexType) && matchedTargetVertexType.contains(targetVertexType)) {
				numOfMatches++;
				HashSet<ConstantLiteral> literalsInMatch = new HashSet<>();
				Map<String, Integer> interestingnessMap = new HashMap<>();
				String entityURI = extractMatch(edge.getSource(), sourceVertexType, edge.getTarget(), targetVertexType, patternTreeNode, literalsInMatch, interestingnessMap);
				if (this.isOnlyInterestingTGFDs() && interestingnessMap.values().stream().anyMatch(n -> n < 2)) {
					continue;
				} else if (!this.isOnlyInterestingTGFDs() && literalsInMatch.size() < patternTreeNode.getGraph().vertexSet().size()) {
					continue;
				}
				if (entityURI != null) {
					entityURIs.putIfAbsent(entityURI, createEmptyArrayListOfSize(this.getNumOfSnapshots()));
					entityURIs.get(entityURI).set(timestamp, entityURIs.get(entityURI).get(timestamp)+1);
				}
				matches.add(literalsInMatch);
			}
		}
		matches.sort(new Comparator<HashSet<ConstantLiteral>>() {
			@Override
			public int compare(HashSet<ConstantLiteral> o1, HashSet<ConstantLiteral> o2) {
				return o1.size() - o2.size();
			}
		});
		return numOfMatches;
	}

	private static List<Integer> createEmptyArrayListOfSize(int size) {
		List<Integer> emptyArray = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			emptyArray.add(0);
		}
		return emptyArray;
	}

	public List<Set<Set<ConstantLiteral>>> getMatchesForPattern(List<GraphLoader> graphs, PatternTreeNode patternTreeNode) {
		// TO-DO: Potential speed up for single-edge/single-node patterns. Iterate through all edges/nodes in graph.
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
//			if (this.getCurrentVSpawnLevel() == 1) {
//				numOfMatchesInTimestamp = extractMatches(graphs.get(year).getGraph().getGraph().edgeSet(), matches, patternTreeNode, entityURIs);
//			} else {
				VF2AbstractIsomorphismInspector<Vertex, RelationshipEdge> results = new VF2SubgraphIsomorphism().execute2(graphs.get(year).getGraph(), patternTreeNode.getPattern(), false);
				if (results.isomorphismExists()) {
					numOfMatchesInTimestamp = extractMatches(results.getMappings(), matches, patternTreeNode, entityURIs, year);
				}
//			}
			System.out.println("Number of matches found: " + numOfMatchesInTimestamp);
			System.out.println("Number of matches found that contain active attributes: " + matches.size());
			matchesPerTimestamps.get(year).addAll(matches);
			printWithTime("Search Cost", (System.currentTimeMillis() - searchStartTime));
		}

		// TO-DO: Should we implement pattern support here to weed out patterns with few matches in later iterations?
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
		this.setPatternSupport(entityURIs, patternTreeNode);

		return matchesPerTimestamps;
	}

	// TO-DO: Merge with other extractMatch method?
	private String extractMatch(GraphMapping<Vertex, RelationshipEdge> result, PatternTreeNode patternTreeNode, HashSet<ConstantLiteral> match, Map<String, Integer> interestingnessMap) {
		String entityURI = null;
		for (Vertex v : patternTreeNode.getGraph().vertexSet()) {
			Vertex currentMatchedVertex = result.getVertexCorrespondence(v, false);
			if (currentMatchedVertex == null) continue;
			String patternVertexType = v.getTypes().iterator().next();
			if (entityURI == null) {
				entityURI = extractAttributes(patternTreeNode, patternVertexType, match, currentMatchedVertex, interestingnessMap);
			} else {
				extractAttributes(patternTreeNode, patternVertexType, match, currentMatchedVertex, interestingnessMap);
			}
		}
		return entityURI;
	}

	// TO-DO: Merge with other extractMatch method?
	private String extractMatch(Vertex currentSourceVertex, String sourceVertexType, Vertex currentTargetVertex, String targetVertexType, PatternTreeNode patternTreeNode, HashSet<ConstantLiteral> match, Map<String, Integer> interestingnessMap) {
		String entityURI = null;
		List<String> patternVerticesTypes = Arrays.asList(sourceVertexType, targetVertexType);
		List<Vertex> vertices = Arrays.asList(currentSourceVertex, currentTargetVertex);
		for (int index = 0; index < vertices.size(); index++) {
			Vertex currentMatchedVertex = vertices.get(index);
			String patternVertexType = patternVerticesTypes.get(index);
			if (entityURI == null) {
				entityURI = extractAttributes(patternTreeNode, patternVertexType, match, currentMatchedVertex, interestingnessMap);
			} else {
				extractAttributes(patternTreeNode, patternVertexType, match, currentMatchedVertex, interestingnessMap);
			}
		}
		return entityURI;
	}

	private String extractAttributes(PatternTreeNode patternTreeNode, String patternVertexType, HashSet<ConstantLiteral> match, Vertex currentMatchedVertex, Map<String, Integer> interestingnessMap) {
		String entityURI = null;
		String centerVertexType = patternTreeNode.getPattern().getCenterVertexType();
		Set<String> matchedVertexTypes = currentMatchedVertex.getTypes();
		for (ConstantLiteral activeAttribute : getActiveAttributesInPattern(patternTreeNode.getGraph().vertexSet(),true)) {
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

	private int extractMatches(Iterator<GraphMapping<Vertex, RelationshipEdge>> iterator, ArrayList<HashSet<ConstantLiteral>> matches, PatternTreeNode patternTreeNode, Map<String, List<Integer>> entityURIs, int timestamp) {
		int numOfMatches = 0;
		while (iterator.hasNext()) {
			numOfMatches++;
			GraphMapping<Vertex, RelationshipEdge> result = iterator.next();
			HashSet<ConstantLiteral> literalsInMatch = new HashSet<>();
			Map<String, Integer> interestingnessMap = new HashMap<>();
			String entityURI = extractMatch(result, patternTreeNode, literalsInMatch, interestingnessMap);
			// ensures that the match is not empty and contains more than just the uri attribute
			if (this.isOnlyInterestingTGFDs() && interestingnessMap.values().stream().anyMatch(n -> n < 2)) {
				continue;
			} else if (!this.isOnlyInterestingTGFDs() && literalsInMatch.size() < patternTreeNode.getGraph().vertexSet().size()) {
				continue;
			}
			if (entityURI != null) {
				entityURIs.putIfAbsent(entityURI, createEmptyArrayListOfSize(this.getNumOfSnapshots()));
				entityURIs.get(entityURI).set(timestamp, entityURIs.get(entityURI).get(timestamp)+1);
			}
			matches.add(literalsInMatch);
		}
		matches.sort(new Comparator<HashSet<ConstantLiteral>>() {
			@Override
			public int compare(HashSet<ConstantLiteral> o1, HashSet<ConstantLiteral> o2) {
				return o1.size() - o2.size();
			}
		});
		return numOfMatches;
	}

	public List<Set<Set<ConstantLiteral>>> getMatchesUsingChangeFiles(PatternTreeNode patternTreeNode) {
		// TO-DO: Should we use changefiles based on freq types??

		List<Set<Set<ConstantLiteral>>> matchesPerTimestamps = new ArrayList<>();
		for (int timestamp = 0; timestamp < this.getNumOfSnapshots(); timestamp++) {
			matchesPerTimestamps.add(new HashSet<>());
		}

//		patternTreeNode.getPattern().setDiameter(this.getCurrentVSpawnLevel());

		TGFD dummyTgfd = new TGFD();
		dummyTgfd.setName(patternTreeNode.getAllEdgeStrings().toString());
		dummyTgfd.setPattern(patternTreeNode.getPattern());

		System.out.println("-----------Snapshot (1)-----------");
		long startTime=System.currentTimeMillis();
		List<TGFD> tgfds = Collections.singletonList(dummyTgfd);
		int numberOfMatchesFound = 0;

		GraphLoader graph;
		if (this.getFirstSnapshotTypeModel() == null && this.getFirstSnapshotDataModel() == null) {
			for (String path : this.getTimestampToFilesMap().get(0).getValue()) {
				if (!path.toLowerCase().endsWith(".ttl") && !path.toLowerCase().endsWith(".nt"))
					continue;
				if (path.toLowerCase().contains("literals") || path.toLowerCase().contains("objects"))
					continue;
				Path input= Paths.get(path);
				Model model = ModelFactory.createDefaultModel();
				System.out.println("Reading Node Types: " + path);
				model.read(input.toUri().toString());
				this.setFirstSnapshotTypeModel(model);
			}
			Model dataModel = ModelFactory.createDefaultModel();
			for (String path: this.getTimestampToFilesMap().get(0).getValue()) {
				if (!path.toLowerCase().endsWith(".ttl") && !path.toLowerCase().endsWith(".nt"))
					continue;
				if (path.toLowerCase().contains("types"))
					continue;
				Path input= Paths.get(path);
				System.out.println("Reading data graph: "+path);
				dataModel.read(input.toUri().toString());
				this.setFirstSnapshotDataModel(dataModel);
			}
		}
//		Config.optimizedLoadingBasedOnTGFD = true; // TO-DO: Does enabling optimized loading cause problems with TypeChange?
		assert this.getFirstSnapshotTypeModel() != null;
		assert this.getFirstSnapshotDataModel() != null;
		if (this.getLoader().equals("dbpedia")) {
			graph = new DBPediaLoader(tgfds, Collections.singletonList(this.getFirstSnapshotTypeModel()), Collections.singletonList(this.getFirstSnapshotDataModel()));
		} else {
			graph = new IMDBLoader(tgfds, Collections.singletonList(this.getFirstSnapshotDataModel()));
		}
//		Config.optimizedLoadingBasedOnTGFD = false;

		if (this.isDissolveSuperVerticesBasedOnCount())
			this.dissolveSuperVerticesBasedOnCount(graph);

		printWithTime("Load graph (1)", System.currentTimeMillis()-startTime);

		// Now, we need to find the matches for each snapshot.
		// Finding the matches...
		Map<String, List<Integer>> entityURIs = new HashMap<>();

		for (TGFD tgfd : tgfds) {
			System.out.println("\n###########" + tgfd.getName() + "###########");

			//Retrieving and storing the matches of each timestamp.
			final long searchStartTime = System.currentTimeMillis();
			this.extractMatchesAcrossSnapshots(Collections.singletonList(graph), patternTreeNode, matchesPerTimestamps, entityURIs);
			printWithTime("Search Cost", (System.currentTimeMillis() - searchStartTime));
			numberOfMatchesFound += matchesPerTimestamps.get(0).size();
		}

		//Load the change files
		for (int i = 0; i < this.getNumOfSnapshots()-1; i++) {
			System.out.println("-----------Snapshot (" + (i+2) + ")-----------");

			startTime = System.currentTimeMillis();
			JSONArray changesJsonArray;
			if (this.isUseTypeChangeFile()) {
				System.out.println("Using type changefiles...");
				changesJsonArray = new JSONArray();
				for (RelationshipEdge e: patternTreeNode.getGraph().edgeSet()) {
					for (String type: e.getSource().getTypes()) {
						String changeFilePath = "changes_t" + (i + 1) + "_t" + (i + 2) + "_" + type + ".json";
						System.out.println(changeFilePath);
						JSONArray changesJsonArrayForType;
						if (this.isStoreInMemory()) {
							System.out.println("Getting changefile from memory");
							changesJsonArrayForType = this.getChangeFilesMap().get(changeFilePath);
						} else {
							System.out.println("Reading changefile from disk");
							changesJsonArrayForType = readJsonArrayFromFile(changeFilePath);
						}
						changesJsonArray.addAll(changesJsonArrayForType);
					}
				}
			} else {
				String changeFilePath = "changes_t" + (i + 1) + "_t" + (i + 2) + "_" + this.getGraphSize() + ".json";
				if (this.isStoreInMemory()) {
					changesJsonArray = this.getChangeFilesMap().get(changeFilePath);
				} else {
					changesJsonArray = readJsonArrayFromFile(changeFilePath);
				}
			}
			ChangeLoader changeLoader = new ChangeLoader(changesJsonArray);
			HashMap<Integer,HashSet<Change>> newChanges = changeLoader.getAllGroupedChanges();
			List<Entry<Integer,HashSet<Change>>> sortedChanges = new ArrayList<>(newChanges.entrySet());
			HashMap<ChangeType, Integer> map = new HashMap<>();
			map.put(ChangeType.deleteAttr, 1);
			map.put(ChangeType.insertAttr, 3);
			map.put(ChangeType.changeAttr, 3);
			map.put(ChangeType.deleteEdge, 0);
			map.put(ChangeType.insertEdge, 4);
			map.put(ChangeType.changeType, 2);
			map.put(ChangeType.deleteVertex, 0);
			map.put(ChangeType.insertVertex, 6);
			sortedChanges.sort(new Comparator<Entry<Integer, HashSet<Change>>>() {
				@Override
				public int compare(Entry<Integer, HashSet<Change>> o1, Entry<Integer, HashSet<Change>> o2) {
					if ((o1.getValue().iterator().next() instanceof AttributeChange || o1.getValue().iterator().next() instanceof TypeChange)
							&& (o2.getValue().iterator().next() instanceof AttributeChange || o2.getValue().iterator().next() instanceof TypeChange)) {
						boolean o1containsTypeChange = false;
						for (Change c: o1.getValue()) {
							if (c instanceof TypeChange) {
								o1containsTypeChange = true;
								break;
							}
						}
						boolean o2containsTypeChange = false;
						for (Change c: o2.getValue()) {
							if (c instanceof TypeChange) {
								o2containsTypeChange = true;
								break;
							}
						}
						if (o1containsTypeChange && !o2containsTypeChange) {
							return -1;
						} else if (!o1containsTypeChange && o2containsTypeChange) {
							return 1;
						} else {
							return 0;
						}
					}
					return map.get(o1.getValue().iterator().next().getTypeOfChange()).compareTo(map.get(o2.getValue().iterator().next().getTypeOfChange()));
				}
			});
			printWithTime("Load changes for Snapshot (" + (i+2) + ")", System.currentTimeMillis()-startTime);
			System.out.println("Total number of changes in changefile: " + newChanges.size());
			List<List<Entry<Integer,HashSet<Change>>>> changes = new ArrayList<>();
			changes.add(sortedChanges);

			System.out.println("Total number of changes: " + changes.size());

			// Now, we need to find the matches for each snapshot.
			// Finding the matches...

			startTime=System.currentTimeMillis();
			System.out.println("Updating the graph...");
			// TO-DO: Do we need to update the subgraphWithinDiameter method used in IncUpdates?
			IncUpdates incUpdatesOnDBpedia = new IncUpdates(graph.getGraph(), tgfds);
			incUpdatesOnDBpedia.AddNewVertices(changeLoader.getAllChanges());
			System.out.println("Added new vertices.");

			HashMap<String, TGFD> tgfdsByName = new HashMap<>();
			for (TGFD tgfd : tgfds) {
				tgfdsByName.put(tgfd.getName(), tgfd);
			}
			Map<String, Set<ConstantLiteral>> newMatches = new HashMap<>();
			Map<String, Set<ConstantLiteral>> removedMatches = new HashMap<>();
			int numOfNewMatchesFoundInSnapshot = 0;
			for (List<Entry<Integer, HashSet<Change>>> changesByFile: changes) {
				for (Entry<Integer, HashSet<Change>> entry : changesByFile) {
					HashMap<String, IncrementalChange> incrementalChangeHashMap = incUpdatesOnDBpedia.updateGraphByGroupOfChanges(entry.getValue(), tgfdsByName);
					if (incrementalChangeHashMap == null)
						continue;
					for (String tgfdName : incrementalChangeHashMap.keySet()) {
						for (Entry<String, Set<ConstantLiteral>> allLiteralsInNewMatchEntry : incrementalChangeHashMap.get(tgfdName).getNewMatches().entrySet()) {
							numOfNewMatchesFoundInSnapshot++;
							HashSet<ConstantLiteral> match = new HashSet<>();
							Map<String, Integer> interestingnessMap = new HashMap<>();
//							String entityURI = null;
							for (ConstantLiteral matchedLiteral: allLiteralsInNewMatchEntry.getValue()) {
								for (ConstantLiteral activeAttribute : getActiveAttributesInPattern(patternTreeNode.getGraph().vertexSet(), true)) {
									if (!matchedLiteral.getVertexType().equals(activeAttribute.getVertexType())) continue;
									if (!matchedLiteral.getAttrName().equals(activeAttribute.getAttrName())) continue;
//									if (matchedLiteral.getVertexType().equals(patternTreeNode.getPattern().getCenterVertexType()) && matchedLiteral.getAttrName().equals("uri")) {
//										entityURI = matchedLiteral.getAttrValue();
//									}
									ConstantLiteral xLiteral = new ConstantLiteral(matchedLiteral.getVertexType(), matchedLiteral.getAttrName(), matchedLiteral.getAttrValue());
									interestingnessMap.merge(matchedLiteral.getVertexType(), 1, Integer::sum);
									match.add(xLiteral);
								}
							}
							if (this.isOnlyInterestingTGFDs() && interestingnessMap.values().stream().anyMatch(n -> n < 2)) {
								continue;
							} else if (!this.isOnlyInterestingTGFDs() && match.size() < patternTreeNode.getGraph().vertexSet().size()) {
								continue;
							}
							newMatches.put(allLiteralsInNewMatchEntry.getKey(), match);
						}

						for (Entry<String, Set<ConstantLiteral>> allLiteralsInRemovedMatchesEntry : incrementalChangeHashMap.get(tgfdName).getRemovedMatches().entrySet()) {
							HashSet<ConstantLiteral> match = new HashSet<>();
							Map<String, Integer> interestingnessMap = new HashMap<>();
//							String entityURI = null;
							for (ConstantLiteral matchedLiteral: allLiteralsInRemovedMatchesEntry.getValue()) {
								for (ConstantLiteral activeAttribute : getActiveAttributesInPattern(patternTreeNode.getGraph().vertexSet(), true)) {
									if (!matchedLiteral.getVertexType().equals(activeAttribute.getVertexType())) continue;
									if (!matchedLiteral.getAttrName().equals(activeAttribute.getAttrName())) continue;
//									if (matchedLiteral.getVertexType().equals(patternTreeNode.getPattern().getCenterVertexType()) && matchedLiteral.getAttrName().equals("uri")) {
//										entityURI = matchedLiteral.getAttrValue();
//									}
									ConstantLiteral xLiteral = new ConstantLiteral(matchedLiteral.getVertexType(), matchedLiteral.getAttrName(), matchedLiteral.getAttrValue());
									interestingnessMap.merge(matchedLiteral.getVertexType(), 1, Integer::sum);
									match.add(xLiteral);
								}
							}
							if (this.isOnlyInterestingTGFDs() && interestingnessMap.values().stream().anyMatch(n -> n < 2)) {
								continue;
							} else if (!this.isOnlyInterestingTGFDs() && match.size() < patternTreeNode.getGraph().vertexSet().size()) {
								continue;
							}
							removedMatches.putIfAbsent(allLiteralsInRemovedMatchesEntry.getKey(), match);
						}
					}
				}
			}
			System.out.println("Number of new matches found: " + numOfNewMatchesFoundInSnapshot);
			System.out.println("Number of new matches found that contain active attributes: " + newMatches.size());
			System.out.println("Number of removed matched: " + removedMatches.size());

			for (Set<ConstantLiteral> newMatch: newMatches.values()) {
				for (ConstantLiteral l: newMatch) {
					if (l.getVertexType().equals(patternTreeNode.getPattern().getCenterVertexType())
							&& l.getAttrName().equals("uri")) {
						String entityURI = l.getAttrValue();
						entityURIs.putIfAbsent(entityURI, createEmptyArrayListOfSize(this.getNumOfSnapshots()));
						entityURIs.get(entityURI).set(i+1, entityURIs.get(entityURI).get(i+1)+1);
					}
				}
				matchesPerTimestamps.get(i+1).add(newMatch);
			}

			int numOfOldMatchesFoundInSnapshot = 0;
			for (Set<ConstantLiteral> previousMatch : matchesPerTimestamps.get(i)) {
				String centerVertexType = patternTreeNode.getPattern().getCenterVertexType();
				String entityURI = null;
				for (ConstantLiteral l: previousMatch) {
					if (l.getVertexType().equals(centerVertexType) && l.getAttrName().equals("uri")) {
						entityURI = l.getAttrValue();
						break;
					}
				}
				if (removedMatches.containsValue(previousMatch))
					continue;
				if (newMatches.containsValue(previousMatch))
					continue;
				if (entityURI != null) {
					entityURIs.putIfAbsent(entityURI, createEmptyArrayListOfSize(this.getNumOfSnapshots()));
					entityURIs.get(entityURI).set(i + 1, entityURIs.get(entityURI).get(i + 1) + 1);
				}
				matchesPerTimestamps.get(i+1).add(previousMatch);
				numOfOldMatchesFoundInSnapshot++;
			}
			System.out.println("Number of valid old matches that are not new or removed: " + numOfOldMatchesFoundInSnapshot);
			System.out.println("Total number of matches with active attributes found in this snapshot: " + matchesPerTimestamps.get(i+1).size());

			numberOfMatchesFound += matchesPerTimestamps.get(i+1).size();

			incUpdatesOnDBpedia.deleteVertices(changeLoader.getAllChanges());

			printWithTime("Update and retrieve matches", System.currentTimeMillis()-startTime);
		}

		System.out.println("-------------------------------------");
		System.out.println("Number of entity URIs found: "+entityURIs.size());
		for (Map.Entry<String, List<Integer>> entry: entityURIs.entrySet()) {
			System.out.println(entry);
		}
		System.out.println("Total number of matches found in all snapshots: " + numberOfMatchesFound);
		this.setPatternSupport(entityURIs, patternTreeNode);

		return matchesPerTimestamps;
	}

	public static void printWithTime(String message, long runTimeInMS)
	{
		System.out.println(message + " time: " + runTimeInMS + "(ms) ** " +
				TimeUnit.MILLISECONDS.toSeconds(runTimeInMS) + "(sec) ** " +
				TimeUnit.MILLISECONDS.toMinutes(runTimeInMS) +  "(min)");
	}
}

