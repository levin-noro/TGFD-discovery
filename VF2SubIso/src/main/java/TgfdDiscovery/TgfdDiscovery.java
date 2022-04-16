package TgfdDiscovery;

import IncrementalRunner.IncUpdates;
import IncrementalRunner.IncrementalChange;
import Infra.*;
import VF2Runner.FastMatching;
import VF2Runner.LocalizedVF2Matching;
import VF2Runner.VF2SubgraphIsomorphism;
import changeExploration.*;
import graphLoader.DBPediaLoader;
import graphLoader.GraphLoader;
import graphLoader.IMDBLoader;
import graphLoader.SyntheticLoader;
import org.apache.commons.cli.*;
import org.apache.commons.math3.util.CombinatoricsUtils;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
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
import java.util.stream.Stream;

public class TgfdDiscovery {
	public static final int DEFAULT_NUM_OF_SNAPSHOTS = 3;
	public static final String NO_REUSE_MATCHES_PARAMETER_TEXT = "noReuseMatches";
	public static final String CHANGEFILE_PARAMETER_TEXT = "changefile";
	private static final int DEFAULT_MAX_LITERALS_NUM = 0;
	public static final String FREQUENT_SIZE_SET_PARAM = "f";
	public static final String MAX_LIT_PARAM = "maxLit";
	protected int INDIVIDUAL_SUPER_VERTEX_INDEGREE_FLOOR = 25;
	public static double MEDIAN_SUPER_VERTEX_TYPE_INDEGREE_FLOOR = 25.0;
	public static final double DEFAULT_MAX_SUPER_VERTEX_DEGREE = 1500.0;
	public static final double DEFAULT_AVG_SUPER_VERTEX_DEGREE = 30.0;
	private boolean fastMatching = false;
	private int maxNumOfLiterals = DEFAULT_MAX_LITERALS_NUM;
	private int T = DEFAULT_NUM_OF_SNAPSHOTS;
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
    private Integer numOfEdgesInAllGraphs = 0;
	private int numOfVerticesInAllGraphs = 0;
	private Map<String, Set<String>> vertexTypesToActiveAttributesMap; // freq attributes come from here
	public PatternTree patternTree;
	private boolean hasMinimalityPruning = true;
	private String graphSize = null;
	private boolean onlyInterestingTGFDs = true;
	private int k = DEFAULT_K;
	private double tgfdTheta = DEFAULT_TGFD_THETA;
	private double patternTheta = DEFAULT_PATTERN_THETA;
	private int gamma = DEFAULT_GAMMA;
	private int frequentSetSize = DEFAULT_FREQUENT_SIZE_SET;
	private Set<String> activeAttributesSet = null;
	private int previousLevelNodeIndex = 0;
	private int candidateEdgeIndex = 0;
	private int currentVSpawnLevel = 0;
	private ArrayList<ArrayList<TGFD>> discoveredTgfds;
	private long discoveryStartTime;
	private final ArrayList<Long> kRuntimes = new ArrayList<>();
	private String experimentStartTimeAndDateStamp = null;
	private boolean kExperiment = false;
	private boolean useChangeFile = false;
	private List<Entry<String, Integer>> sortedVertexHistogram; // freq nodes come from here
	private List<Entry<String, Integer>> sortedFrequentEdgesHistogram = null; // freq edges come from here
	private Map<String, Integer> vertexHistogram;
	private boolean hasSupportPruning = true;
	private final List<Double> medianPatternSupportsList = new ArrayList<>();
	protected ArrayList<Double> patternSupportsListForThisSnapshot = new ArrayList<>();
	private final List<Double> medianConstantTgfdSupportsList = new ArrayList<>();
	protected ArrayList<Double> constantTgfdSupportsListForThisSnapshot = new ArrayList<>();
	private final List<Double> medianGeneralTgfdSupportsList = new ArrayList<>();
	private ArrayList<Double> generalTgfdSupportsListForThisSnapshot = new ArrayList<>();
	private final ArrayList<Double> vertexFrequenciesList = new ArrayList<>();
	private final ArrayList<Double> edgeFrequenciesList = new ArrayList<>();
	private final List<Long> totalSupergraphCheckingTime = new ArrayList<>();
	private final List<Long> totalVisitedPathCheckingTime = new ArrayList<>();
	// TODO: Replace with RuntimeList
	private final List<Long> totalMatchingTime = new ArrayList<>();
	private final List<Long> totalSupersetPathCheckingTime = new ArrayList<>();
	private final List<Long> totalFindEntitiesTime = new ArrayList<>();
	private final List<Long> totalVSpawnTime = new ArrayList<>();
	private final List<Long> totalDiscoverConstantTGFDsTime = new ArrayList<>();
	private final List<Long> totalDiscoverGeneralTGFDTime = new ArrayList<>();
	private String experimentName;
	private String loader;
	private List<Entry<String, List<String>>> timestampToFilesMap = new ArrayList<>();
	private HashMap<String, JSONArray> changeFilesMap = null;
	private List<GraphLoader> graphs;
	private boolean isStoreInMemory = true;
	private Map<String, Double> vertexTypesToAvgInDegreeMap = new HashMap<>();
	private Model firstSnapshotTypeModel = null;
	private Model firstSnapshotDataModel = null;
	private long totalHistogramTime = 0;
	private final Set<String> interestLabelsSet = new HashSet<>();
	private final List<Integer> rhsInconsistencies = new ArrayList<>();
	private int numOfConsistentRHS = 0;
	private PrintStream logStream = null;
	private PrintStream summaryStream = null;
	private boolean printToLogFile = false;

	public TgfdDiscovery() {
		this.setDiscoveryStartTime(System.currentTimeMillis());

		printInfo();

		this.initializeTgfdLists();
	}

	public TgfdDiscovery(String[] args) {

		String timeAndDateStamp = ZonedDateTime.now(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern("uuuu.MM.dd.HH.mm.ss"));
		this.setExperimentStartTimeAndDateStamp(timeAndDateStamp);
		this.setDiscoveryStartTime(System.currentTimeMillis());

		Options options = TgfdDiscovery.initializeCmdOptions();
		CommandLine cmd = TgfdDiscovery.parseArgs(options, args);

		if (cmd.hasOption("path")) {
			this.path = cmd.getOptionValue("path").replaceFirst("^~", System.getProperty("user.home"));
			if (!Files.isDirectory(Path.of(this.getPath()))) {
				System.out.println(Path.of(this.getPath()) + " is not a valid directory.");
				System.exit(1);
			}
			this.setGraphSize(Path.of(this.getPath()).getFileName().toString());
		}

		if (!cmd.hasOption("loader")) {
			System.out.println("No loader is specified.");
			System.exit(1);
		} else {
			this.setLoader(cmd.getOptionValue("loader"));
		}

		this.setStoreInMemory(!cmd.hasOption("dontStore"));

		if (cmd.hasOption("name")) {
			this.setExperimentName(cmd.getOptionValue("name"));
		} else  {
			this.setExperimentName("experiment");
		}

		setPrintToLogFile(!cmd.hasOption("console"));
		divertOutputToLogFile();

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
			if (cmd.getOptionValue(CHANGEFILE_PARAMETER_TEXT).equalsIgnoreCase("type")) {
				this.setUseTypeChangeFile(true);
			}
		}
		this.setReUseMatches(reUseMatchesTemp);
		this.setValidationSearch(validationSearchTemp);

		this.setGeneratek0Tgfds(cmd.hasOption("k0"));
		this.setSkipK1(cmd.hasOption("skipK1"));

		this.setT(cmd.getOptionValue("t") == null ? TgfdDiscovery.DEFAULT_NUM_OF_SNAPSHOTS : Integer.parseInt(cmd.getOptionValue("t")));
		this.setNumOfSnapshots(this.getT());
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
			INDIVIDUAL_SUPER_VERTEX_INDEGREE_FLOOR = Integer.parseInt(cmd.getOptionValue("simplifySuperVertex"));
			this.setDissolveSuperVerticesBasedOnCount(true);
		}

		switch (this.getLoader().toLowerCase()) {
			case "dbpedia" -> this.setDBpediaTimestampsAndFilePaths(this.getPath());
			case "citation" -> this.setCitationTimestampsAndFilePaths();
			case "imdb" -> this.setImdbTimestampToFilesMapFromPath(this.getPath());
			case "synthetic" -> this.setSyntheticTimestampToFilesMapFromPath(this.getPath());
			default -> {
				System.out.println("No valid loader specified.");
				System.exit(1);
			}
		}

		if (cmd.hasOption("interestLabels")) {
			String[] interestLabels = cmd.getOptionValue("interestLabels").split(",");
			this.getInterestLabelsSet().addAll(List.of(interestLabels));
		}

		if (cmd.hasOption("fast")) {
			this.setFastMatching(true);
		}

		printInfo();
	}

	private void divertOutputToLogFile() {
		if (isPrintToLogFile()) {
			String fileName = "tgfd-discovery-log-" + this.getExperimentStartTimeAndDateStamp() + ".txt";
			if (logStream == null) {
				try {
					logStream = new PrintStream(fileName);
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
			}
			divertOutputToFile(fileName, logStream);
		}
	}

	private void divertOutputToSummaryFile() {
		if (isPrintToLogFile()) {
			String fileName = "tgfd-discovery-summary-" + this.getExperimentStartTimeAndDateStamp() + ".txt";
			if (summaryStream == null) {
				try {
					summaryStream = new PrintStream(fileName);
				} catch (FileNotFoundException e) {
					e.printStackTrace();
				}
			}
			divertOutputToFile(fileName, summaryStream);
		}
	}

	private void divertOutputToFile(String fileName, PrintStream stream) {

		System.setOut(stream);
	}

	protected void printInfo() {
		this.divertOutputToSummaryFile();

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
				String.join("=", "interestLabels", String.join(",", this.getInterestLabelsSet())),
		};

		System.out.println(String.join(", ", info));

		this.divertOutputToLogFile();
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
			System.exit(1);
		}
		return cmd;
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

	@Override
	public String toString() {
		String[] infoArray = {"G"+this.getGraphSize()
				, "t" + this.getT()
				, "k" + this.getCurrentVSpawnLevel()
				, this.getMaxNumOfLiterals() > 0 ? MAX_LIT_PARAM + this.getMaxNumOfLiterals() : ""
				, this.getPatternTheta() != this.getTgfdTheta() ? "pTheta" + this.getPatternTheta() : ""
				, "theta" + this.getTgfdTheta()
				, "gamma" + this.getGamma()
				, "freqSet" + (this.getFrequentSetSize() == Integer.MAX_VALUE ? "All" : this.getFrequentSetSize())
				, this.interestLabelsSet.size() > 0 ? "interestLabels" : ""
				, (this.isFastMatching() ? "fast" : "")
				, (this.isValidationSearch() ? "validation" : "")
				, (this.useChangeFile() ? "changefile"+(this.isUseTypeChangeFile()?"Type":"All") : "")
				, (!this.reUseMatches() ? "noMatchesReUsed" : "")
				, (!this.isOnlyInterestingTGFDs() ? "uninteresting" : "")
				, (!this.hasMinimalityPruning() ? "noMinimalityPruning" : "")
				, (!this.hasSupportPruning() ? "noSupportPruning" : "")
				, (this.isDissolveSuperVertexTypes() ? "simplifySuperTypes"+(this.getSuperVertexDegree()) : "")
				, (this.isDissolveSuperVerticesBasedOnCount() ? "simplifySuperNodes"+(INDIVIDUAL_SUPER_VERTEX_INDEGREE_FLOOR) : "")
				, (this.getExperimentStartTimeAndDateStamp() == null ? "" : this.getExperimentStartTimeAndDateStamp())
		};
		List<String> list = Stream.of(infoArray).filter(s -> !s.equals("")).collect(Collectors.toList());
		return String.join("-", list);
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
	}

	public void printExperimentRuntimestoFile() {
		try {
			PrintStream printStream = new PrintStream(this.getExperimentName() + "-runtimes-" + this.getExperimentStartTimeAndDateStamp() + ".txt");
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

		tgfdDiscovery.loadGraphsAndComputeHistogram2();
//        tgfdDiscovery.loadGraphsAndComputeHistogram(tgfdDiscovery.getTimestampToFilesMap());

        tgfdDiscovery.initialize();
		while (tgfdDiscovery.getCurrentVSpawnLevel() <= tgfdDiscovery.getK()) {

			PatternTreeNode patternTreeNode = null;
			while (patternTreeNode == null && tgfdDiscovery.getCurrentVSpawnLevel() <= tgfdDiscovery.getK())
				patternTreeNode = tgfdDiscovery.vSpawn();

			if (tgfdDiscovery.getCurrentVSpawnLevel() > tgfdDiscovery.getK())
				break;

			if (patternTreeNode == null)
				throw new NullPointerException("patternTreeNode == null");

			List<Set<Set<ConstantLiteral>>> matchesPerTimestamps;
			long matchingTime = System.currentTimeMillis();
			if (tgfdDiscovery.isValidationSearch())
				matchesPerTimestamps = tgfdDiscovery.getMatchesForPatternUsingVF2(patternTreeNode);
			else if (tgfdDiscovery.useChangeFile())
				matchesPerTimestamps = tgfdDiscovery.getMatchesUsingChangeFiles3(patternTreeNode);
			else
				matchesPerTimestamps = tgfdDiscovery.findMatchesUsingCenterVertices2(tgfdDiscovery.getGraphs(), patternTreeNode);

			matchingTime = System.currentTimeMillis() - matchingTime;
			TgfdDiscovery.printWithTime("Pattern matching", (matchingTime));
			tgfdDiscovery.addToTotalMatchingTime(matchingTime);

			if (tgfdDiscovery.doesNotSatisfyTheta(patternTreeNode)) {
				System.out.println("Mark as pruned. Real pattern support too low for pattern " + patternTreeNode.getPattern());
				if (tgfdDiscovery.hasSupportPruning())
					patternTreeNode.setIsPruned();
				continue;
			}

			if (tgfdDiscovery.isSkipK1() && tgfdDiscovery.getCurrentVSpawnLevel() == 1)
				continue;

			final long hSpawnStartTime = System.currentTimeMillis();
			ArrayList<TGFD> tgfds = tgfdDiscovery.hSpawn(patternTreeNode, matchesPerTimestamps);
			TgfdDiscovery.printWithTime("hSpawn", (System.currentTimeMillis() - hSpawnStartTime));
			tgfdDiscovery.getDiscoveredTgfds().get(tgfdDiscovery.getCurrentVSpawnLevel()).addAll(tgfds);
		}

		tgfdDiscovery.divertOutputToSummaryFile();
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
		printWithTime("Total vSpawn", this.getTotalVSpawnTime(level)-this.getTotalSupergraphCheckingTime(level));
		printWithTime("Total Supergraph Checking", this.getTotalSupergraphCheckingTime(level));
		printWithTime("Total Matching", this.getTotalMatchingTime(level));
		printWithTime("Total Visited Path Checking", this.getTotalVisitedPathCheckingTime(level));
		printWithTime("Total Superset Path Checking", this.getTotalSupersetPathCheckingTime(level));
		printWithTime("Total Find Entities", this.getTotalFindEntitiesTime(level));
		printWithTime("Total Discover Constant TGFDs", this.getTotalDiscoverConstantTGFDsTime(level));
		printWithTime("Total Discover General TGFD", this.getTotalDiscoverGeneralTGFDTime(level));
	}

	public void printTimeStatistics() {
		System.out.println("----------------Total Time Statistics-----------------");
		printWithTime("Total Histogram", this.getTotalHistogramTime());
		printWithTime("Total vSpawn", this.getTotalVSpawnTime().stream().reduce(0L, Long::sum)-this.getTotalSupergraphCheckingTime().stream().reduce(0L, Long::sum));
		printWithTime("Total Supergraph Checking", this.getTotalSupergraphCheckingTime().stream().reduce(0L, Long::sum));
		printWithTime("Total Matching", this.getTotalMatchingTime().stream().reduce(0L, Long::sum));
		printWithTime("Total Visited Path Checking", this.getTotalVisitedPathCheckingTime().stream().reduce(0L, Long::sum));
		printWithTime("Total Superset Path Checking", this.getTotalSupersetPathCheckingTime().stream().reduce(0L, Long::sum));
		printWithTime("Total Find Entities", this.getTotalFindEntitiesTime().stream().reduce(0L, Long::sum));
		printWithTime("Total Discover Constant TGFDs", this.getTotalDiscoverConstantTGFDsTime().stream().reduce(0L, Long::sum));
		printWithTime("Total Discover General TGFD", this.getTotalDiscoverGeneralTGFDTime().stream().reduce(0L, Long::sum));
		System.out.println("----------------Additional Statistics-----------------");
		System.out.println("Number of candidate constant TGFDs: "+(this.numOfConsistentRHS+this.rhsInconsistencies.size()));
		System.out.println("Number of consistent candidate constant TGFDs: "+this.numOfConsistentRHS);
		System.out.println("Number of inconsistent candidate constant TGFDs: "+this.rhsInconsistencies.size());
		System.out.println("Average number of inconsistencies per inconsistent candidate constant TGFD: "+((double)this.rhsInconsistencies.stream().reduce(0, Integer::sum) / (double)this.rhsInconsistencies.size()));
		List<Integer> intervalWidths = this.getDiscoveredTgfds().stream().map(list -> list.stream().map(tgfd -> tgfd.getDelta().getIntervalWidth()).collect(Collectors.toList())).flatMap(List::stream).collect(Collectors.toList());//(ArrayList::listIterator).map(tgfdListIterator -> tgfdListIterator.next().getDelta().getIntervalWidth()).collect(Collectors.toList());
		intervalWidths.sort(Comparator.naturalOrder());
		if (intervalWidths.size() > 0) {
			System.out.println("Minimum delta interval width: " + intervalWidths.get(0));
			System.out.println("Maximum delta interval width: " + intervalWidths.get(intervalWidths.size() - 1));
			System.out.println("Average delta interval width: "+intervalWidths.stream().reduce(0, Integer::sum)/intervalWidths.size());
		} else {
			System.out.println("Cannot report statistics on delta interval widths. No TGFDs found in dataset");
		}
	}

	protected void countTotalNumberOfMatchesFound(List<Set<Set<ConstantLiteral>>> matchesPerTimestamps) {
		int numberOfMatchesFound = 0;
		for (Set<Set<ConstantLiteral>> matchesInOneTimestamp : matchesPerTimestamps) {
			numberOfMatchesFound += matchesInOneTimestamp.size();
		}
		System.out.println("Total number of matches found across all snapshots: " + numberOfMatchesFound);
	}

	public List<Set<Set<ConstantLiteral>>> findMatchesUsingCenterVertices2(List<GraphLoader> graphs, PatternTreeNode patternTreeNode) {

		LocalizedVF2Matching localizedVF2Matching;
		if (this.isFastMatching())
			localizedVF2Matching = new FastMatching(patternTreeNode, this.getT(), this.isOnlyInterestingTGFDs(), this.getVertexTypesToActiveAttributesMap(), this.reUseMatches());
		else
			localizedVF2Matching = new LocalizedVF2Matching(patternTreeNode, this.getT(), this.isOnlyInterestingTGFDs(), this.getVertexTypesToActiveAttributesMap(), this.reUseMatches());

		localizedVF2Matching.findMatches(graphs, this.getT());

		List<Set<Set<ConstantLiteral>>> matchesPerTimestamp = localizedVF2Matching.getMatchesPerTimestamp();
		this.countTotalNumberOfMatchesFound(matchesPerTimestamp);

		Map<String, List<Integer>> entityURIs = localizedVF2Matching.getEntityURIs();
		if (this.reUseMatches())
			patternTreeNode.setEntityURIs(entityURIs);

		localizedVF2Matching.printEntityURIs();

		double S = this.getVertexHistogram().get(patternTreeNode.getPattern().getCenterVertexType());
		double patternSupport = TgfdDiscovery.calculatePatternSupport(entityURIs, S, this.getT());
		this.patternSupportsListForThisSnapshot.add(patternSupport);
		patternTreeNode.setPatternSupport(patternSupport);

		return matchesPerTimestamp;
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

	protected void printSupportStatisticsForThisSnapshot() {
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

	// TODO: Can this be merged with the code in histogram?
	protected void dissolveSuperVerticesBasedOnCount(GraphLoader graph) {
		System.out.println("Dissolving super vertices based on count...");
		System.out.println("Initial edge count of first snapshot: "+graph.getGraph().getGraph().edgeSet().size());
		for (Vertex v: graph.getGraph().getGraph().vertexSet()) {
			int inDegree = graph.getGraph().getGraph().incomingEdgesOf(v).size();
			if (inDegree > INDIVIDUAL_SUPER_VERTEX_INDEGREE_FLOOR) {
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

	public void loadGraphsAndComputeHistogram2() {
		this.divertOutputToSummaryFile();
		System.out.println("Computing Histogram...");
		Histogram histogram = new Histogram(this.getT(), this.getTimestampToFilesMap(), this.getLoader(), this.getFrequentSetSize(), this.getGamma(), this.getInterestLabelsSet());
		Integer superVertexDegree = this.isDissolveSuperVerticesBasedOnCount() ? INDIVIDUAL_SUPER_VERTEX_INDEGREE_FLOOR : null;
		if (this.useChangeFile()) {
			List<String> changefilePaths = new ArrayList<>();
			for (int t = 1; t < this.getT(); t++) {
				String changeFilePath = "changes_t" + t + "_t" + (t+1) + "_" + this.getGraphSize() + ".json";
				changefilePaths.add(changeFilePath);
			}
			histogram.computeHistogramUsingChangefiles(changefilePaths, this.isStoreInMemory(), superVertexDegree, this.isDissolveSuperVertexTypes());
			if (this.isStoreInMemory())
				this.setChangeFilesMap(histogram.getChangefilesToJsonArrayMap());
		} else {
			histogram.computeHistogramByReadingGraphsFromFile(this.isStoreInMemory(), superVertexDegree);
			if (this.isStoreInMemory())
				this.setGraphs(histogram.getGraphs());
		}

		this.setVertexTypesToAvgInDegreeMap(histogram.getVertexTypesToMedianInDegreeMap());

		this.setActiveAttributesSet(histogram.getActiveAttributesSet());
		this.setVertexTypesToActiveAttributesMap(histogram.getVertexTypesToActiveAttributesMap());

		this.setSortedFrequentEdgesHistogram(histogram.getSortedFrequentEdgesHistogram());
		this.setSortedVertexHistogram(histogram.getSortedVertexTypesHistogram());
		this.setVertexHistogram(histogram.getVertexHistogram());

		this.setTotalHistogramTime(histogram.getTotalHistogramTime());
		this.divertOutputToLogFile();
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
				Set<String> attrNameSet = this.getVertexTypesToActiveAttributesMap().get(vertexType);
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

	public ArrayList<TGFD> deltaDiscovery(PatternTreeNode patternNode, LiteralTreeNode literalTreeNode, AttributeDependency literalPath, List<Set<Set<ConstantLiteral>>> matchesPerTimestamps) {
		ArrayList<TGFD> tgfds = new ArrayList<>();

		// Add dependency attributes to pattern
		// TODO: Fix - when multiple vertices in a pattern have the same type, attribute values get overwritten
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
		// TODO: Try discover general TGFD even if no constant TGFD candidate met support threshold
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
		// TODO: Verify if TreeSet<Pair> is being sorted correctly
		// TODO: Does this method only produce intervals (x,y), where x == y ?
		ArrayList<Pair> currSatisfyingAttrValues = new ArrayList<>();
		for (Pair deltaPair: deltaToPairsMap.keySet().stream().sorted().collect(Collectors.toList())) {
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
			double support = TgfdDiscovery.calculateSupport(numberOfSatisfyingPairs, entitiesSize, this.getT());
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

	private ArrayList<TGFD> discoverConstantTGFDs(PatternTreeNode patternNode, ConstantLiteral yLiteral, Map<Set<ConstantLiteral>, ArrayList<Entry<ConstantLiteral, List<Integer>>>> entities, Map<Pair, ArrayList<TreeSet<Pair>>> deltaToPairsMap) {
		long discoverConstantTGFDsTime = System.currentTimeMillis(); long supersetPathCheckingTimeForThisDependency = 0;
		ArrayList<TGFD> tgfds = new ArrayList<>();
		String yVertexType = yLiteral.getVertexType();
		String yAttrName = yLiteral.getAttrName();
		for (Entry<Set<ConstantLiteral>, ArrayList<Entry<ConstantLiteral, List<Integer>>>> entityEntry : entities.entrySet()) {
			VF2PatternGraph newPattern = patternNode.getPattern().copy();
			Dependency newDependency = new Dependency();
			AttributeDependency constantPath = new AttributeDependency();
			for (Vertex v : newPattern.getPattern().vertexSet()) {
				String vType = new ArrayList<>(v.getTypes()).get(0);
				if (vType.equalsIgnoreCase(yVertexType)) { // TODO: What if our pattern has duplicate vertex types?
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
				this.numOfConsistentRHS += 1;
				List<Integer> timestampCounts = rhsAttrValuesTimestampsSortedByFreq.get(0).getValue();
				Pair candidateDelta = getMinMaxPair(timestampCounts);
				if (candidateDelta == null) continue;
				candidateDeltas.add(candidateDelta);
			} else if (rhsAttrValuesTimestampsSortedByFreq.size() > 1) {
				this.rhsInconsistencies.add(rhsAttrValuesTimestampsSortedByFreq.size());
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
					double candidateSupport = TgfdDiscovery.calculateSupport(numerator, entities.size(), this.getT());

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
			boolean isNotMinimal = false;
			if (this.hasMinimalityPruning() && constantPath.isSuperSetOfPathAndSubsetOfDelta(patternNode.getAllMinimalConstantDependenciesOnThisPath())) { // Ensures we don't expand constant TGFDs from previous iterations
				System.out.println("Candidate constant TGFD " + constantPath + " is a superset of an existing minimal constant TGFD");
				isNotMinimal = true;
			}
			supersetPathCheckingTime = System.currentTimeMillis()-supersetPathCheckingTime;
			supersetPathCheckingTimeForThisDependency += supersetPathCheckingTime;
			printWithTime("supersetPathCheckingTime", supersetPathCheckingTime);
			addToTotalSupersetPathCheckingTime(supersetPathCheckingTime);

			if (isNotMinimal)
				continue;

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
				if (this.hasMinimalityPruning())
					patternNode.addMinimalConstantDependency(constantPath);
			}
		}

		discoverConstantTGFDsTime = System.currentTimeMillis() - discoverConstantTGFDsTime - supersetPathCheckingTimeForThisDependency;
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

		for (Map.Entry<String,Integer> frequentEdgeEntry : this.getSortedFrequentEdgesHistogram()) {
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

	public void setSyntheticTimestampToFilesMapFromPath(String path) {
		HashMap<String, List<String>> timestampToFilesMap = generateSyntheticTimestampToFilesMapFromPath(path);
		this.setTimestampToFilesMap(new ArrayList<>(timestampToFilesMap.entrySet()));
	}

	@NotNull
	public static HashMap<String, List<String>> generateSyntheticTimestampToFilesMapFromPath(String path) {
		List<File> allFilesInDirectory = new ArrayList<>(List.of(Objects.requireNonNull(new File(path).listFiles(File::isFile))));
		allFilesInDirectory.sort(Comparator.comparing(File::getName));
		HashMap<String,List<String>> timestampToFilesMap = new HashMap<>();
		for (File ntFile: allFilesInDirectory) {
			String regex = "^graph([0-9]+)\\.txt$";
			Pattern pattern = Pattern.compile(regex);
			Matcher matcher = pattern.matcher(ntFile.getName());
			if (matcher.find()) {
				String timestamp = matcher.group(1);
				timestampToFilesMap.putIfAbsent(timestamp, new ArrayList<>());
				timestampToFilesMap.get(timestamp).add(ntFile.getPath());
			}
		}
		return timestampToFilesMap;
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
		if (this.isUseTypeChangeFile()) { // TODO: Deprecate type changefiles?
			for (Map.Entry<String,Integer> frequentVertexTypeEntry : this.getVertexHistogram().entrySet()) {
				for (int i = 0; i < this.getT() - 1; i++) {
					System.out.println("-----------Snapshot (" + (i + 2) + ")-----------");
					String changeFilePath = "changes_t" + (i + 1) + "_t" + (i + 2) + "_" + frequentVertexTypeEntry.getKey() + ".json";
					JSONArray jsonArray = readJsonArrayFromFile(changeFilePath);
					System.out.println("Storing " + changeFilePath + " in memory");
					changeFilesMap.put(changeFilePath, jsonArray);
				}
			}
		} else {
			for (int i = 0; i < this.getT() - 1; i++) {
				System.out.println("-----------Snapshot (" + (i + 2) + ")-----------");
				String changeFilePath = "changes_t" + (i + 1) + "_t" + (i + 2) + "_" + this.getGraphSize() + ".json";
				JSONArray jsonArray = readJsonArrayFromFile(changeFilePath);
				System.out.println("Storing " + changeFilePath + " in memory");
				changeFilesMap.put(changeFilePath, jsonArray);
			}
		}
		this.setChangeFilesMap(changeFilesMap);
	}

	public static JSONArray readJsonArrayFromFile(String changeFilePath) {
		System.out.println("Reading JSON array from file "+changeFilePath);
		JSONParser parser = new JSONParser();
		Object json;
		JSONArray jsonArray = null;
		try {
			json = parser.parse(new FileReader(changeFilePath));
			jsonArray = (JSONArray) json;
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
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

	protected static boolean isUsedVertexType(String vertexType, ArrayList<ConstantLiteral> parentsPathToRoot) {
		for (ConstantLiteral literal : parentsPathToRoot) {
			if (literal.getVertexType().equals(vertexType)) {
				System.out.println("Skip. Literal has a vertex type that is already part of interesting dependency.");
				return true;
			}
		}
		return false;
	}

	protected static boolean literalPathIsMissingTypesInPattern(ArrayList<ConstantLiteral> parentsPathToRoot, Set<Vertex> patternVertexSet) {
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
		System.out.println("VSpawn level " + this.getCurrentVSpawnLevel());
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
		TgfdDiscovery.printWithTime("vSpawn", vSpawnTime);
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

	public ArrayList<ArrayList<TGFD>> getDiscoveredTgfds() {
		return discoveredTgfds;
	}

	public void initializeTgfdLists() {
		this.discoveredTgfds = new ArrayList<>();
		for (int vSpawnLevel = 0; vSpawnLevel <= this.getK(); vSpawnLevel++) {
			getDiscoveredTgfds().add(new ArrayList<>());
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
			this.setNumOfSnapshots(this.getChangeFilesMap().entrySet().size() + 1);
//			this.setNumOfSnapshots(this.getTimestampToFilesMap().size());
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
		return T;
	}

	public void setT(int t) {
		this.T = t;
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

	public long getDiscoveryStartTime() {
		return discoveryStartTime;
	}

	public void setDiscoveryStartTime(long discoveryStartTime) {
		this.discoveryStartTime = discoveryStartTime;
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

	public String getExperimentStartTimeAndDateStamp() {
		return experimentStartTimeAndDateStamp;
	}

	public void setExperimentStartTimeAndDateStamp(String experimentStartTimeAndDateStamp) {
		this.experimentStartTimeAndDateStamp = experimentStartTimeAndDateStamp;
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

	public int getNumOfVerticesInAllGraphs() {
		return numOfVerticesInAllGraphs;
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

	public List<Long> getTotalSupergraphCheckingTime() {
		return totalSupergraphCheckingTime;
	}

	public Long getTotalSupergraphCheckingTime(int index) {
		return returnLongAtIndexIfExistsElseZero(this.getTotalSupergraphCheckingTime(), index);
	}

	public void addToTotalSupergraphCheckingTime(long supergraphCheckingTime) {
		addToValueInListAtIndex(this.getTotalSupergraphCheckingTime(), this.getCurrentVSpawnLevel(), supergraphCheckingTime);
	}

	public List<Long> getTotalVisitedPathCheckingTime() {
		return totalVisitedPathCheckingTime;
	}

	public Long getTotalVisitedPathCheckingTime(int index) {
		return returnLongAtIndexIfExistsElseZero(this.getTotalVisitedPathCheckingTime(), index);
	}

	public void addToTotalVisitedPathCheckingTime(long visitedPathCheckingTime) {
		addToValueInListAtIndex(this.getTotalVisitedPathCheckingTime(), this.getCurrentVSpawnLevel(), visitedPathCheckingTime);
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

	public void addToTotalSupersetPathCheckingTime(long supersetPathCheckingTime) {
		addToValueInListAtIndex(this.getTotalSupersetPathCheckingTime(), this.getCurrentVSpawnLevel(), supersetPathCheckingTime);
	}

	public List<Long> getTotalFindEntitiesTime() {
		return totalFindEntitiesTime;
	}

	public Long getTotalFindEntitiesTime(int index) {
		return returnLongAtIndexIfExistsElseZero(this.getTotalFindEntitiesTime(), index);
	}

	public void addToTotalFindEntitiesTime(long findEntitiesTime) {
		addToValueInListAtIndex(this.getTotalFindEntitiesTime(), this.getCurrentVSpawnLevel(), findEntitiesTime);
	}

	public List<Long> getTotalDiscoverConstantTGFDsTime() {
		return totalDiscoverConstantTGFDsTime;
	}

	public Long getTotalDiscoverConstantTGFDsTime(int index) {
		return returnLongAtIndexIfExistsElseZero(this.getTotalDiscoverConstantTGFDsTime(), index);
	}

	public void addToTotalDiscoverConstantTGFDsTime(long discoverConstantTGFDsTime) {
		addToValueInListAtIndex(this.getTotalDiscoverConstantTGFDsTime(), this.getCurrentVSpawnLevel(), discoverConstantTGFDsTime);
	}

	public List<Long> getTotalDiscoverGeneralTGFDTime() {
		return totalDiscoverGeneralTGFDTime;
	}

	public Long getTotalDiscoverGeneralTGFDTime(int index) {
		return returnLongAtIndexIfExistsElseZero(this.getTotalDiscoverGeneralTGFDTime(), index);
	}

	public void addToTotalDiscoverGeneralTGFDTime(long discoverGeneralTGFDTime) {
		addToValueInListAtIndex(this.getTotalDiscoverGeneralTGFDTime(), this.getCurrentVSpawnLevel(), discoverGeneralTGFDTime);
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

	public Set<String> getInterestLabelsSet() {
		return interestLabelsSet;
	}

	public Set<String> getActiveAttributesSet() {
		return activeAttributesSet;
	}

	public void setActiveAttributesSet(Set<String> activeAttributesSet) {
		this.activeAttributesSet = activeAttributesSet;
	}

	public Map<String, Integer> getVertexHistogram() {
		return vertexHistogram;
	}

	public Map<String, Set<String>> getVertexTypesToActiveAttributesMap() {
		return vertexTypesToActiveAttributesMap;
	}

	public List<Entry<String, Integer>> getSortedFrequentEdgesHistogram() {
		return sortedFrequentEdgesHistogram;
	}

	public void setSortedFrequentEdgesHistogram(List<Entry<String, Integer>> sortedFrequentEdgesHistogram) {
		this.sortedFrequentEdgesHistogram = sortedFrequentEdgesHistogram;
	}

	public void setSortedVertexHistogram(List<Entry<String, Integer>> sortedVertexHistogram) {
		this.sortedVertexHistogram = sortedVertexHistogram;
	}

	public void setVertexTypesToActiveAttributesMap(Map<String, Set<String>> vertexTypesToActiveAttributesMap) {
		this.vertexTypesToActiveAttributesMap = vertexTypesToActiveAttributesMap;
	}

	public void setVertexHistogram(Map<String, Integer> vertexHistogram) {
		this.vertexHistogram = vertexHistogram;
	}

	public boolean isPrintToLogFile() {
		return printToLogFile;
	}

	public void setPrintToLogFile(boolean printToLogFile) {
		this.printToLogFile = printToLogFile;
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

//			int numOfInstancesOfVertexType = this.getSortedVertexHistogram().get(i).getValue();
//			int numOfInstancesOfAllVertexTypes = this.getNumOfVerticesInAllGraphs();

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
					LocalizedVF2Matching localizedVF2Matching = new LocalizedVF2Matching(patternTreeNode, this.getT(), this.isOnlyInterestingTGFDs(), this.getVertexTypesToActiveAttributesMap(), this.reUseMatches());
					for (int t = 0; t < this.getT(); t++)
						localizedVF2Matching.extractListOfCenterVerticesInSnapshot(patternTreeNode.getPattern().getCenterVertexType(), entityURIs, t, this.getGraphs().get(t));

					System.out.println("Number of center vertex URIs found containing active attributes: " + entityURIs.size());
					for (Map.Entry<String, List<Integer>> entry: entityURIs.entrySet()) {
						System.out.println(entry);
					}
					if (this.reUseMatches())
						patternTreeNode.setEntityURIs(entityURIs);
					double S = this.getVertexHistogram().get(patternTreeNode.getPattern().getCenterVertexType());
					double patternSupport = TgfdDiscovery.calculatePatternSupport(entityURIs, S, this.getT());
					this.patternSupportsListForThisSnapshot.add(patternSupport);
					patternTreeNode.setPatternSupport(patternSupport);
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
					LocalizedVF2Matching localizedVF2Matching;
					if (this.isFastMatching())
						localizedVF2Matching = new FastMatching(patternTreeNode, this.getT(), this.isOnlyInterestingTGFDs(), this.getVertexTypesToActiveAttributesMap(), this.reUseMatches());
					else
						localizedVF2Matching = new LocalizedVF2Matching(patternTreeNode, this.getT(), this.isOnlyInterestingTGFDs(), this.getVertexTypesToActiveAttributesMap(), this.reUseMatches());

					localizedVF2Matching.findMatches(this.getGraphs(), this.getT());

					Map<String, List<Integer>> entityURIs = localizedVF2Matching.getEntityURIs();
					localizedVF2Matching.printEntityURIs();
					if (this.reUseMatches())
						patternTreeNode.setEntityURIs(entityURIs);

					double S = this.getVertexHistogram().get(patternTreeNode.getPattern().getCenterVertexType());
					double patternSupport = TgfdDiscovery.calculatePatternSupport(entityURIs, S, this.getT());
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
//		System.out.println("Calculating pattern support...");
//		String centerVertexType = patternTreeNode.getPattern().getCenterVertexType();
//		System.out.println("Center vertex type: " + centerVertexType);
		int numOfPossiblePairs = 0;
		for (Entry<String, List<Integer>> entityUriEntry : entityURIs.entrySet()) {
			int numberOfAcrossMatchesOfEntity = (int) entityUriEntry.getValue().stream().filter(x -> x > 0).count();
			int k = 2;
			if (numberOfAcrossMatchesOfEntity >= k) {
				numOfPossiblePairs += CombinatoricsUtils.binomialCoefficient(numberOfAcrossMatchesOfEntity, k);
			}
			int numberOfWithinMatchesOfEntity = (int) entityUriEntry.getValue().stream().filter(x -> x > 1).count();
			numOfPossiblePairs += numberOfWithinMatchesOfEntity;
		}
//		int S = this.vertexHistogram.get(centerVertexType);
// 		patternTreeNode.calculatePatternSupport(patternSupport);
		return calculateSupport(numOfPossiblePairs, S, T);
	}

	protected static double calculateSupport(double numerator, double S, int T) {
		System.out.println("S = "+S);
		double denominator = S * CombinatoricsUtils.binomialCoefficient(T+1,2);
		System.out.print("Support: " + numerator + " / " + denominator + " = ");
		if (numerator > denominator)
			throw new IllegalArgumentException("numerator > denominator");
		double support = numerator / denominator;
		System.out.println(support);
		return support;
	}

	protected boolean doesNotSatisfyTheta(PatternTreeNode patternTreeNode) {
		if (patternTreeNode.getPatternSupport() == null)
			throw new IllegalArgumentException("patternTreeNode.getPatternSupport() == null");
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
			} else if (edge.getSource().getTypes().contains(targetType) && edge.getTarget().getTypes().contains(sourceType)) {
				return true;
			}
		}
		return false;
	}

	public PatternTreeNode vSpawn() {

		long vSpawnTime = System.currentTimeMillis();

		if (this.getCandidateEdgeIndex() > this.getSortedFrequentEdgesHistogram().size()-1) {
			this.setCandidateEdgeIndex(0);
			this.setPreviousLevelNodeIndex(this.getPreviousLevelNodeIndex() + 1);
		}

		if (this.getPreviousLevelNodeIndex() >= this.patternTree.getLevel(this.getCurrentVSpawnLevel()-1).size()) {
			this.getkRuntimes().add(System.currentTimeMillis() - this.getDiscoveryStartTime());
			this.printTgfdsToFile(this.getExperimentName(), this.getDiscoveredTgfds().get(this.getCurrentVSpawnLevel()));
			if (this.iskExperiment()) this.printExperimentRuntimestoFile();
			this.printSupportStatisticsForThisSnapshot();
			this.printTimeStatisticsForThisSnapshot(this.getCurrentVSpawnLevel());
			this.addToTotalVSpawnTime(System.currentTimeMillis()-vSpawnTime);
			this.setCurrentVSpawnLevel(this.getCurrentVSpawnLevel() + 1);
			vSpawnTime = System.currentTimeMillis();
			if (this.getCurrentVSpawnLevel() > this.getK()) {
				this.addToTotalVSpawnTime(System.currentTimeMillis()-vSpawnTime);
				return null;
			}
			this.patternTree.addLevel();
			this.setPreviousLevelNodeIndex(0);
			this.setCandidateEdgeIndex(0);
		}

		System.out.println("Performing VSpawn");
		System.out.println("VSpawn Level " + this.getCurrentVSpawnLevel());

		ArrayList<PatternTreeNode> previousLevel = this.patternTree.getLevel(this.getCurrentVSpawnLevel() - 1);
		if (previousLevel.size() == 0) {
			System.out.println("Previous level of vSpawn contains no pattern nodes.");
			this.setPreviousLevelNodeIndex(this.getPreviousLevelNodeIndex() + 1);
			this.addToTotalVSpawnTime(System.currentTimeMillis()-vSpawnTime);
			return null;
		}
		PatternTreeNode previousLevelNode = previousLevel.get(this.getPreviousLevelNodeIndex());
		System.out.println("Processing previous level node " + this.getPreviousLevelNodeIndex() + "/" + (previousLevel.size()-1));
		System.out.println("Performing VSpawn on pattern: " + previousLevelNode.getPattern());

		System.out.println("Level " + (this.getCurrentVSpawnLevel() - 1) + " pattern: " + previousLevelNode.getPattern());
		if (this.hasSupportPruning() && previousLevelNode.isPruned()) {
			System.out.println("Marked as pruned. Skip.");
			this.setPreviousLevelNodeIndex(this.getPreviousLevelNodeIndex() + 1);
			this.addToTotalVSpawnTime(System.currentTimeMillis()-vSpawnTime);
			return null;
		}

		System.out.println("Processing candidate edge " + this.getCandidateEdgeIndex() + "/" + (this.getSortedFrequentEdgesHistogram().size()-1));
		Map.Entry<String, Integer> candidateEdge = this.getSortedFrequentEdgesHistogram().get(this.getCandidateEdgeIndex());
		String candidateEdgeString = candidateEdge.getKey();
		System.out.println("Candidate edge:" + candidateEdgeString);


		String sourceVertexType = candidateEdgeString.split(" ")[0];
		String targetVertexType = candidateEdgeString.split(" ")[2];

		if (this.getVertexTypesToActiveAttributesMap().get(targetVertexType).size() == 0) {
			System.out.println("Target vertex in candidate edge does not contain active attributes");
			this.setCandidateEdgeIndex(this.getCandidateEdgeIndex() + 1);
			return null;
		}

		// TODO: We should add support for duplicate vertex types in the future
		if (sourceVertexType.equals(targetVertexType)) {
			System.out.println("Candidate edge contains duplicate vertex types. Skip.");
			this.setCandidateEdgeIndex(this.getCandidateEdgeIndex() + 1);
			this.addToTotalVSpawnTime(System.currentTimeMillis()-vSpawnTime);
			return null;
		}
		String edgeType = candidateEdgeString.split(" ")[1];

		// Check if candidate edge already exists in pattern
		if (isDuplicateEdge(previousLevelNode.getPattern(), edgeType, sourceVertexType, targetVertexType)) {
			System.out.println("Candidate edge: " + candidateEdge.getKey());
			System.out.println("already exists in pattern");
			this.setCandidateEdgeIndex(this.getCandidateEdgeIndex() + 1);
			this.addToTotalVSpawnTime(System.currentTimeMillis()-vSpawnTime);
			return null;
		}

		if (isMultipleEdge(previousLevelNode.getPattern(), sourceVertexType, targetVertexType)) {
			System.out.println("We do not support multiple edges between existing vertices.");
			this.setCandidateEdgeIndex(this.getCandidateEdgeIndex() + 1);
			this.addToTotalVSpawnTime(System.currentTimeMillis()-vSpawnTime);
			return null;
		}

		// Checks if candidate edge extends pattern
		PatternVertex sourceVertex = isDuplicateVertex(previousLevelNode.getPattern(), sourceVertexType);
		PatternVertex targetVertex = isDuplicateVertex(previousLevelNode.getPattern(), targetVertexType);
		if (sourceVertex == null && targetVertex == null) {
			System.out.println("Candidate edge: " + candidateEdge.getKey());
			System.out.println("does not extend from pattern");
			this.setCandidateEdgeIndex(this.getCandidateEdgeIndex() + 1);
			this.addToTotalVSpawnTime(System.currentTimeMillis()-vSpawnTime);
			return null;
		}

		PatternTreeNode patternTreeNode = null;
		// TODO: FIX label conflict. What if an edge has same vertex type on both sides?
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

			// TODO: Debug - Why does this work with strings but not subgraph isomorphism???
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
				newPattern.assignOptimalCenterVertex(this.getVertexTypesToAvgInDegreeMap(), this.isFastMatching());
				patternTreeNode = new PatternTreeNode(newPattern, previousLevelNode, candidateEdgeString);
//				for (RelationshipEdge e : newPattern.getPattern().edgeSet()) {
//					Vertex source = e.getSource();
//					String sourceType = source.getTypes().iterator().next();
//					Vertex target = e.getTarget();
//					String targetType = target.getTypes().iterator().next();
//					Vertex centerVertex = this.getVertexTypesToAvgInDegreeMap().get(sourceType) > this.getVertexTypesToAvgInDegreeMap().get(targetType) ? source : target;
//					newPattern.setCenterVertex(centerVertex);
//					newPattern.setRadius(1);
//					newPattern.setDiameter(1);
//				}
				this.patternTree.getTree().get(this.getCurrentVSpawnLevel()).add(patternTreeNode);
				this.patternTree.findSubgraphParents(this.getCurrentVSpawnLevel()-1, patternTreeNode);
				this.patternTree.findCenterVertexParent(this.getCurrentVSpawnLevel()-1, patternTreeNode, true);
			} else {
				newPattern.assignOptimalCenterVertex(this.getVertexTypesToAvgInDegreeMap(), this.isFastMatching());
				boolean considerAlternativeParents = true;
//				newPattern.getCenterVertexType(); // TODO: Should we move the following code into either VF2PatternGraph or PatternTreeNode?
				if (this.isFastMatching() && this.getCurrentVSpawnLevel() > 2) {
					if (newPattern.getPatternType() == PatternType.Line) {
//						newPattern.setCenterVertex(newPattern.getFirstNode());
						considerAlternativeParents = false;
					}
				}
//				else {
//					int minRadius = newPattern.getPattern().vertexSet().size();
//					for (Vertex newV : newPattern.getPattern().vertexSet()) {
//						minRadius = Math.min(minRadius, newPattern.calculateRadiusForGivenVertex(newV));
//					}
//					Map<Vertex, Double> maxDegreeTypes = new HashMap<>();
//					for (Vertex newV : newPattern.getPattern().vertexSet()) {
//						if (minRadius == newPattern.calculateRadiusForGivenVertex(newV)) {
//							String type = newV.getTypes().iterator().next();
//							maxDegreeTypes.put(newV, this.getVertexTypesToAvgInDegreeMap().get(type));
//						}
//					}
//					if (maxDegreeTypes.size() <= 0)
//						throw new IllegalArgumentException("maxDegreeTypes.size() <= 0");
//					List<Entry<Vertex, Double>> entries = new ArrayList<>(maxDegreeTypes.entrySet());
//					entries.sort(new Comparator<Entry<Vertex, Double>>() {
//						@Override
//						public int compare(Entry<Vertex, Double> o1, Entry<Vertex, Double> o2) {
//							return o2.getValue().compareTo(o1.getValue());
//						}
//					});
//					Vertex centerVertex = entries.get(0).getKey();
//					newPattern.setCenterVertex(centerVertex);
//				}
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
		this.addToTotalVSpawnTime(System.currentTimeMillis()-vSpawnTime);
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

	// TODO: Should this be done using real subgraph isomorphism instead of strings?
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

	public static List<Integer> createEmptyArrayListOfSize(int size) {
		List<Integer> emptyArray = new ArrayList<>();
		for (int i = 0; i < size; i++) {
			emptyArray.add(0);
		}
		return emptyArray;
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
		double S = this.getVertexHistogram().get(patternTreeNode.getPattern().getCenterVertexType());
		double patternSupport = TgfdDiscovery.calculatePatternSupport(entityURIs, S, this.getT());
		this.patternSupportsListForThisSnapshot.add(patternSupport);
		patternTreeNode.setPatternSupport(patternSupport);

		return matchesPerTimestamps;
	}

	// TODO: Merge with other extractMatch method?
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

	protected int extractMatches(Iterator<GraphMapping<Vertex, RelationshipEdge>> iterator, ArrayList<HashSet<ConstantLiteral>> matches, PatternTreeNode patternTreeNode, Map<String, List<Integer>> entityURIs, int timestamp) {
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

	public List<Set<Set<ConstantLiteral>>> getMatchesUsingChangeFiles3(PatternTreeNode patternTreeNode) {
		LocalizedVF2Matching localizedVF2Matching;
		if (this.isFastMatching())
			localizedVF2Matching = new FastMatching(patternTreeNode, this.getT(), this.isOnlyInterestingTGFDs(), this.getVertexTypesToActiveAttributesMap(), this.reUseMatches());
		else
			localizedVF2Matching = new LocalizedVF2Matching(patternTreeNode, this.getT(), this.isOnlyInterestingTGFDs(), this.getVertexTypesToActiveAttributesMap(), this.reUseMatches());

		GraphLoader graph = loadFirstSnapshot();
		localizedVF2Matching.findMatchesInSnapshot(graph, 0);
		for (int t = 1; t < this.getT(); t++) {
			updateGraphUsingChangefile(graph, t);
			localizedVF2Matching.findMatchesInSnapshot(graph, t);
		}
		Map<String, List<Integer>> entityURIs = localizedVF2Matching.getEntityURIs();
		if (this.reUseMatches())
			patternTreeNode.setEntityURIs(entityURIs);

		System.out.println("-------------------------------------");
		System.out.println("Number of entity URIs found: "+entityURIs.size());
		localizedVF2Matching.printEntityURIs();

		double S = this.getVertexHistogram().get(patternTreeNode.getPattern().getCenterVertexType());
		double patternSupport = TgfdDiscovery.calculatePatternSupport(entityURIs, S, this.getT());
		this.patternSupportsListForThisSnapshot.add(patternSupport);
		patternTreeNode.setPatternSupport(patternSupport);

		return localizedVF2Matching.getMatchesPerTimestamp();
	}

	protected void updateGraphUsingChangefile(GraphLoader graph, int t) {
		System.out.println("-----------Snapshot (" + (t + 1) + ")-----------");
		String changeFilePath = "changes_t" + t + "_t" + (t + 1) + "_" + this.getGraphSize() + ".json";
		JSONArray jsonArray = this.isStoreInMemory() ? this.getChangeFilesMap().get(changeFilePath) : readJsonArrayFromFile(changeFilePath);
		ChangeLoader changeLoader = new ChangeLoader(jsonArray, true);
		IncUpdates incUpdatesOnDBpedia = new IncUpdates(graph.getGraph(), new ArrayList<>());
		sortChanges(changeLoader.getAllChanges());
		incUpdatesOnDBpedia.updateEntireGraph(changeLoader.getAllChanges());
	}

	public List<Set<Set<ConstantLiteral>>> getMatchesUsingChangeFiles(PatternTreeNode patternTreeNode) {
		// TODO: Should we use changefiles based on freq types??

//		List<Set<Set<ConstantLiteral>>> matchesPerTimestamps = new ArrayList<>();

//		patternTreeNode.getPattern().setDiameter(this.getCurrentVSpawnLevel());

		TGFD dummyTgfd = new TGFD();
		if (this.getCurrentVSpawnLevel() == 0)
			dummyTgfd.setName(patternTreeNode.getGraph().vertexSet().toString());
		else
			dummyTgfd.setName(patternTreeNode.getAllEdgeStrings().toString());

		dummyTgfd.setPattern(patternTreeNode.getPattern());

		System.out.println("-----------Snapshot (1)-----------");
		List<TGFD> tgfds = Collections.singletonList(dummyTgfd);
		int numberOfMatchesFound = 0;

//		GraphLoader graph = loadFirstSnapshot();

		// Now, we need to find the matches for each snapshot.
		// Finding the matches...
//		Map<String, List<Integer>> entityURIs = new HashMap<>();

//		for (TGFD tgfd : tgfds) {
//			System.out.println("\n###########" + tgfd.getName() + "###########");

		//Retrieving and storing the matches of each timestamp.
		final long matchingStartTime = System.currentTimeMillis();
//		if (this.getCurrentVSpawnLevel() < 1) {
//			if (patternTreeNode.getGraph().vertexSet().size() != 1)
//				throw new IllegalArgumentException("For vSpawn level 0, patternTreeNode.getGraph().vertexSet().size() != 1");
//			String patternVertexType = patternTreeNode.getGraph().vertexSet().iterator().next().getTypes().iterator().next();
//			this.findMatchesUsingCenterVerticesForVSpawnInit(Collections.singletonList(graph), patternVertexType, patternTreeNode, matchesPerTimestamps, null, entityURIs);
//		} else {
//			this.extractMatchesAcrossSnapshots(Collections.singletonList(graph), patternTreeNode, matchesPerTimestamps, entityURIs);
//		}
		LocalizedVF2Matching localizedVF2Matching;
		if (this.isFastMatching())
			localizedVF2Matching = new FastMatching(patternTreeNode, this.getT(), this.isOnlyInterestingTGFDs(), this.getVertexTypesToActiveAttributesMap(), this.reUseMatches());
		else
			localizedVF2Matching = new LocalizedVF2Matching(patternTreeNode, this.getT(), this.isOnlyInterestingTGFDs(), this.getVertexTypesToActiveAttributesMap(), this.reUseMatches());

		GraphLoader graph = loadFirstSnapshot();
		localizedVF2Matching.findMatchesInSnapshot(graph, 0);
		List<Set<Set<ConstantLiteral>>> matchesPerTimestamps = localizedVF2Matching.getMatchesPerTimestamp();
		Map<String, List<Integer>> entityURIs = localizedVF2Matching.getEntityURIs();
		final long totalMatchingTime = System.currentTimeMillis() - matchingStartTime;
		printWithTime("Snapshot 1 matching", totalMatchingTime);
		this.addToTotalMatchingTime(totalMatchingTime);
		numberOfMatchesFound += matchesPerTimestamps.get(0).size();
//		}

		//Load the change files
		for (int i = 0; i < this.getT()-1; i++) {
			System.out.println("-----------Snapshot (" + (i+2) + ")-----------");

			final long loadChangefileStartTime = System.currentTimeMillis();
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
			ChangeLoader changeLoader = new ChangeLoader(changesJsonArray, this.getCurrentVSpawnLevel() != 0);
			HashMap<Integer,HashSet<Change>> newChanges = changeLoader.getAllGroupedChanges();
			System.out.println("Total number of changes in changefile: " + newChanges.size());

			List<Entry<Integer, HashSet<Change>>> sortedChanges = getSortedChanges(newChanges);
			List<List<Entry<Integer,HashSet<Change>>>> changefiles = new ArrayList<>();
			changefiles.add(sortedChanges);

			long totalLoadChangefileTime = System.currentTimeMillis() - loadChangefileStartTime;
			printWithTime("Load changes for Snapshot (" + (i+2) + ")", totalLoadChangefileTime);
			// TODO: Should we add this time to some tally?

			System.out.println("Total number of changefiles: " + changefiles.size());

			// Now, we need to find the matches for each snapshot.
			// Finding the matches...

			final long graphUpdateAndMatchTime = System.currentTimeMillis();
			System.out.println("Updating the graph...");
			// TODO: Do we need to update the subgraphWithinDiameter method used in IncUpdates?
			IncUpdates incUpdatesOnDBpedia = new IncUpdates(graph.getGraph(), tgfds);
			if (this.currentVSpawnLevel > 0) {
				incUpdatesOnDBpedia.AddNewVertices(changeLoader.getAllChanges());
				System.out.println("Added new vertices.");
			}

			HashMap<String, TGFD> tgfdsByName = new HashMap<>();
			for (TGFD tgfd : tgfds) {
				tgfdsByName.put(tgfd.getName(), tgfd);
			}
			Map<String, Set<ConstantLiteral>> newMatches = new HashMap<>();
			Map<String, Set<ConstantLiteral>> removedMatches = new HashMap<>();
			int numOfNewMatchesFoundInSnapshot = 0;
			for (int changefileIndex = 0; changefileIndex < changefiles.size(); changefileIndex++) {
				List<Entry<Integer, HashSet<Change>>> changesInChangefile = changefiles.get(changefileIndex);
				for (int changeIndex = 0; changeIndex < changesInChangefile.size(); changeIndex++) {
					Entry<Integer, HashSet<Change>> groupedChangeEntry = changesInChangefile.get(changeIndex);
					HashMap<String, IncrementalChange> incrementalChangeHashMap = incUpdatesOnDBpedia.updateGraphByGroupOfChanges(groupedChangeEntry.getValue(), tgfdsByName, this.getCurrentVSpawnLevel() == 0);
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
					if (changeIndex % 100000 == 0) System.out.println("Processed changes "+changeIndex+"/"+changesInChangefile.size());
				}
				System.out.println("Processed changefile "+changefileIndex+"/"+changefiles.size());
			}
			System.out.println("Number of new matches found: " + numOfNewMatchesFoundInSnapshot);
			System.out.println("Number of new matches found that contain active attributes: " + newMatches.size());
			System.out.println("Number of removed matched: " + removedMatches.size());

			int processedMatches = 0;
			matchesPerTimestamps.add(new HashSet<>());
			for (Set<ConstantLiteral> newMatch: newMatches.values()) {
				for (ConstantLiteral l: newMatch) {
					if (l.getVertexType().equals(patternTreeNode.getPattern().getCenterVertexType())
							&& l.getAttrName().equals("uri")) {
						String entityURI = l.getAttrValue();
						entityURIs.putIfAbsent(entityURI, createEmptyArrayListOfSize(this.getNumOfSnapshots()));
						entityURIs.get(entityURI).set(i+1, entityURIs.get(entityURI).get(i+1)+1);
						break;
					}
				}
				matchesPerTimestamps.get(i+1).add(newMatch);
				processedMatches++;
				if (processedMatches % 100000 == 0) System.out.println("Processed 100000 matches");
			}
			System.out.println("Processed "+ (processedMatches % 100000) + " matches");

			int numOfOldMatchesFoundInSnapshot = 0;
			processedMatches = 0;
			Set<Set<ConstantLiteral>> removedMatchesSet = new HashSet<>(removedMatches.values());
			Set<Set<ConstantLiteral>> newMatchesSet = new HashSet<>(newMatches.values());
			for (Set<ConstantLiteral> previousMatch : matchesPerTimestamps.get(i)) {
				processedMatches++; if (processedMatches % 100000 == 0) System.out.println("Processed 100000 matches");
				String centerVertexType = patternTreeNode.getPattern().getCenterVertexType();
				String entityURI = null;
				for (ConstantLiteral l: previousMatch) {
					if (l.getVertexType().equals(centerVertexType) && l.getAttrName().equals("uri")) {
						entityURI = l.getAttrValue();
						break;
					}
				}
				if (removedMatchesSet.contains(previousMatch))
					continue;
				if (newMatchesSet.contains(previousMatch))
					continue;
				if (entityURI != null) {
					entityURIs.putIfAbsent(entityURI, createEmptyArrayListOfSize(this.getNumOfSnapshots()));
					entityURIs.get(entityURI).set(i + 1, entityURIs.get(entityURI).get(i + 1) + 1);
				}
				matchesPerTimestamps.get(i+1).add(previousMatch);
				numOfOldMatchesFoundInSnapshot++;
			}
			System.out.println("Processed "+ (processedMatches % 100000) + " matches");

			System.out.println("Number of valid old matches that are not new or removed: " + numOfOldMatchesFoundInSnapshot);
			System.out.println("Total number of matches with active attributes found in this snapshot: " + matchesPerTimestamps.get(i+1).size());

			numberOfMatchesFound += matchesPerTimestamps.get(i+1).size();

			if (this.currentVSpawnLevel > 0)
				incUpdatesOnDBpedia.deleteVertices(changeLoader.getAllChanges());

			final long finalGraphUpdateAndMatchTime = System.currentTimeMillis() - graphUpdateAndMatchTime;
			printWithTime("Update graph and retrieve matches", finalGraphUpdateAndMatchTime);
			this.addToTotalMatchingTime(finalGraphUpdateAndMatchTime);
		}

		System.out.println("-------------------------------------");
		System.out.println("Number of entity URIs found: "+entityURIs.size());
		for (Map.Entry<String, List<Integer>> entry: entityURIs.entrySet()) {
			System.out.println(entry);
		}
		System.out.println("Total number of matches found in all snapshots: " + numberOfMatchesFound);
		double S = this.getVertexHistogram().get(patternTreeNode.getPattern().getCenterVertexType());
		double patternSupport = TgfdDiscovery.calculatePatternSupport(entityURIs, S, this.getT());
		this.patternSupportsListForThisSnapshot.add(patternSupport);
		patternTreeNode.setPatternSupport(patternSupport);

		return matchesPerTimestamps;
	}

	public static void sortChanges(List<Change> changes) {
		System.out.println("Number of changes: "+changes.size());
		HashMap<ChangeType, Integer> map = new HashMap<>();
		map.put(ChangeType.deleteAttr, 2);
		map.put(ChangeType.insertAttr, 2);
		map.put(ChangeType.changeAttr, 2);
		map.put(ChangeType.deleteEdge, 3);
		map.put(ChangeType.insertEdge, 3);
		map.put(ChangeType.changeType, 1);
		map.put(ChangeType.deleteVertex, 0);
		map.put(ChangeType.insertVertex, 0);
		changes.sort(new Comparator<Change>() {
			@Override
			public int compare(Change o1, Change o2) {
				return map.get(o1.getTypeOfChange()).compareTo(map.get(o2.getTypeOfChange()));
			}
		});
		System.out.println("Sorted changes.");
	}

	@NotNull
	private static List<Entry<Integer, HashSet<Change>>> getSortedChanges(HashMap<Integer, HashSet<Change>> newChanges) {
		List<Entry<Integer,HashSet<Change>>> sortedChanges = new ArrayList<>(newChanges.entrySet());
		HashMap<ChangeType, Integer> map = new HashMap<>();
		map.put(ChangeType.deleteAttr, 2);
		map.put(ChangeType.insertAttr, 4);
		map.put(ChangeType.changeAttr, 4);
		map.put(ChangeType.deleteEdge, 0);
		map.put(ChangeType.insertEdge, 5);
		map.put(ChangeType.changeType, 3);
		map.put(ChangeType.deleteVertex, 1);
		map.put(ChangeType.insertVertex, 1);
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
		return sortedChanges;
	}

	@NotNull
	protected GraphLoader loadFirstSnapshot() {
		final long startTime = System.currentTimeMillis();
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
//		Config.optimizedLoadingBasedOnTGFD = true; // TODO: Does enabling optimized loading cause problems with TypeChange?
		if (this.getLoader().equals("dbpedia")) {
			if (this.getFirstSnapshotTypeModel() == null || this.getFirstSnapshotDataModel() == null)
				throw new NullPointerException("this.getFirstSnapshotTypeModel() == null || this.getFirstSnapshotDataModel() == null");
			graph = new DBPediaLoader(new ArrayList<>(), Collections.singletonList(this.getFirstSnapshotTypeModel()), Collections.singletonList(this.getFirstSnapshotDataModel()));
		} else if (this.getLoader().equals("synthetic")) {
			graph = new SyntheticLoader(new ArrayList<>(), this.getTimestampToFilesMap().get(0).getValue());
		} else {
			if (this.getFirstSnapshotDataModel() == null)
				throw new NullPointerException("this.getFirstSnapshotDataModel() == null");
			graph = new IMDBLoader(new ArrayList<>(), Collections.singletonList(this.getFirstSnapshotDataModel()));
		}
//		Config.optimizedLoadingBasedOnTGFD = false;

		if (this.isDissolveSuperVerticesBasedOnCount())
			this.dissolveSuperVerticesBasedOnCount(graph);

		printWithTime("Load graph (1)", System.currentTimeMillis()- startTime);
		return graph;
	}

	public static void printWithTime(String message, long runTimeInMS)
	{
		System.out.println(message + " time: " + runTimeInMS + "(ms) ** " +
				TimeUnit.MILLISECONDS.toSeconds(runTimeInMS) + "(sec) ** " +
				TimeUnit.MILLISECONDS.toMinutes(runTimeInMS) +  "(min)");
	}
}

