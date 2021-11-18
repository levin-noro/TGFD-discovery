package TgfdDiscovery;

import IncrementalRunner.IncUpdates;
import IncrementalRunner.IncrementalChange;
import Infra.*;
import VF2Runner.VF2SubgraphIsomorphism;
import changeExploration.Change;
import changeExploration.ChangeLoader;
import graphLoader.CitationLoader;
import graphLoader.DBPediaLoader;
import graphLoader.GraphLoader;
import org.apache.commons.cli.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.jetbrains.annotations.NotNull;
import org.jgrapht.Graph;
import org.jgrapht.GraphMapping;
import org.jgrapht.alg.isomorphism.VF2AbstractIsomorphismInspector;
import org.json.simple.JSONArray;
import org.json.simple.parser.JSONParser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.PrintStream;
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
import java.util.stream.Collectors;

public class TgfdDiscovery {
	public static final int DEFAULT_NUM_OF_SNAPSHOTS = 3;
	private int numOfSnapshots;
	public static final double DEFAULT_PATTERN_SUPPORT_THRESHOLD = 0.001;
	public static final int DEFAULT_GAMMA = 20;
	public static final int DEFAULT_K = 3;
	public static final double DEFAULT_THETA = 0.5;
	private final boolean dontSortHistogram;
	private final boolean useSubgraph;
	private final boolean generatek0Tgfds;
    private final boolean skipK1;
    private Integer NUM_OF_EDGES_IN_GRAPH;
	public int NUM_OF_VERTICES_IN_GRAPH;
	public Map<String, HashSet<String>> vertexTypesAttributes; // freq attributes come from here
	public PatternTree patternTree;
	public boolean noMinimalityPruning;
	public Long graphSize = null;
	private final boolean interestingTGFDs;
	private final int k;
	private final double theta;
	private final int gamma;
	private final double edgeSupportThreshold;
	private HashSet<String> activeAttributesSet;
	private int previousLevelNodeIndex = 0;
	private int candidateEdgeIndex = 0;
	private int currentVSpawnLevel = 0;
	private final ArrayList<ArrayList<TGFD>> tgfds;
	private final long startTime;
	private final ArrayList<Long> kRuntimes = new ArrayList<>();
	private String timeAndDateStamp = null;
	private boolean isKExperiment = false;
	private boolean useChangeFile;
	private final ArrayList<Model> models = new ArrayList<>();
	private final HashMap<String, org.json.simple.JSONArray> changeFilesMap = new HashMap<>();
	private List<Entry<String, Integer>> sortedVertexHistogram; // freq nodes come from here
	private List<Entry<String, Integer>> sortedEdgeHistogram; // freq edges come from here
	private final HashMap<String, Integer> vertexHistogram = new HashMap<>();
	private boolean noSupportPruning;
	private ArrayList<Float> patternSupportsList = new ArrayList<>();
	private ArrayList<Float> constantTgfdSupportsList = new ArrayList<>();
	private ArrayList<Float> generalTgfdSupportsList = new ArrayList<>();
	private final ArrayList<Float> vertexSupportsList = new ArrayList<>();
	private final ArrayList<Float> edgeSupportsList = new ArrayList<>();
	private long totalVisitedPathCheckingTime = 0;
	private long totalMatchingTime = 0;
	private long totalSupersetPathCheckingTime = 0;
	private long totalFindEntitiesTime = 0;
	private long totalVSpawnTime = 0;
	private long totalDiscoverConstantTGFDsTime = 0;
	private long totalDiscoverGeneralTGFDTime = 0;
	private String experimentName;

	public TgfdDiscovery(int numOfSnapshots) {
		this.startTime = System.currentTimeMillis();
		this.k = DEFAULT_K;
		this.theta = DEFAULT_THETA;
		this.gamma = DEFAULT_GAMMA;
		this.setNumOfSnapshots(numOfSnapshots);
		this.edgeSupportThreshold = DEFAULT_PATTERN_SUPPORT_THRESHOLD;
		this.noMinimalityPruning = false;
		this.interestingTGFDs = false;
		this.setUseChangeFile(false);
		this.setNoSupportPruning(false);
		this.dontSortHistogram = false;
		this.useSubgraph = false;
		this.generatek0Tgfds = false;
        this.skipK1 = true;

		System.out.println("Running experiment for |G|="+this.graphSize+", k="+ this.getK() +", theta="+ this.getTheta() +", gamma"+this.gamma+", patternSupport="+this.edgeSupportThreshold +", interesting="+this.interestingTGFDs+", optimized="+!this.noMinimalityPruning);

		this.tgfds = new ArrayList<>();
		for (int vSpawnLevel = 0; vSpawnLevel <= this.getK(); vSpawnLevel++) {
			getTgfds().add(new ArrayList<>());
		}
	}

	public TgfdDiscovery(String experimentName, int k, double theta, int gamma, Long graphSize, double patternSupport, boolean noMinimalityPruning, boolean interestingTGFDsOnly, boolean useChangeFile, boolean noSupportPruning, boolean dontSortHistogram, boolean useSubgraph, boolean generatek0Tgfds, boolean skipK1) {
		this.startTime = System.currentTimeMillis();
		this.experimentName = experimentName;
		this.k = k;
		this.theta = theta;
		this.gamma = gamma;
		this.graphSize = graphSize;
		this.edgeSupportThreshold = patternSupport;
		this.noMinimalityPruning = noMinimalityPruning;
		this.interestingTGFDs = interestingTGFDsOnly;
		this.setUseChangeFile(useChangeFile);
		this.setNoSupportPruning(noSupportPruning);
		this.dontSortHistogram = dontSortHistogram;
		this.useSubgraph = useSubgraph;
		this.generatek0Tgfds = generatek0Tgfds;
        this.skipK1 = skipK1;

		System.out.println("Running experiment for |G|="+this.graphSize
				+", k="+ this.getK()
				+", theta="+ this.getTheta()
				+", gamma"+this.gamma
				+", edgeSupport=" +this.edgeSupportThreshold
				+", interesting="+this.interestingTGFDs
				+", optimized="+!this.noMinimalityPruning
				+", noSupportPruning="+ this.hasNoSupportPruning());

		this.tgfds = new ArrayList<>();
		for (int vSpawnLevel = 0; vSpawnLevel <= this.getK(); vSpawnLevel++) {
			getTgfds().add(new ArrayList<>());
		}
	}

	public static CommandLine parseArgs(String[] args) {
		Options options = new Options();
		options.addOption("name", true, "output files will be given the specified name");
		options.addOption("console", false, "print to console");
		options.addOption("noMinimalityPruning", false, "run algorithm without minimality pruning");
		options.addOption("noSupportPruning", false, "run algorithm without support pruning");
		options.addOption("dontSortHistogram", false, "run algorithm without sorting histograms");
		options.addOption("interesting", false, "run algorithm and only consider interesting TGFDs");
		options.addOption("g", true, "run experiment on a specific graph size");
		options.addOption("k", true, "run experiment for k iterations");
		options.addOption("a", true, "run experiment for specified active attribute set size");
		options.addOption("theta", true, "run experiment using a specific support threshold");
		options.addOption("K", false, "run experiment for k = 1 to 5");
		options.addOption("p", true, "run experiment using specific pattern support threshold");
		options.addOption("changefile", false, "run experiment using changefiles instead of snapshots");
		options.addOption("subgraph", false, "run experiment using incremental subgraphs instead of snapshots");
		options.addOption("k0", false, "run experiment and generate tgfds for single-node patterns");
		options.addOption("dataset", true, "run experiment using specified dataset");
		options.addOption("path", true, "path to dataset");
		options.addOption("skipK1", false, "run experiment and generate tgfds for k > 1");

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

	public void initialize(ArrayList<GraphLoader> graphs) {
		vSpawnInit(graphs);
		if (this.isGeneratek0Tgfds()) {
			this.printTgfdsToFile(this.experimentName, this.getTgfds().get(this.getCurrentVSpawnLevel()));
		}
		this.kRuntimes.add(System.currentTimeMillis() - this.startTime);
		this.patternTree.addLevel();
		this.setCurrentVSpawnLevel(this.getCurrentVSpawnLevel() + 1);
	}

	@Override
	public String toString() {
		return (this.graphSize == null ? "" : "-G"+this.graphSize) +
				"-k" + this.getCurrentVSpawnLevel() +
				"-theta" + this.getTheta() +
				"-a" + this.gamma +
				"-eSupp" + this.edgeSupportThreshold +
				(this.isUseChangeFile() ? "-changefile" : "") +
				(this.isUseSubgraph() ? "-subgraph" : "") +
				(this.interestingTGFDs ? "-interesting" : "") +
				(this.noMinimalityPruning ? "-noMinimalityPruning" : "") +
				(this.hasNoSupportPruning() ? "-noSupportPruning" : "") +
				(this.timeAndDateStamp == null ? "" : ("-"+this.timeAndDateStamp));
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

	public void printExperimentRuntimestoFile(String experimentName, ArrayList<Long> runtimes) {
		try {
			PrintStream printStream = new PrintStream(experimentName + "-experiments-runtimes-" + this + ".txt");
			for (int i  = 0; i < runtimes.size(); i++) {
				printStream.print("k = " + i);
				printStream.println(", execution time = " + runtimes.get(i));
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {
		CommandLine cmd = TgfdDiscovery.parseArgs(args);

		String dataset;
		if (!cmd.hasOption("dataset")) {
			System.out.println("No dataset is specified.");
			return;
		} else {
			dataset = cmd.getOptionValue("dataset");
		}

		String timeAndDateStamp = ZonedDateTime.now(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern("uuuu.MM.dd.HH.mm.ss"));

		String experimentName = null;
		if (cmd.hasOption("name")) {
			experimentName = cmd.getOptionValue("name");
		} else  {
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
		boolean useChangeFile = cmd.hasOption("changefile");
		boolean useSubgraph = cmd.hasOption("subgraph");
		boolean generatek0Tgfds = cmd.hasOption("k0");
		boolean skipK1 = cmd.hasOption("skipK1");

		Long graphSize = null;
		if (cmd.getOptionValue("g") != null) {
			graphSize = Long.parseLong(cmd.getOptionValue("g"));
		}
		int gamma = cmd.getOptionValue("a") == null ? TgfdDiscovery.DEFAULT_GAMMA : Integer.parseInt(cmd.getOptionValue("a"));
		double theta = cmd.getOptionValue("theta") == null ? TgfdDiscovery.DEFAULT_THETA : Double.parseDouble(cmd.getOptionValue("theta"));
		int k = cmd.getOptionValue("k") == null ? TgfdDiscovery.DEFAULT_K : Integer.parseInt(cmd.getOptionValue("k"));
		double patternSupportThreshold = cmd.getOptionValue("p") == null ? TgfdDiscovery.DEFAULT_PATTERN_SUPPORT_THRESHOLD : Double.parseDouble(cmd.getOptionValue("p"));

		TgfdDiscovery tgfdDiscovery = new TgfdDiscovery(experimentName, k, theta, gamma, graphSize, patternSupportThreshold, noMinimalityPruning, interestingTGFDs, useChangeFile, noSupportPruning, dontSortHistogram, useSubgraph, generatek0Tgfds, skipK1);

		ArrayList<GraphLoader> graphs;
		if (dataset.equals("dbpedia")) {
//			graphs = tgfdDiscovery.loadDBpediaSnapshots(graphSize);
			if (cmd.hasOption("path")) {
				graphs = tgfdDiscovery.loadDBpediaSnapshots2(cmd.getOptionValue("path"));
			} else {
				System.out.println("Invalid path dataset files.");
				return;
			}
			final long histogramTime = System.currentTimeMillis();
			tgfdDiscovery.histogram(graphs);
			TgfdDiscovery.printWithTime("histogramTime", (System.currentTimeMillis() - histogramTime));
		} else if (dataset.equals("citation")) {
			graphs = tgfdDiscovery.loadCitationSnapshots(Arrays.asList("dblp_papers_v11.txt","dblp.v12.json","dblpv13.json"));
			final long histogramTime = System.currentTimeMillis();
			tgfdDiscovery.histogram(graphs);
			TgfdDiscovery.printWithTime("histogramTime", (System.currentTimeMillis() - histogramTime));
		} else {
			System.out.println("No dataset is specified.");
			return;
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
			vSpawnTime = System.currentTimeMillis()-vSpawnTime;
			TgfdDiscovery.printWithTime("vSpawn", vSpawnTime);
			tgfdDiscovery.addToTotalVSpawnTime(vSpawnTime);
			if (tgfdDiscovery.getCurrentVSpawnLevel() > tgfdDiscovery.getK()) break;
			ArrayList<ArrayList<HashSet<ConstantLiteral>>> matches = new ArrayList<>();
			for (int timestamp = 0; timestamp < tgfdDiscovery.getNumOfSnapshots(); timestamp++) {
				matches.add(new ArrayList<>());
			}
			long matchingTime = System.currentTimeMillis();

			assert patternTreeNode != null;
			if (useSubgraph) {
				tgfdDiscovery.getMatchesUsingCenterVertices(graphs, patternTreeNode, matches);
				matchingTime = System.currentTimeMillis() - matchingTime;
				TgfdDiscovery.printWithTime("getMatchesUsingCenterVertices", (matchingTime));
				tgfdDiscovery.addToTotalMatchingTime(matchingTime);
			} else if (useChangeFile) {
				tgfdDiscovery.getMatchesForPattern2(graphs, patternTreeNode, matches);
				TgfdDiscovery.printWithTime("getMatchesForPattern2", (System.currentTimeMillis() - matchingTime));
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
	}

	public void printTimeStatistics() {
		System.out.println("----------------Total Time Statistics-----------------");
		System.out.println("totalVspawnTime time: " + this.getTotalVSpawnTime() + "(ms) ** " +
				TimeUnit.MILLISECONDS.toSeconds(this.getTotalVSpawnTime()) +  "(sec)" +
				TimeUnit.MILLISECONDS.toMinutes(this.getTotalVSpawnTime()) +  "(min)");
		System.out.println("totalMatchingTime time: " + this.getTotalMatchingTime() + "(ms) ** " +
				TimeUnit.MILLISECONDS.toSeconds(this.getTotalMatchingTime()) +  "(sec)" +
				TimeUnit.MILLISECONDS.toMinutes(this.getTotalMatchingTime()) +  "(min)");
		System.out.println("totalVisitedPathCheckingTime time: " + this.totalVisitedPathCheckingTime + "(ms) ** " +
				TimeUnit.MILLISECONDS.toSeconds(this.totalVisitedPathCheckingTime) +  "(sec)" +
				TimeUnit.MILLISECONDS.toMinutes(this.totalVisitedPathCheckingTime) +  "(min)");
		System.out.println("totalSupersetPathCheckingTime time: " + this.totalSupersetPathCheckingTime + "(ms) ** " +
				TimeUnit.MILLISECONDS.toSeconds(this.totalSupersetPathCheckingTime) +  "(sec)" +
				TimeUnit.MILLISECONDS.toMinutes(this.totalSupersetPathCheckingTime) +  "(min)");
		System.out.println("totalFindEntitiesTime time: " + this.totalFindEntitiesTime + "(ms) ** " +
				TimeUnit.MILLISECONDS.toSeconds(this.totalFindEntitiesTime) +  "(sec)" +
				TimeUnit.MILLISECONDS.toMinutes(this.totalFindEntitiesTime) +  "(min)");
		System.out.println("totalDiscoverConstantTGFDsTime time: " + this.totalDiscoverConstantTGFDsTime + "(ms) ** " +
				TimeUnit.MILLISECONDS.toSeconds(this.totalDiscoverConstantTGFDsTime) +  "(sec)" +
				TimeUnit.MILLISECONDS.toMinutes(this.totalDiscoverConstantTGFDsTime) +  "(min)");
		System.out.println("totalDiscoverGeneralTGFDTime time: " + this.totalDiscoverGeneralTGFDTime + "(ms) ** " +
				TimeUnit.MILLISECONDS.toSeconds(this.totalDiscoverGeneralTGFDTime) +  "(sec)" +
				TimeUnit.MILLISECONDS.toMinutes(this.totalDiscoverGeneralTGFDTime) +  "(min)");
	}

	private void getMatchesUsingCenterVerticesForK1 (ArrayList<GraphLoader> graphs, PatternTreeNode patternTreeNode, ArrayList<ArrayList<HashSet<ConstantLiteral>>> matchesPerTimestamps) {
		Set<String> edgeLabels = patternTreeNode.getGraph().edgeSet().stream().map(RelationshipEdge::getLabel).collect(Collectors.toSet());
		List<Set<String>> vertexTypesSets = patternTreeNode.getGraph().vertexSet().stream().map(Vertex::getTypes).collect(Collectors.toList());
		Set<String> vertexTypes = new HashSet<>();
		for (Set<String> vertexTypesSet: vertexTypesSets) {
			vertexTypes.addAll(vertexTypesSet);
		}
		String sourceType = patternTreeNode.getGraph().edgeSet().iterator().next().getSource().getTypes().iterator().next();
		String targetType = patternTreeNode.getGraph().edgeSet().iterator().next().getTarget().getTypes().iterator().next();
		HashSet<String> entityURIs = new HashSet<>();
		String centerVertexType = patternTreeNode.getPattern().getCenterVertexType();
		System.out.println("Center vertex type: "+centerVertexType);
		ArrayList<ArrayList<DataVertex>> matchesOfCenterVertex;
		if (patternTreeNode.getCenterVertexParent() != null) {
			matchesOfCenterVertex = patternTreeNode.getCenterVertexParent().getMatchesOfCenterVertices();
		} else {
			matchesOfCenterVertex = extractMatchesForCenterVertex(graphs, centerVertexType);
		}
		ArrayList<ArrayList<DataVertex>> newMatchesOfCenterVertex = new ArrayList<>();
		int diameter = this.getCurrentVSpawnLevel();
		System.out.println("Searching for patterns of diameter: " + diameter);
		for (int year = 0; year < this.getNumOfSnapshots(); year++) {
			HashSet<HashSet<ConstantLiteral>> matchesSet = new HashSet<>();
			ArrayList<DataVertex> centerVertexMatchesInThisTimestamp = matchesOfCenterVertex.get(year);
			newMatchesOfCenterVertex.add(new ArrayList<>());
			int numOfMatchesInTimestamp = 0;
			for (DataVertex dataVertex : centerVertexMatchesInThisTimestamp) {
				ArrayList<HashSet<ConstantLiteral>> matches = new ArrayList<>();
				Set<RelationshipEdge> edgeSet;
				if (centerVertexType.equals(sourceType)) {
					edgeSet = graphs.get(year).getGraph().getGraph().outgoingEdgesOf(dataVertex).stream().filter(e -> edgeLabels.contains(e.getLabel()) && e.getTarget().getTypes().contains(targetType)).collect(Collectors.toSet());
				} else {
					edgeSet = graphs.get(year).getGraph().getGraph().incomingEdgesOf(dataVertex).stream().filter(e -> edgeLabels.contains(e.getLabel()) && e.getSource().getTypes().contains(sourceType)).collect(Collectors.toSet());
				}
				int numOfMatchesForCenterVertex = extractMatches(edgeSet, matches, patternTreeNode, entityURIs);
				if (numOfMatchesForCenterVertex > 0) { // equivalent to results.isomorphismExists()
					matchesSet.addAll(matches);
					newMatchesOfCenterVertex.get(year).add(dataVertex);
				}
				numOfMatchesInTimestamp += numOfMatchesForCenterVertex;
			}
			System.out.println("Number of matches found: " + numOfMatchesInTimestamp);
			System.out.println("Number of matches found that contain active attributes: " + matchesSet.size());
			matchesPerTimestamps.get(year).addAll(matchesSet);
		}
		patternTreeNode.setMatchesOfCenterVertices(newMatchesOfCenterVertex);
		int numberOfMatchesFound = 0;
		for (ArrayList<HashSet<ConstantLiteral>> matchesInOneTimestamp : matchesPerTimestamps) {
			numberOfMatchesFound += matchesInOneTimestamp.size();
		}
		System.out.println("Total number of matches found across all snapshots:" + numberOfMatchesFound);
		calculatePatternSupport(entityURIs.size(), patternTreeNode);
	}

	public void getMatchesUsingCenterVertices(ArrayList<GraphLoader> graphs, PatternTreeNode patternTreeNode, ArrayList<ArrayList<HashSet<ConstantLiteral>>> matchesPerTimestamps) {
		if (this.getCurrentVSpawnLevel() == 1) {
			getMatchesUsingCenterVerticesForK1(graphs, patternTreeNode, matchesPerTimestamps);
			return;
		}
		Set<String> edgeLabels = patternTreeNode.getGraph().edgeSet().stream().map(RelationshipEdge::getLabel).collect(Collectors.toSet());
		List<Set<String>> vertexTypesSets = patternTreeNode.getGraph().vertexSet().stream().map(Vertex::getTypes).collect(Collectors.toList());
		Set<String> vertexTypes = new HashSet<>();
		for (Set<String> vertexTypesSet: vertexTypesSets) {
			vertexTypes.addAll(vertexTypesSet);
		}
		HashSet<String> entityURIs = new HashSet<>();
		String centerVertexType = patternTreeNode.getPattern().getCenterVertexType();
		System.out.println("Center vertex type: "+centerVertexType);
		ArrayList<ArrayList<DataVertex>> matchesOfCenterVertex;
		if (patternTreeNode.getCenterVertexParent() != null) {
			matchesOfCenterVertex = patternTreeNode.getCenterVertexParent().getMatchesOfCenterVertices();
		} else {
			matchesOfCenterVertex = extractMatchesForCenterVertex(graphs, centerVertexType);
		}
		ArrayList<ArrayList<DataVertex>> newMatchesOfCenterVertex = new ArrayList<>();
		// We need to use k because calculated diameter of pattern is for directed graphs,
		// whereas getSubGraphWithinDiameter requires an undirected diameter.
		int diameter = patternTreeNode.getPattern().getDiameter();
		System.out.println("Searching for patterns of diameter: " + diameter);
		for (int year = 0; year < this.getNumOfSnapshots(); year++) {
			HashSet<HashSet<ConstantLiteral>> matchesSet = new HashSet<>();
			ArrayList<DataVertex> centerVertexMatchesInThisTimestamp = matchesOfCenterVertex.get(year);
			System.out.println("Number of center vertex matches in this timestamp from previous levels: "+centerVertexMatchesInThisTimestamp.size());
			newMatchesOfCenterVertex.add(new ArrayList<>());
			int numOfMatchesInTimestamp = 0;
			for (DataVertex dataVertex: centerVertexMatchesInThisTimestamp) {
				ArrayList<HashSet<ConstantLiteral>> matches = new ArrayList<>();

				Graph<Vertex, RelationshipEdge> subgraph = graphs.get(year).getGraph().getSubGraphWithinDiameter(dataVertex, diameter, edgeLabels, vertexTypes);
				VF2AbstractIsomorphismInspector<Vertex, RelationshipEdge> results = new VF2SubgraphIsomorphism().execute2(subgraph, patternTreeNode.getPattern(), false);
				if (results.isomorphismExists()) {
					int numOfMatchesForCenterVertex = extractMatches(results.getMappings(), matches, patternTreeNode, entityURIs);
					matchesSet.addAll(matches);
					newMatchesOfCenterVertex.get(year).add(dataVertex);
					numOfMatchesInTimestamp += numOfMatchesForCenterVertex;
				}
			}
			System.out.println("Number of matches found: " + numOfMatchesInTimestamp);
			System.out.println("Number of matches found that contain active attributes: " + matchesSet.size());
			System.out.println("Number of center vertex matches in this timestamp in this level: " + newMatchesOfCenterVertex.get(year).size());
			matchesPerTimestamps.get(year).addAll(matchesSet);
		}
		patternTreeNode.setMatchesOfCenterVertices(newMatchesOfCenterVertex);
		int numberOfMatchesFound = 0;
		for (ArrayList<HashSet<ConstantLiteral>> matchesInOneTimestamp : matchesPerTimestamps) {
			numberOfMatchesFound += matchesInOneTimestamp.size();
		}
		System.out.println("Total number of matches found across all snapshots:" + numberOfMatchesFound);
		calculatePatternSupport(entityURIs.size(), patternTreeNode);
	}

	private void printSupportStatistics() {
		System.out.println("----------------Statistics for vSpawn level "+ this.getCurrentVSpawnLevel() +"-----------------");
		Collections.sort(this.vertexSupportsList);
		Collections.sort(this.edgeSupportsList);
		Collections.sort(this.patternSupportsList);
		Collections.sort(this.constantTgfdSupportsList);
		Collections.sort(this.generalTgfdSupportsList);
		float medianVertexSupport = 0;
		if (this.vertexSupportsList.size() > 0) {
			medianVertexSupport = this.vertexSupportsList.size() % 2 != 0 ? this.vertexSupportsList.get(this.vertexSupportsList.size() / 2) : ((this.vertexSupportsList.get(this.vertexSupportsList.size() / 2) + this.vertexSupportsList.get(this.vertexSupportsList.size() / 2 - 1)) / 2);
		}
		float medianEdgeSupport = 0;
		if (this.edgeSupportsList.size() > 0) {
			medianEdgeSupport = this.edgeSupportsList.size() % 2 != 0 ? this.edgeSupportsList.get(this.edgeSupportsList.size() / 2) : ((this.edgeSupportsList.get(this.edgeSupportsList.size() / 2) + this.edgeSupportsList.get(this.edgeSupportsList.size() / 2 - 1)) / 2);
		}
		float patternSupportsList = 0;
		if (this.patternSupportsList.size() > 0) {
			patternSupportsList = this.patternSupportsList.size() % 2 != 0 ? this.patternSupportsList.get(this.patternSupportsList.size() / 2) : ((this.patternSupportsList.get(this.patternSupportsList.size() / 2) + this.patternSupportsList.get(this.patternSupportsList.size() / 2 - 1)) / 2);
		}
		float constantTgfdSupportsList = 0;
		if (this.constantTgfdSupportsList.size() > 0) {
			constantTgfdSupportsList = this.constantTgfdSupportsList.size() % 2 != 0 ? this.constantTgfdSupportsList.get(this.constantTgfdSupportsList.size() / 2) : ((this.constantTgfdSupportsList.get(this.constantTgfdSupportsList.size() / 2) + this.constantTgfdSupportsList.get(this.constantTgfdSupportsList.size() / 2 - 1)) / 2);
		}
		float generalTgfdSupportsList = 0;
		if (this.generalTgfdSupportsList.size() > 0) {
			generalTgfdSupportsList = this.generalTgfdSupportsList.size() % 2 != 0 ? this.generalTgfdSupportsList.get(this.generalTgfdSupportsList.size() / 2) : ((this.generalTgfdSupportsList.get(this.generalTgfdSupportsList.size() / 2) + this.generalTgfdSupportsList.get(this.generalTgfdSupportsList.size() / 2 - 1)) / 2);
		}
		System.out.println("Median Vertex Support: " + medianVertexSupport);
		System.out.println("Median Edge Support: " + medianEdgeSupport);
		System.out.println("Median Pattern Support: " + patternSupportsList);
		System.out.println("Median Constant TGFD Support: " + constantTgfdSupportsList);
		System.out.println("Median General TGFD Support: " + generalTgfdSupportsList);
		// Reset for each level of vSpawn
		this.patternSupportsList = new ArrayList<>();
		this.constantTgfdSupportsList = new ArrayList<>();
		this.generalTgfdSupportsList = new ArrayList<>();
	}

	public void markAsKexperiment() {
		this.isKExperiment = true;
	}

	public void setExperimentDateAndTimeStamp(String timeAndDateStamp) {
		this.timeAndDateStamp = timeAndDateStamp;
	}

	public void computeVertexHistogram(ArrayList<GraphLoader> graphs) {

		System.out.println("Computing Node Histogram");

		Map<String, Integer> vertexTypesHistogram = new HashMap<>();
		Map<String, Set<String>> tempVertexAttrFreqMap = new HashMap<>();
		Map<String, String> vertexNameToTypeMap = new HashMap<>();

		for (GraphLoader graph: graphs) {
			int numOfVertices = 0;
			for (Vertex v: graph.getGraph().getGraph().vertexSet()) {
				numOfVertices++;
				for (String vertexType : v.getTypes()) {
					String vertexName = v.getAttributeValueByName("uri");
					vertexTypesHistogram.merge(vertexType, 1, Integer::sum);
					tempVertexAttrFreqMap.putIfAbsent(vertexType, new HashSet<>());
					vertexNameToTypeMap.putIfAbsent(vertexName, vertexType); // Every vertex only has one type?
				}
			}
			System.out.println("Number of vertices in graph: " + numOfVertices);
		}

		this.NUM_OF_VERTICES_IN_GRAPH = 0;
		for (Map.Entry<String, Integer> entry : vertexTypesHistogram.entrySet()) {
			this.NUM_OF_VERTICES_IN_GRAPH += entry.getValue();
		}
		System.out.println("Number of vertices across all graphs: " + this.NUM_OF_VERTICES_IN_GRAPH);
		System.out.println("Number of vertex types across all graphs: " + vertexTypesHistogram.size());

		getSortedFrequentVertexTypesHistogram(vertexTypesHistogram);

		computeAttrHistogram(graphs, vertexNameToTypeMap, tempVertexAttrFreqMap);
		computeEdgeHistogram(graphs, vertexNameToTypeMap, vertexTypesHistogram);

	}

	public void computeAttrHistogram(ArrayList<GraphLoader> graphs, Map<String, String> nodesRecord, Map<String, Set<String>> tempVertexAttrFreqMap) {
		System.out.println("Computing attributes histogram");

		Map<String, Set<String>> attrDistributionMap = new HashMap<>();

		for (GraphLoader graph: graphs) {
			int numOfAttributes = 0;
			for (Vertex v: graph.getGraph().getGraph().vertexSet()) {
				String vertexName = v.getAttributeValueByName("uri");
				if (nodesRecord.get(vertexName) != null) {
					String vertexType = nodesRecord.get(vertexName);
					for (String attrName: v.getAllAttributesNames()) {
						if (attrName.equals("uri")) continue;
						numOfAttributes++;
						if (tempVertexAttrFreqMap.containsKey(vertexType)) {
							tempVertexAttrFreqMap.get(vertexType).add(attrName);
						}
						if (!attrDistributionMap.containsKey(attrName)) {
							attrDistributionMap.put(attrName, new HashSet<>());
						} else {
							attrDistributionMap.get(attrName).add(vertexType);
						}
					}
				}
			}
			System.out.println("Number of attributes in graph: " + numOfAttributes);
		}

		ArrayList<Entry<String,Set<String>>> sortedAttrDistributionMap = new ArrayList<>(attrDistributionMap.entrySet());
		sortedAttrDistributionMap.sort((o1, o2) -> o2.getValue().size() - o1.getValue().size());
		HashSet<String> mostDistributedAttributesSet = new HashSet<>();
		for (Entry<String, Set<String>> attrNameEntry : sortedAttrDistributionMap.subList(0, Math.min(this.gamma, sortedAttrDistributionMap.size()))) {
			mostDistributedAttributesSet.add(attrNameEntry.getKey());
		}
		this.activeAttributesSet = mostDistributedAttributesSet;

		Map<String, HashSet<String>> vertexTypesAttributes = new HashMap<>();
		for (String vertexType : tempVertexAttrFreqMap.keySet()) {
			Set<String> attrNameSet = tempVertexAttrFreqMap.get(vertexType);
			vertexTypesAttributes.put(vertexType, new HashSet<>());
			for (String attrName : attrNameSet) {
				if (mostDistributedAttributesSet.contains(attrName)) { // Filters non-frequent attributes
					vertexTypesAttributes.get(vertexType).add(attrName);
				}
			}
		}

		this.vertexTypesAttributes = vertexTypesAttributes;
	}

	public void computeEdgeHistogram(ArrayList<GraphLoader> graphs, Map<String, String> nodesRecord, Map<String, Integer> vertexTypesHistogram) {
		System.out.println("Computing edges histogram");

		Map<String, Integer> edgeTypesHistogram = new HashMap<>();

		for (GraphLoader graph: graphs) {
			int numOfEdges = 0;
			for (RelationshipEdge e: graph.getGraph().getGraph().edgeSet()) {
				numOfEdges++;
				String subjectName = e.getSource().getAttributeValueByName("uri");
				String predicateName = e.getLabel();
				String objectName = e.getTarget().getAttributeValueByName("uri");
				if (nodesRecord.get(subjectName) != null && nodesRecord.get(objectName) != null) {
					String uniqueEdge = nodesRecord.get(subjectName) + " " + predicateName + " " + nodesRecord.get(objectName);
					edgeTypesHistogram.merge(uniqueEdge, 1, Integer::sum);
				}
			}
			System.out.println("Number of edges in graph: " + numOfEdges);
		}

		this.NUM_OF_EDGES_IN_GRAPH = 0;
		for (Map.Entry<String, Integer> entry : edgeTypesHistogram.entrySet()) {
			this.NUM_OF_EDGES_IN_GRAPH += entry.getValue();
		}
		System.out.println("Number of edges across all graphs: " + this.NUM_OF_EDGES_IN_GRAPH);
		System.out.println("Number of edges labels across all graphs: " + edgeTypesHistogram.size());

		this.sortedEdgeHistogram = getSortedFrequentEdgeHistogram(edgeTypesHistogram, vertexTypesHistogram);
	}

	public void getSortedFrequentVertexTypesHistogram(Map<String, Integer> vertexTypesHistogram) {
		ArrayList<Entry<String, Integer>> sortedVertexTypesHistogram = new ArrayList<>(vertexTypesHistogram.entrySet());
		if (this.isDontSortHistogram()) {
			this.sortedVertexHistogram = sortedVertexTypesHistogram;
			return;
		}
		sortedVertexTypesHistogram.sort(new Comparator<Entry<String, Integer>>() {
			@Override
			public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
				return o2.getValue() - o1.getValue();
			}
		});
		int size = 0;
		for (Entry<String, Integer> entry : sortedVertexTypesHistogram) {
			this.vertexHistogram.put(entry.getKey(),entry.getValue());
			float vertexSupport = (float) entry.getValue() / this.NUM_OF_VERTICES_IN_GRAPH;
			this.vertexSupportsList.add(vertexSupport);
			if (vertexSupport >= this.edgeSupportThreshold) {
				size++;
			} else {
				break;
			}
		}
		this.sortedVertexHistogram = sortedVertexTypesHistogram.subList(0, size);
	}

	public List<Entry<String, Integer>> getSortedFrequentEdgeHistogram(Map<String, Integer> edgeTypesHist, Map<String, Integer> vertexTypesHistogram) {
		ArrayList<Entry<String, Integer>> sortedEdgesHist = new ArrayList<>(edgeTypesHist.entrySet());
		if (this.isDontSortHistogram()) {
			for (Entry<String, Integer> entry : sortedEdgesHist) {
				String[] edgeString = entry.getKey().split(" ");
				String sourceType = edgeString[0];
				String targetType = edgeString[2];
				this.vertexHistogram.put(sourceType, vertexTypesHistogram.get(sourceType));
				this.vertexHistogram.put(targetType, vertexTypesHistogram.get(targetType));
			}
			return sortedEdgesHist;
		}
		sortedEdgesHist.sort(new Comparator<Entry<String, Integer>>() {
			@Override
			public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2) {
				return o2.getValue() - o1.getValue();
			}
		});
		int size = 0;
		for (Entry<String, Integer> entry : sortedEdgesHist) {
			float edgeSupport = (float) entry.getValue() / this.NUM_OF_EDGES_IN_GRAPH;
			this.edgeSupportsList.add(edgeSupport);
			if (edgeSupport >= this.edgeSupportThreshold) {
				String[] edgeString = entry.getKey().split(" ");
				String sourceType = edgeString[0];
				String targetType = edgeString[2];
				this.vertexHistogram.put(sourceType, vertexTypesHistogram.get(sourceType));
				this.vertexHistogram.put(targetType, vertexTypesHistogram.get(targetType));
				size++;
			} else {
				break;
			}
		}
		return sortedEdgesHist.subList(0, size);
	}

	public void histogram(ArrayList<GraphLoader> graphs) {
		computeVertexHistogram(graphs);
		printHistogram();
	}

	public void printHistogram() {

		System.out.println("Number of vertex types: " + this.sortedVertexHistogram.size());
		System.out.println("Frequent Vertices:");
		for (Entry<String, Integer> entry : this.sortedVertexHistogram) {
			String vertexType = entry.getKey();
			Set<String> attributes = this.vertexTypesAttributes.get(vertexType);
			System.out.println(vertexType + "={count=" + entry.getValue() + ", support=" + (1.0 * entry.getValue() / this.NUM_OF_VERTICES_IN_GRAPH) + ", attributes=" + attributes + "}");
		}

		System.out.println();
		System.out.println("Size of active attributes set: " + this.activeAttributesSet.size());
		System.out.println("Attributes:");
		for (String attrName : this.activeAttributesSet) {
			System.out.println(attrName);
		}
		System.out.println();
		System.out.println("Number of edge types: " + this.sortedEdgeHistogram.size());
		System.out.println("Frequent Edges:");
		for (Entry<String, Integer> entry : this.sortedEdgeHistogram) {
			System.out.println("edge=\"" + entry.getKey() + "\", count=" + entry.getValue() + ", support=" +(1.0 * entry.getValue() / this.NUM_OF_EDGES_IN_GRAPH));
		}
		System.out.println();
	}

	public static Map<Set<ConstantLiteral>, ArrayList<Entry<ConstantLiteral, List<Integer>>>> findEntities(AttributeDependency attributes, ArrayList<ArrayList<HashSet<ConstantLiteral>>> matchesPerTimestamps) {
		String yVertexType = attributes.getRhs().getVertexType();
		String yAttrName = attributes.getRhs().getAttrName();
		Set<ConstantLiteral> xAttributes = attributes.getLhs();
		Map<Set<ConstantLiteral>, Map<ConstantLiteral, List<Integer>>> entitiesWithRHSvalues = new HashMap<>();
		int t = 2015;
		for (ArrayList<HashSet<ConstantLiteral>> matchesInOneTimeStamp : matchesPerTimestamps) {
			System.out.println("---------- Attribute values in " + t + " ---------- ");
			int numOfMatches = 0;
			if (matchesInOneTimeStamp.size() > 0) {
				for(HashSet<ConstantLiteral> match : matchesInOneTimeStamp) {
					if (match.size() < attributes.size()) continue;
					Set<ConstantLiteral> entity = new HashSet<>();
					ConstantLiteral rhs = null;
					for (ConstantLiteral literalInMatch : match) {
						if (literalInMatch.getVertexType().equals(yVertexType) && literalInMatch.getAttrName().equals(yAttrName)) {
							rhs = new ConstantLiteral(literalInMatch.getVertexType(), literalInMatch.getAttrName(), literalInMatch.getAttrValue());
							continue;
						}
						for (ConstantLiteral attibute : xAttributes) {
							if (literalInMatch.getVertexType().equals(attibute.getVertexType()) && literalInMatch.getAttrName().equals(attibute.getAttrName())) {
								entity.add(new ConstantLiteral(literalInMatch.getVertexType(), literalInMatch.getAttrName(), literalInMatch.getAttrValue()));
							}
						}
					}
					if (entity.size() < xAttributes.size() || rhs == null) continue;

					if (!entitiesWithRHSvalues.containsKey(entity)) {
						entitiesWithRHSvalues.put(entity, new HashMap<>());
					}
					if (!entitiesWithRHSvalues.get(entity).containsKey(rhs)) {
						entitiesWithRHSvalues.get(entity).put(rhs, new ArrayList<>());
					}
					entitiesWithRHSvalues.get(entity).get(rhs).add(t);
					numOfMatches++;
				}
			}
			System.out.println("Number of matches: " + numOfMatches);
			t++;
		}
		if (entitiesWithRHSvalues.size() == 0) return null;

		Comparator<Entry<ConstantLiteral, List<Integer>>> comparator = new Comparator<Entry<ConstantLiteral, List<Integer>>>() {
			@Override
			public int compare(Entry<ConstantLiteral, List<Integer>> o1, Entry<ConstantLiteral, List<Integer>> o2) {
				return o2.getValue().size() - o1.getValue().size();
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
				Set<String> attrNameSet = this.vertexTypesAttributes.get(vertexType);
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

	public boolean isPathVisited(AttributeDependency path, ArrayList<AttributeDependency> visitedPaths) {
		long visitedPathCheckingTime = System.currentTimeMillis();
		for (AttributeDependency visitedPath : visitedPaths) {
			if (visitedPath.size() == path.size()
					&& visitedPath.getLhs().containsAll(path.getLhs())
					&& visitedPath.getRhs().equals(path.getRhs())) {
				System.out.println("This literal path was already visited.");
				return true;
			}
		}
		visitedPathCheckingTime = System.currentTimeMillis() - visitedPathCheckingTime;
		printWithTime("visitedPathCheckingTime", visitedPathCheckingTime);
		totalVisitedPathCheckingTime += visitedPathCheckingTime;
		return false;
	}

	public boolean isSupersetPath(AttributeDependency path, ArrayList<AttributeDependency> prunedPaths) {
		long supersetPathCheckingTime = System.currentTimeMillis();
		boolean isPruned = false;
		for (AttributeDependency prunedPath : prunedPaths) {
			if (path.getRhs().equals(prunedPath.getRhs()) && path.getLhs().containsAll(prunedPath.getLhs())) {
				System.out.println("Candidate path " + path + " is a superset of pruned path " + prunedPath);
				isPruned = true;
			}
		}
		supersetPathCheckingTime = System.currentTimeMillis()-supersetPathCheckingTime;
		printWithTime("supersetPathCheckingTime", supersetPathCheckingTime);
		totalSupersetPathCheckingTime += supersetPathCheckingTime;
		return isPruned;
	}

	public ArrayList<TGFD> deltaDiscovery(PatternTreeNode patternNode, LiteralTreeNode literalTreeNode, AttributeDependency literalPath, ArrayList<ArrayList<HashSet<ConstantLiteral>>> matchesPerTimestamps) {
		ArrayList<TGFD> tgfds = new ArrayList<>();

		// Add dependency attributes to pattern
		// TO-DO: Fix - when multiple vertices in a pattern have the same type, attribute values get overwritten
		VF2PatternGraph patternForDependency = patternNode.getPattern().copy();
		Set<ConstantLiteral> attributesSetForDependency = new HashSet<>();
		attributesSetForDependency.addAll(literalPath.getLhs());
		attributesSetForDependency.add(literalPath.getRhs());
		for (Vertex v : patternForDependency.getPattern().vertexSet()) {
			String vType = new ArrayList<>(v.getTypes()).get(0);
			for (ConstantLiteral attribute : attributesSetForDependency) {
				if (vType.equals(attribute.getVertexType())) {
					v.addAttribute(new Attribute(attribute.getAttrName()));
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
		totalFindEntitiesTime += findEntitiesTime;
		if (entities == null) {
			System.out.println("Mark as Pruned. No entities found during entity discovery.");
			if (!this.hasNoSupportPruning()) {
				literalTreeNode.setIsPruned();
				patternNode.addLowSupportDependency(literalPath);
			}
			return tgfds;
		}

		System.out.println("Discovering constant TGFDs");

		// Find Constant TGFDs
		ArrayList<Pair> constantXdeltas = new ArrayList<>();
		ArrayList<TreeSet<Pair>> satisfyingAttrValues = new ArrayList<>();
		long discoverConstantTGFDsTime = System.currentTimeMillis();
		ArrayList<TGFD> constantTGFDs = discoverConstantTGFDs(patternNode, literalPath.getRhs(), entities, constantXdeltas, satisfyingAttrValues);
		discoverConstantTGFDsTime = System.currentTimeMillis() - discoverConstantTGFDsTime;
		printWithTime("discoverConstantTGFDsTime", discoverConstantTGFDsTime);
		totalDiscoverConstantTGFDsTime += discoverConstantTGFDsTime;
		// TO-DO: Try discover general TGFD even if no constant TGFD candidate met support threshold
		System.out.println("Constant TGFDs discovered: " + constantTGFDs.size());
		tgfds.addAll(constantTGFDs);

		System.out.println("Discovering general TGFDs");

		// Find general TGFDs
		long discoverGeneralTGFDTime = System.currentTimeMillis();
		ArrayList<TGFD> generalTGFD = discoverGeneralTGFD(patternNode, patternNode.getPatternSupport(), literalPath, entities.size(), constantXdeltas, satisfyingAttrValues);
		discoverGeneralTGFDTime = System.currentTimeMillis() - discoverGeneralTGFDTime;
		printWithTime("discoverGeneralTGFDTime", discoverGeneralTGFDTime);
		totalDiscoverGeneralTGFDTime += discoverGeneralTGFDTime;
		if (generalTGFD.size() > 0) {
			System.out.println("Marking literal node as pruned. Discovered general TGFDs for this dependency.");
			if (!this.noMinimalityPruning) {
				literalTreeNode.setIsPruned();
				patternNode.addMinimalDependency(literalPath);
			}
		}
		tgfds.addAll(generalTGFD);

		return tgfds;
	}

	private ArrayList<TGFD> discoverGeneralTGFD(PatternTreeNode patternTreeNode, double patternSupport, AttributeDependency literalPath, int entitiesSize, ArrayList<Pair> constantXdeltas, ArrayList<TreeSet<Pair>> satisfyingAttrValues) {

		ArrayList<TGFD> tgfds = new ArrayList<>();

		System.out.println("Size of constantXdeltas: " + constantXdeltas.size());
		for (Pair deltaPair : constantXdeltas) {
			System.out.println("constant delta: " + deltaPair);
		}

		System.out.println("Size of satisfyingAttrValues: " + satisfyingAttrValues.size());
		for (Set<Pair> satisfyingPairs : satisfyingAttrValues) {
			System.out.println("satisfyingAttrValues entry: " + satisfyingPairs);
		}

		// Find intersection delta
		HashMap<Pair, ArrayList<TreeSet<Pair>>> intersections = new HashMap<>();
		int currMin = 0;
		int currMax = this.getNumOfSnapshots() - 1;
		// TO-DO: Verify if TreeSet<Pair> is being sorted correctly
		ArrayList<TreeSet<Pair>> currSatisfyingAttrValues = new ArrayList<>();
		for (int index = 0; index < constantXdeltas.size(); index++) {
			Pair deltaPair = constantXdeltas.get(index);
			if (Math.max(currMin, deltaPair.min()) <= Math.min(currMax, deltaPair.max())) {
				currMin = Math.max(currMin, deltaPair.min());
				currMax = Math.min(currMax, deltaPair.max());
				currSatisfyingAttrValues.add(satisfyingAttrValues.get(index)); // By axiom 4
			} else {
				intersections.putIfAbsent(new Pair(currMin, currMax), currSatisfyingAttrValues);
				currSatisfyingAttrValues = new ArrayList<>();
				currMin = 0;
				currMax = this.getNumOfSnapshots() - 1;
				if (Math.max(currMin, deltaPair.min()) <= Math.min(currMax, deltaPair.max())) {
					currMin = Math.max(currMin, deltaPair.min());
					currMax = Math.min(currMax, deltaPair.max());
					currSatisfyingAttrValues.add(satisfyingAttrValues.get(index));
				}
			}
		}
		intersections.putIfAbsent(new Pair(currMin, currMax), currSatisfyingAttrValues);

		ArrayList<Entry<Pair, ArrayList<TreeSet<Pair>>>> sortedIntersections = new ArrayList<>(intersections.entrySet());
		sortedIntersections.sort(new Comparator<Entry<Pair, ArrayList<TreeSet<Pair>>>>() {
			@Override
			public int compare(Entry<Pair, ArrayList<TreeSet<Pair>>> o1, Entry<Pair, ArrayList<TreeSet<Pair>>> o2) {
				return o2.getValue().size() - o1.getValue().size();
			}
		});

		System.out.println("Candidate deltas for general TGFD:");
		for (Entry<Pair, ArrayList<TreeSet<Pair>>> intersection : sortedIntersections) {
			System.out.println(intersection.getKey());
		}

		System.out.println("Evaluating candidate deltas for general TGFD...");
		for (Entry<Pair, ArrayList<TreeSet<Pair>>> intersection : sortedIntersections) {
			Pair candidateDelta = intersection.getKey();
//			if (!this.noSupportPruning && isSupersetPath(literalPath, candidateDelta, patternTreeNode.getAllLowSupportGeneralTgfds())) {
//				continue;
//			}
			int generalMin = candidateDelta.min();
			int generalMax = candidateDelta.max();
			System.out.println("Calculating support for candidate general TGFD candidate delta: " + intersection.getKey());

			// Compute general support
			float numerator;
			float denominator = 2 * entitiesSize * this.getNumOfSnapshots();

			int numberOfSatisfyingPairs = 0;
			for (TreeSet<Pair> timestamps : intersection.getValue()) {
				TreeSet<Pair> satisfyingPairs = new TreeSet<Pair>();
				for (Pair timestamp : timestamps) {
					if (timestamp.max() - timestamp.min() >= generalMin && timestamp.max() - timestamp.min() <= generalMax) {
						satisfyingPairs.add(new Pair(timestamp.min(), timestamp.max()));
					}
				}
				numberOfSatisfyingPairs += satisfyingPairs.size();
			}

			System.out.println("Number of satisfying pairs: " + numberOfSatisfyingPairs);

			numerator = numberOfSatisfyingPairs;

			float support = numerator / denominator;
			System.out.println("Candidate general TGFD support: " + support);
			this.generalTgfdSupportsList.add(support);
			if (support < this.getTheta()) {
//				if (!this.noSupportPruning) patternTreeNode.addLowSupportDependency(literalPath);
				System.out.println("Support for candidate general TGFD is below support threshold");
				continue;
			}

			System.out.println("TGFD Support = " + numerator + "/" + denominator);

			Delta delta = new Delta(Period.ofDays(generalMin * 183), Period.ofDays(generalMax * 183 + 1), Duration.ofDays(183));

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

			System.out.println("Creating new general TGFD...");
			TGFD tgfd = new TGFD(patternTreeNode.getPattern(), delta, generalDependency, support, patternSupport, "");
			System.out.println("TGFD: " + tgfd);
			tgfds.add(tgfd);
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

	private ArrayList<TGFD> discoverConstantTGFDs(PatternTreeNode patternNode, ConstantLiteral yLiteral, Map<Set<ConstantLiteral>, ArrayList<Entry<ConstantLiteral, List<Integer>>>> entities, ArrayList<Pair> constantXdeltas, ArrayList<TreeSet<Pair>> satisfyingAttrValues) {
		ArrayList<TGFD> tgfds = new ArrayList<>();
		String yVertexType = yLiteral.getVertexType();
		String yAttrName = yLiteral.getAttrName();
		for (Entry<Set<ConstantLiteral>, ArrayList<Entry<ConstantLiteral, List<Integer>>>> entityEntry : entities.entrySet()) {
			VF2PatternGraph newPattern = patternNode.getPattern().copy();
			Dependency newDependency = new Dependency();
			AttributeDependency constantPath = new AttributeDependency();
			for (Vertex v : newPattern.getPattern().vertexSet()) {
				String vType = new ArrayList<>(v.getTypes()).get(0);
				if (vType.equalsIgnoreCase(yVertexType)) { // TO-DO: What if our pattern has duplicate vertex types?
					v.addAttribute(new Attribute(yAttrName));
					if (newDependency.getY().size() == 0) {
						VariableLiteral newY = new VariableLiteral(yVertexType, yAttrName, yVertexType, yAttrName);
						newDependency.addLiteralToY(newY);
					}
				}
				for (ConstantLiteral xLiteral : entityEntry.getKey()) {
					if (xLiteral.getVertexType().equalsIgnoreCase(vType)) {
						v.addAttribute(new Attribute(xLiteral.getAttrName(), xLiteral.getAttrValue()));
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
			ArrayList<Entry<ConstantLiteral, List<Integer>>> attrValuesTimestampsSortedByFreq = entityEntry.getValue();
			for (Map.Entry<ConstantLiteral, List<Integer>> entry : attrValuesTimestampsSortedByFreq) {
				System.out.println(entry.getKey() + ":" + entry.getValue());
			}

			ArrayList<Pair> candidateDeltas = new ArrayList<>();
			if (attrValuesTimestampsSortedByFreq.size() == 1) {
				List<Integer> timestamps = attrValuesTimestampsSortedByFreq.get(0).getValue();
				int minDistance = this.getNumOfSnapshots() - 1;
				int maxDistance = timestamps.get(timestamps.size() - 1) - timestamps.get(0);
				for (int index = 1; index < timestamps.size(); index++) {
					minDistance = Math.min(minDistance, timestamps.get(index) - timestamps.get(index - 1));
				}
				if (minDistance > maxDistance) {
					System.out.println("Not enough timestamped matches found for entity.");
					continue;
				}
				candidateDeltas.add(new Pair(minDistance, maxDistance));
			} else if (attrValuesTimestampsSortedByFreq.size() > 1) {
				int minExclusionDistance = this.getNumOfSnapshots() - 1;
				int maxExclusionDistance = 0;
				ArrayList<Integer> distances = new ArrayList<>();
				int l1 = attrValuesTimestampsSortedByFreq.get(0).getValue().get(0);
				int u1 = attrValuesTimestampsSortedByFreq.get(0).getValue().get(attrValuesTimestampsSortedByFreq.get(0).getValue().size() - 1);
				for (int index = 1; index < attrValuesTimestampsSortedByFreq.size(); index++) {
					int l2 = attrValuesTimestampsSortedByFreq.get(index).getValue().get(0);
					int u2 = attrValuesTimestampsSortedByFreq.get(index).getValue().get(attrValuesTimestampsSortedByFreq.get(index).getValue().size() - 1);
					distances.add(Math.abs(u2 - l1));
					distances.add(Math.abs(u1 - l2));
				}
				for (int index = 0; index < distances.size(); index++) {
					minExclusionDistance = Math.min(minExclusionDistance, distances.get(index));
					maxExclusionDistance = Math.max(maxExclusionDistance, distances.get(index));
				}

				if (minExclusionDistance > 0) {
					Pair deltaPair = new Pair(0, minExclusionDistance - 1);
					candidateDeltas.add(deltaPair);
				}
				if (maxExclusionDistance < this.getNumOfSnapshots() - 1) {
					Pair deltaPair = new Pair(maxExclusionDistance + 1, this.getNumOfSnapshots() - 1);
					candidateDeltas.add(deltaPair);
				}
			}
			if (candidateDeltas.size() == 0) {
				System.out.println("Could not find any deltas for entity: " + entityEntry.getKey());
				continue;
			}

			// Compute TGFD support
			Delta candidateTGFDdelta;
			float candidateTGFDsupport = 0;
			Pair mostSupportedDelta = null;
			TreeSet<Pair> mostSupportedSatisfyingPairs = null;
			for (Pair candidateDelta : candidateDeltas) {
				int minDistance = candidateDelta.min();
				int maxDistance = candidateDelta.max();
				if (minDistance <= maxDistance) {
					System.out.println("Calculating support for candidate delta ("+minDistance+","+maxDistance+")");
					float numer;
					float denom = 2 * entities.size() * this.getNumOfSnapshots();
					List<Integer> timestamps = attrValuesTimestampsSortedByFreq.get(0).getValue();
					TreeSet<Pair> satisfyingPairs = new TreeSet<Pair>();
					for (int index = 0; index < timestamps.size() - 1; index++) {
						for (int j = index + 1; j < timestamps.size(); j++) {
							if (timestamps.get(j) - timestamps.get(index) >= minDistance && timestamps.get(j) - timestamps.get(index) <= maxDistance) {
								satisfyingPairs.add(new Pair(timestamps.get(index), timestamps.get(j)));
							}
						}
					}

					System.out.println("Satisfying pairs: " + satisfyingPairs);

					numer = satisfyingPairs.size();
					float candidateSupport = numer / denom;

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
			satisfyingAttrValues.add(mostSupportedSatisfyingPairs);
			constantXdeltas.add(mostSupportedDelta);

			this.constantTgfdSupportsList.add(candidateTGFDsupport); // Statistics

			// Only output constant TGFDs that satisfy support
			if (candidateTGFDsupport < this.getTheta()) {
				System.out.println("Could not satisfy TGFD support threshold for entity: " + entityEntry.getKey());
				continue;
			}
			int minDistance = mostSupportedDelta.min();
			int maxDistance = mostSupportedDelta.max();
			candidateTGFDdelta = new Delta(Period.ofDays(minDistance * 183), Period.ofDays(maxDistance * 183 + 1), Duration.ofDays(183));
			System.out.println("Constant TGFD delta: "+candidateTGFDdelta);

			if (!this.noMinimalityPruning && isSupersetPath(constantPath, patternNode.getAllMinimalConstantDependenciesOnThisPath())) { // Ensures we don't expand constant TGFDs from previous iterations
				System.out.println("Candidate constant TGFD " + constantPath + " is a superset of an existing minimal constant TGFD");
				continue;
			}
			System.out.println("Creating new constant TGFD...");
			TGFD entityTGFD = new TGFD(newPattern, candidateTGFDdelta, newDependency, candidateTGFDsupport, patternNode.getPatternSupport(), "");
			System.out.println("TGFD: " + entityTGFD);
			tgfds.add(entityTGFD);
			if (!this.noMinimalityPruning) patternNode.addMinimalConstantDependency(constantPath);
		}
		constantXdeltas.sort(new Comparator<Pair>() {
			@Override
			public int compare(Pair o1, Pair o2) {
				return o1.compareTo(o2);
			}
		});
		return tgfds;
	}

	public ArrayList<TGFD> getDummyTGFDs() {
		ArrayList<TGFD> dummyTGFDs = new ArrayList<>();
//		for (Map.Entry<String,Integer> frequentVertexTypeEntry : getSortedFrequentVertexTypesHistogram()) {
//			String frequentVertexType = frequentVertexTypeEntry.getKey();
//			VF2PatternGraph patternGraph = new VF2PatternGraph();
//			PatternVertex patternVertex = new PatternVertex(frequentVertexType);
//			patternGraph.addVertex(patternVertex);
////			HashSet<ConstantLiteral> activeAttributes = getActiveAttributesInPattern(patternGraph.getPattern().vertexSet());
////			for (ConstantLiteral activeAttribute: activeAttributes) {
//				TGFD dummyTGFD = new TGFD();
//				dummyTGFD.setName(frequentVertexType);
//				dummyTGFD.setPattern(patternGraph);
////				Dependency dependency = new Dependency();
////				dependency.addLiteralToY(activeAttribute);
//				dummyTGFDs.add(dummyTGFD);
////			}
//		}
		for (Map.Entry<String,Integer> frequentEdgeEntry : this.sortedEdgeHistogram) {
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
				dummyTGFD.setName(frequentEdge);
				dummyTGFD.setPattern(patternGraph);
//				Dependency dependency = new Dependency();
//				dependency.addLiteralToY(activeAttribute);
				dummyTGFDs.add(dummyTGFD);
//			}
		}
		return dummyTGFDs;
	}


	private void loadModels(Long graphSize) {
		String fileSuffix = graphSize == null ? "" : "-" + graphSize;
		for (String pathString : Arrays.asList("2015types" + fileSuffix + ".ttl", "2015literals" + fileSuffix + ".ttl", "2015objects" + fileSuffix + ".ttl")) {
			System.out.println("Reading " + pathString);
			Model model = ModelFactory.createDefaultModel();
			Path input = Paths.get(pathString);
			model.read(input.toUri().toString());
			this.models.add(model);
		}
	}

	public ArrayList<GraphLoader> loadCitationSnapshots(List<String> filePaths) {
		ArrayList<GraphLoader> graphs = new ArrayList<>();
		for (String filePath: filePaths) {
			long graphLoadTime = System.currentTimeMillis();
			graphs.add(new CitationLoader(filePath, filePath.contains("v11")));
			printWithTime(filePath+" graphLoadTime", (System.currentTimeMillis() - graphLoadTime));
			if (this.isUseChangeFile()) break;
		}
		return graphs;
	}

	public ArrayList<GraphLoader> loadDBpediaSnapshots2(String path) {
		ArrayList<TGFD> dummyTGFDs = new ArrayList<>();
		ArrayList<GraphLoader> graphs = new ArrayList<>();
		ArrayList<File> directories = new ArrayList<>(List.of(Objects.requireNonNull(new File(path).listFiles(File::isDirectory))));
		directories.sort(Comparator.comparing(File::getName));
		for (File directory: directories) {
			final long graphLoadTime = System.currentTimeMillis();
			ArrayList<File> files = new ArrayList<>(List.of(Objects.requireNonNull(new File(directory.getPath()).listFiles(File::isFile))));
			List<String> paths = files.stream().map(File::getPath).collect(Collectors.toList());
			DBPediaLoader dbpedia = new DBPediaLoader(dummyTGFDs, paths);
			graphs.add(dbpedia);
			printWithTime("graphLoadTime", (System.currentTimeMillis() - graphLoadTime));
			if (this.isUseChangeFile()) break;
		}
		this.setNumOfSnapshots(graphs.size());
		return graphs;
	}

	public ArrayList<GraphLoader> loadDBpediaSnapshots(Long graphSize) {
		ArrayList<TGFD> dummyTGFDs = new ArrayList<>();
		ArrayList<GraphLoader> graphs = new ArrayList<>();
		String fileSuffix = graphSize == null ? "" : "-" + graphSize;
		for (int year = 5; year < 8; year++) {
			final long graphLoadTime = System.currentTimeMillis();
			String typeFileName = "201" + year + "types" + fileSuffix + ".ttl";
			String literalsFileName = "201" + year + "literals" + fileSuffix + ".ttl";
			String objectsFileName = "201" + year + "objects" + fileSuffix + ".ttl";
			DBPediaLoader dbpedia = new DBPediaLoader(dummyTGFDs, new ArrayList<>(Collections.singletonList(typeFileName)), new ArrayList<>(Arrays.asList(literalsFileName, objectsFileName)));
			graphs.add(dbpedia);
			printWithTime("graphLoadTime", (System.currentTimeMillis() - graphLoadTime));
			if (this.isUseChangeFile()) break;
		}
		return graphs;
	}

	public ArrayList<TGFD> hSpawn(PatternTreeNode patternTreeNode, ArrayList<ArrayList<HashSet<ConstantLiteral>>> matchesPerTimestamps) {
		ArrayList<TGFD> tgfds = new ArrayList<>();

		System.out.println("Performing HSpawn for " + patternTreeNode.getPattern());

		HashSet<ConstantLiteral> activeAttributes = getActiveAttributesInPattern(patternTreeNode.getGraph().vertexSet(), false);

		LiteralTree literalTree = new LiteralTree();
		for (int j = 0; j < activeAttributes.size(); j++) {

			System.out.println("HSpawn level " + j + "/" + activeAttributes.size());

			if (j == 0) {
				literalTree.addLevel();
				for (ConstantLiteral literal: activeAttributes) {
					literalTree.createNodeAtLevel(j, literal, null);
				}
			} else if ((j + 1) <= patternTreeNode.getGraph().vertexSet().size()) { // Ensures # of literals in dependency equals number of vertices in graph
				ArrayList<LiteralTreeNode> literalTreePreviousLevel = literalTree.getLevel(j - 1);
				if (literalTreePreviousLevel.size() == 0) {
					System.out.println("Previous level of literal tree is empty. Nothing to expand. End HSpawn");
					break;
				}
				literalTree.addLevel();
				ArrayList<AttributeDependency> visitedPaths = new ArrayList<>(); //TO-DO: Can this be implemented as HashSet to improve performance?
				ArrayList<TGFD> currentLevelTGFDs = new ArrayList<>();
				for (LiteralTreeNode previousLevelLiteral : literalTreePreviousLevel) {
					System.out.println("Creating literal tree node " + literalTree.getLevel(j).size() + "/" + (literalTreePreviousLevel.size() * (literalTreePreviousLevel.size()-1)));
					if (previousLevelLiteral.isPruned()) continue;
					ArrayList<ConstantLiteral> parentsPathToRoot = previousLevelLiteral.getPathToRoot(); //TO-DO: Can this be implemented as HashSet to improve performance?
					for (ConstantLiteral literal: activeAttributes) {
						if (this.interestingTGFDs) { // Ensures all vertices are involved in dependency
							if (isUsedVertexType(literal.getVertexType(), parentsPathToRoot)) continue;
						} else { // Check if leaf node exists in path already
							if (parentsPathToRoot.contains(literal)) continue;
						}

						// Check if path to candidate leaf node is unique
						AttributeDependency newPath = new AttributeDependency(previousLevelLiteral.getPathToRoot(),literal);
						System.out.println("New candidate literal path: " + newPath);
						if (isPathVisited(newPath, visitedPaths)) { // TO-DO: Is this relevant anymore?
							System.out.println("Skip. Duplicate literal path.");
							continue;
						}

						if (!this.hasNoSupportPruning() && isSupersetPath(newPath, patternTreeNode.getLowSupportDependenciesOnThisPath())) { // Ensures we don't re-explore dependencies whose subsets have no entities
							System.out.println("Skip. Candidate literal path is a superset of low-support dependency.");
							continue;
						}
						if (!this.noMinimalityPruning && isSupersetPath(newPath, patternTreeNode.getAllMinimalDependenciesOnThisPath())) { // Ensures we don't re-explore dependencies whose subsets have already have a general dependency
							System.out.println("Skip. Candidate literal path is a superset of minimal dependency.");
							continue;
						}
						System.out.println("Newly created unique literal path: " + newPath);

						// Add leaf node to tree
						LiteralTreeNode literalTreeNode = literalTree.createNodeAtLevel(j, literal, previousLevelLiteral);

						// Ensures delta discovery only occurs when # of literals in dependency equals number of vertices in graph
						if (this.interestingTGFDs && (j + 1) != patternTreeNode.getGraph().vertexSet().size()) {
							System.out.println("|LHS|+|RHS| != |Q|. Skip performing Delta Discovery HSpawn level " + j);
							continue;
						}
						visitedPaths.add(newPath);

						System.out.println("Performing Delta Discovery at HSpawn level " + j);
						final long deltaDiscoveryTime = System.currentTimeMillis();
						ArrayList<TGFD> discoveredTGFDs = deltaDiscovery(patternTreeNode, literalTreeNode, newPath, matchesPerTimestamps);
						printWithTime("deltaDiscovery", System.currentTimeMillis()-deltaDiscoveryTime);
						currentLevelTGFDs.addAll(discoveredTGFDs);
					}
				}
				if (currentLevelTGFDs.size() > 0) {
					System.out.println("TGFDs generated at HSpawn level " + j + ": " + currentLevelTGFDs.size());
					tgfds.addAll(currentLevelTGFDs);
				}
			} else {
				break;
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

	public double getTheta() {
		return theta;
	}

	public long getTotalVSpawnTime() {
		return totalVSpawnTime;
	}

	public void addToTotalVSpawnTime(long vSpawnTime) {
		this.totalVSpawnTime += vSpawnTime;
	}

	public long getTotalMatchingTime() {
		return totalMatchingTime;
	}

	public void addToTotalMatchingTime(long matchingTime) {
		this.totalMatchingTime += matchingTime;
	}

	public boolean hasNoSupportPruning() {
		return this.noSupportPruning;
	}

	public void setNoSupportPruning(boolean noSupportPruning) {
		this.noSupportPruning = noSupportPruning;
	}

	public boolean isSkipK1() {
		return skipK1;
	}

	public ArrayList<ArrayList<TGFD>> getTgfds() {
		return tgfds;
	}

	public boolean isDontSortHistogram() {
		return dontSortHistogram;
	}

	public boolean isUseSubgraph() {
		return useSubgraph;
	}

	public boolean isGeneratek0Tgfds() {
		return generatek0Tgfds;
	}

	public boolean isUseChangeFile() {
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

	public void setNumOfSnapshots(int numOfSnapshots) {
		this.numOfSnapshots = numOfSnapshots;
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
	}

//	public void discover(int k, double theta, String experimentName) {
//		// TO-DO: Is it possible to prune the graphs by passing them our histogram's frequent nodes and edges as dummy TGFDs ???
////		Config.optimizedLoadingBasedOnTGFD = !tgfdDiscovery.isNaive;
//
//		PrintStream kExperimentResultsFile = null;
//		if (experimentName.startsWith("k")) {
//			try {
//				String timeAndDateStamp = ZonedDateTime.now(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern("uuuu.MM.dd.HH.mm.ss"));
//				kExperimentResultsFile = new PrintStream("k-experiments-runtimes-" + (isNaive ? "naive" : "optimized") + (interestingTGFDs ? "-interesting" : "") + "-" + timeAndDateStamp + ".txt");
//			} catch (FileNotFoundException e) {
//				e.printStackTrace();
//			}
//		}
//		final long kStartTime = System.currentTimeMillis();
//		loadDBpediaSnapshots();
//		for (int i = 0; i <= k; i++) {
//			ArrayList<TGFD> tgfds = vSpawn(i);
//
//			printTgfdsToFile(experimentName, i, theta, tgfds);
//
//			final long kEndTime = System.currentTimeMillis();
//			final long kRunTime = kEndTime - kStartTime;
//			if (experimentName.startsWith("k") && kExperimentResultsFile != null) {
//				System.out.println("Total execution time for k = " + k + " : " + kRunTime);
//				kExperimentResultsFile.print("k = " + i);
//				kExperimentResultsFile.println(", execution time = " + kRunTime);
//			}
//
//			System.gc();
//		}
//	}

	public void vSpawnInit(ArrayList<GraphLoader> graphs) {
		this.patternTree = new PatternTree();
		this.patternTree.addLevel();

		System.out.println("VSpawn Level 0");
		for (int i = 0; i < this.sortedVertexHistogram.size(); i++) {
			System.out.println("VSpawnInit with single-node pattern " + (i+1) + "/" + this.sortedVertexHistogram.size());
			String vertexType = this.sortedVertexHistogram.get(i).getKey();

			if (this.vertexTypesAttributes.get(vertexType).size() < 2)
				continue; // TO-DO: Are we interested in TGFDs where LHS is empty?

			int numOfInstancesOfVertexType = this.sortedVertexHistogram.get(i).getValue();
			int numOfInstancesOfAllVertexTypes = this.NUM_OF_VERTICES_IN_GRAPH;

			double patternSupport = (double) numOfInstancesOfVertexType / (double) numOfInstancesOfAllVertexTypes;
			System.out.println("Estimate Pattern Support: " + patternSupport);

			System.out.println("Vertex type: "+vertexType);
			VF2PatternGraph candidatePattern = new VF2PatternGraph();
			PatternVertex vertex = new PatternVertex(vertexType);
			candidatePattern.addVertex(vertex);
			candidatePattern.getCenterVertexType();
			System.out.println("VSpawnInit with single-node pattern " + (i+1) + "/" + this.sortedVertexHistogram.size() + ": " + candidatePattern.getPattern().vertexSet());

			PatternTreeNode patternTreeNode;
			if (this.isDontSortHistogram()) {
				if (patternSupport >= this.edgeSupportThreshold) {
					patternTreeNode = this.patternTree.createNodeAtLevel(this.getCurrentVSpawnLevel(), candidatePattern, patternSupport);
				} else {
					System.out.println("Pattern support of " + patternSupport + " is below threshold.");
					continue;
				}
			} else {
				patternTreeNode = this.patternTree.createNodeAtLevel(this.getCurrentVSpawnLevel(), candidatePattern, patternSupport);
			}
			if (this.isUseSubgraph() && !this.isGeneratek0Tgfds()) {
				final long extractMatchesForCenterVertexTime = System.currentTimeMillis();
				ArrayList<ArrayList<DataVertex>> matchesOfThisCenterVertexPerTimestamp = extractMatchesForCenterVertex(graphs, vertexType);
				patternTreeNode.setMatchesOfCenterVertices(matchesOfThisCenterVertexPerTimestamp);
				int numOfMatches = 0;
				for (ArrayList<DataVertex> matchesOfThisCenterVertex: matchesOfThisCenterVertexPerTimestamp) {
					numOfMatches += matchesOfThisCenterVertex.size();
				}
				System.out.println("Number of center vertex matches found: " + numOfMatches);
				printWithTime("extractMatchesForCenterVertexTime", (System.currentTimeMillis() - extractMatchesForCenterVertexTime));
				continue;
			}
			if (this.isGeneratek0Tgfds()) {
				ArrayList<ArrayList<HashSet<ConstantLiteral>>> matchesPerTimestamps = new ArrayList<>();
				for (int year = 0; year < this.getNumOfSnapshots(); year++) {
					matchesPerTimestamps.add(new ArrayList<>());
				}
				getMatchesUsingCenterVertices(graphs, patternTreeNode, matchesPerTimestamps);
				if (patternTreeNode.getPatternSupport() < this.getTheta()) {
					System.out.println("Mark as pruned. Real pattern support too low for pattern " + patternTreeNode.getPattern());
					if (!this.hasNoSupportPruning()) patternTreeNode.setIsPruned();
					continue;
				}
				final long hSpawnStartTime = System.currentTimeMillis();
				ArrayList<TGFD> tgfds = this.hSpawn(patternTreeNode, matchesPerTimestamps);
				printWithTime("hSpawn", (System.currentTimeMillis() - hSpawnStartTime));
				this.getTgfds().get(0).addAll(tgfds);
			}
		}
		System.out.println("GenTree Level " + this.getCurrentVSpawnLevel() + " size: " + this.patternTree.getLevel(this.getCurrentVSpawnLevel()).size());
		for (PatternTreeNode node : this.patternTree.getLevel(this.getCurrentVSpawnLevel())) {
			System.out.println("Pattern: " + node.getPattern());
			System.out.println("Pattern Support: " + node.getPatternSupport());
//			System.out.println("Dependency: " + node.getDependenciesSets());
		}

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

		if (this.getCandidateEdgeIndex() > this.sortedEdgeHistogram.size()-1) {
			this.setCandidateEdgeIndex(0);
			this.setPreviousLevelNodeIndex(this.getPreviousLevelNodeIndex() + 1);
		}

		if (this.getPreviousLevelNodeIndex() >= this.patternTree.getLevel(this.getCurrentVSpawnLevel() -1).size()) {
			this.kRuntimes.add(System.currentTimeMillis() - this.startTime);
			this.printTgfdsToFile(this.experimentName, this.getTgfds().get(this.getCurrentVSpawnLevel()));
			if (this.isKExperiment) this.printExperimentRuntimestoFile(experimentName, this.kRuntimes);
			this.printSupportStatistics();
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
		if (!this.hasNoSupportPruning() && previousLevelNode.isPruned()) {
			System.out.println("Marked as pruned. Skip.");
			this.setPreviousLevelNodeIndex(this.getPreviousLevelNodeIndex() + 1);
			return null;
		}

		System.out.println("Processing candidate edge " + this.getCandidateEdgeIndex() + "/" + this.sortedEdgeHistogram.size());
		Map.Entry<String, Integer> candidateEdge = this.sortedEdgeHistogram.get(this.getCandidateEdgeIndex());
		String candidateEdgeString = candidateEdge.getKey();
		System.out.println("Candidate edge:" + candidateEdgeString);

		// For non-optimized version only - checks if candidate edge is frequent enough
		if (this.isDontSortHistogram() && (1.0 * candidateEdge.getValue() / NUM_OF_EDGES_IN_GRAPH) < this.edgeSupportThreshold) {
			System.out.println("Candidate edge is below pattern support threshold. Skip");
			this.setCandidateEdgeIndex(this.getCandidateEdgeIndex() + 1);
			return null;
		}

		String sourceVertexType = candidateEdgeString.split(" ")[0];
		String targetVertexType = candidateEdgeString.split(" ")[2];

		if (this.vertexTypesAttributes.get(targetVertexType).size() == 0) {
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

			if (v.isMarked()) {
				System.out.println("Skip vertex. Already added candidate edge to vertex: " + v.getTypes());
				continue;
			}
			if (!v.getTypes().contains(sourceVertexType) && !v.getTypes().contains(targetVertexType)) {
				System.out.println("Skip vertex. Candidate edge does not connect to vertex: " + v.getTypes());
				v.setMarked(true);
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

//			double numerator = Double.MAX_VALUE;
//			double denominator = this.NUM_OF_EDGES_IN_GRAPH;
//
//			for (RelationshipEdge tempE : newPattern.getPattern().edgeSet()) {
//				String sourceType = (new ArrayList<>(tempE.getSource().getTypes())).get(0);
//				String targetType = (new ArrayList<>(tempE.getTarget().getTypes())).get(0);
//				String uniqueEdge = sourceType + " " + tempE.getLabel() + " " + targetType;
//				numerator = Math.min(numerator, uniqueEdgesHist.get(uniqueEdge));
//			}
//			assert numerator <= denominator;
//			double estimatedPatternSupport = numerator / denominator;
//			System.out.println("Estimate Pattern Support: " + estimatedPatternSupport);

			// TO-DO: Debug - Why does this work with strings but not subgraph isomorphism???
			if (isIsomorphicPattern(newPattern, this.patternTree)) {
				v.setMarked(true);
				System.out.println("Skip. Candidate pattern is an isomorph of existing pattern");
				continue;
			}

			if (!this.hasNoSupportPruning() && isSupergraphPattern(newPattern, this.patternTree)) {
				v.setMarked(true);
				System.out.println("Skip. Candidate pattern is a supergraph of pruned pattern");
				continue;
			}
			patternTreeNode = this.patternTree.createNodeAtLevel(this.getCurrentVSpawnLevel(), newPattern, previousLevelNode, candidateEdgeString);
			System.out.println("Marking vertex " + v.getTypes() + "as expanded.");
			break;
		}
		if (patternTreeNode == null) {
			for (Vertex v : previousLevelNode.getGraph().vertexSet()) {
				System.out.println("Unmarking all vertices in current pattern for the next candidate edge");
				v.setMarked(false);
			}
			this.setCandidateEdgeIndex(this.getCandidateEdgeIndex() + 1);
		}
		return patternTreeNode;
	}

	private boolean isIsomorphicPattern(VF2PatternGraph newPattern, PatternTree patternTree) {
		final long isIsomorphicPatternCheckTime = System.currentTimeMillis();
	    System.out.println("Checking if following pattern is isomorphic\n" + newPattern);
	    ArrayList<String> newPatternEdges = new ArrayList<>();
        newPattern.getPattern().edgeSet().forEach((edge) -> {newPatternEdges.add(edge.toString());});
        boolean isIsomorphic = false;
		for (PatternTreeNode otherPattern: patternTree.getLevel(this.getCurrentVSpawnLevel())) {
//			VF2AbstractIsomorphismInspector<Vertex, RelationshipEdge> results = new VF2SubgraphIsomorphism().execute2(newPattern.getPattern(), otherPattern.getPattern(), false);
//			if (results.isomorphismExists()) {
            ArrayList<String> otherPatternEdges = new ArrayList<>();
            otherPattern.getGraph().edgeSet().forEach((edge) -> {otherPatternEdges.add(edge.toString());});
//            if (newPattern.toString().equals(otherPattern.getPattern().toString())) {
            if (newPatternEdges.containsAll(otherPatternEdges)) {
				System.out.println("Candidate pattern: " + newPattern);
				System.out.println("is an isomorph of current VSpawn level pattern: " + otherPattern.getPattern());
				isIsomorphic = true;
			}
		}
		printWithTime("isIsomorphicPatternCheck", System.currentTimeMillis()-isIsomorphicPatternCheckTime);
		return isIsomorphic;
	}

	private boolean isSupergraphPattern(VF2PatternGraph newPattern, PatternTree patternTree) {
		final long isSupergraphPatternTime = System.currentTimeMillis();
        ArrayList<String> newPatternEdges = new ArrayList<>();
        newPattern.getPattern().edgeSet().forEach((edge) -> {newPatternEdges.add(edge.toString());});
		int i = this.getCurrentVSpawnLevel();
		boolean isSupergraph = false;
		while (i > 0) {
			for (PatternTreeNode otherPattern : patternTree.getLevel(i)) {
				if (otherPattern.isPruned()) {
//					VF2AbstractIsomorphismInspector<Vertex, RelationshipEdge> results = new VF2SubgraphIsomorphism().execute2(newPattern.getPattern(), otherPattern.getPattern(), false);
//			        if (results.isomorphismExists()) {
//                    if (newPattern.toString().equals(otherPattern.getPattern().toString())) {
                    ArrayList<String> otherPatternEdges = new ArrayList<>();
                    otherPattern.getGraph().edgeSet().forEach((edge) -> {otherPatternEdges.add(edge.toString());});
                    if (newPatternEdges.containsAll(otherPatternEdges)) {
                        System.out.println("Candidate pattern: " + newPattern);
						System.out.println("is a superset of pruned subgraph pattern: " + otherPattern.getPattern());
						isSupergraph = true;
					}
				}
			}
			i--;
		}
		printWithTime("isSupergraphPattern", System.currentTimeMillis()-isSupergraphPatternTime);
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

	public ArrayList<ArrayList<DataVertex>> extractMatchesForCenterVertex(ArrayList<GraphLoader> graphs, String patternVertexType) {
		ArrayList<ArrayList<DataVertex>> matchesOfThisCenterVertex = new ArrayList<>(3);
		for (int year = 0; year < this.getNumOfSnapshots(); year++) {
			ArrayList<DataVertex> matchesInThisTimestamp = new ArrayList<>();
			for (Vertex matchedVertex : graphs.get(year).getGraph().getGraph().vertexSet()) {
				if (matchedVertex.getTypes().iterator().next().equals(patternVertexType)) {
					DataVertex dataVertex = (DataVertex) matchedVertex;
					matchesInThisTimestamp.add(dataVertex);
				}
			}
			System.out.println("Number of matches found: " + matchesInThisTimestamp.size());
			matchesOfThisCenterVertex.add(matchesInThisTimestamp);
		}
		return matchesOfThisCenterVertex;
	}

	public int extractMatches(Set<RelationshipEdge> edgeSet, ArrayList<HashSet<ConstantLiteral>> matches, PatternTreeNode patternTreeNode, HashSet<String> entityURIs) {
		String patternEdgeLabel = patternTreeNode.getGraph().edgeSet().iterator().next().getLabel();
		String sourceVertexType = patternTreeNode.getGraph().edgeSet().iterator().next().getSource().getTypes().iterator().next();
		String targetVertexType = patternTreeNode.getGraph().edgeSet().iterator().next().getTarget().getTypes().iterator().next();
		int numOfMatches = 0;
		for (RelationshipEdge edge: edgeSet) {
			String matchedEdgeLabel = edge.getLabel();
			String matchedSourceVertexType = edge.getSource().getTypes().iterator().next();
			String matchedTargetVertexType = edge.getTarget().getTypes().iterator().next();
			if (matchedEdgeLabel.equals(patternEdgeLabel) && sourceVertexType.equals(matchedSourceVertexType) && targetVertexType.equals(matchedTargetVertexType)) {
				numOfMatches++;
				HashSet<ConstantLiteral> match = new HashSet<>();
				extractMatch(edge.getSource(), edge.getTarget(), patternTreeNode, match, entityURIs);
				if (match.size() <= patternTreeNode.getGraph().vertexSet().size()) continue;
				matches.add(match);
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

	public void getMatchesForPattern(ArrayList<GraphLoader> graphs, PatternTreeNode patternTreeNode, ArrayList<ArrayList<HashSet<ConstantLiteral>>> matchesPerTimestamps) {
		// TO-DO: Potential speed up for single-edge/single-node patterns. Iterate through all edges/nodes in graph.
		HashSet<String> entityURIs = new HashSet<>();
		patternTreeNode.getPattern().getCenterVertexType();

		for (int year = 0; year < this.getNumOfSnapshots(); year++) {
			long searchStartTime = System.currentTimeMillis();
			ArrayList<HashSet<ConstantLiteral>> matches = new ArrayList<>();
			int numOfMatchesInTimestamp = 0;
			if (this.getCurrentVSpawnLevel() == 1) {
				numOfMatchesInTimestamp = extractMatches(graphs.get(year).getGraph().getGraph().edgeSet(), matches, patternTreeNode, entityURIs);
			} else {
				VF2AbstractIsomorphismInspector<Vertex, RelationshipEdge> results = new VF2SubgraphIsomorphism().execute2(graphs.get(year).getGraph(), patternTreeNode.getPattern(), false);
				if (results.isomorphismExists()) {
					numOfMatchesInTimestamp = extractMatches(results.getMappings(), matches, patternTreeNode, entityURIs);
				}
			}
			System.out.println("Number of matches found: " + numOfMatchesInTimestamp);
			System.out.println("Number of matches found that contain active attributes: " + matches.size());
			matchesPerTimestamps.get(year).addAll(matches);
			printWithTime("Search Cost", (System.currentTimeMillis() - searchStartTime));
		}

		// TO-DO: Should we implement pattern support here to weed out patterns with few matches in later iterations?
		// Is there an ideal pattern support threshold after which very few TGFDs are discovered?
		// How much does the real pattern differ from the estimate?
		int numberOfMatchesFound = 0;
		for (ArrayList<HashSet<ConstantLiteral>> matchesInOneTimestamp : matchesPerTimestamps) {
			numberOfMatchesFound += matchesInOneTimestamp.size();
		}
		System.out.println("Total number of matches found across all snapshots:" + numberOfMatchesFound);

		calculatePatternSupport(entityURIs.size(), patternTreeNode);
	}

	private void calculatePatternSupport(int numberOfEntitiesFound, PatternTreeNode patternTreeNode) {
		String centerVertexType = patternTreeNode.getPattern().getCenterVertexType();
		System.out.println("Center vertex type: " + centerVertexType);
		float s = this.vertexHistogram.get(centerVertexType);
//		float numerator = numberOfEntitiesFound >= 2 ? CombinatoricsUtils.binomialCoefficient(numberOfEntitiesFound, 2) : numberOfEntitiesFound;
		float numerator = 2 * numberOfEntitiesFound * this.getNumOfSnapshots();
		float denominator = (2 * s * this.getNumOfSnapshots());
		assert numerator <= denominator;
		float realPatternSupport = numerator / denominator;
		System.out.println("Real Pattern Support: "+numerator+" / "+denominator+" = " + realPatternSupport);
		patternTreeNode.setPatternSupport(realPatternSupport);
		this.patternSupportsList.add(realPatternSupport);
	}

	private void extractMatch(GraphMapping<Vertex, RelationshipEdge> result, PatternTreeNode patternTreeNode, HashSet<ConstantLiteral> match, HashSet<String> entityURIs) {
		String entityURI = null;
		for (Vertex v : patternTreeNode.getGraph().vertexSet()) {
			Vertex currentMatchedVertex = result.getVertexCorrespondence(v, false);
			if (currentMatchedVertex == null) continue;
			if (entityURI == null) {
				entityURI = extractAttributes(patternTreeNode, match, currentMatchedVertex);
			} else {
				extractAttributes(patternTreeNode, match, currentMatchedVertex);
			}
		}
		if (entityURI != null && match.size() > patternTreeNode.getGraph().vertexSet().size()) {
			entityURIs.add(entityURI);
		}
	}

	private void extractMatch(Vertex currentSourceVertex, Vertex currentTargetVertex, PatternTreeNode patternTreeNode, HashSet<ConstantLiteral> match, HashSet<String> entityURIs) {
		String entityURI = null;
		for (Vertex currentMatchedVertex: Arrays.asList(currentSourceVertex, currentTargetVertex)) {
			if (entityURI == null) {
				entityURI = extractAttributes(patternTreeNode, match, currentMatchedVertex);
			} else {
				extractAttributes(patternTreeNode, match, currentMatchedVertex);
			}
		}
		if (entityURI != null && match.size() > patternTreeNode.getGraph().vertexSet().size()) {
			entityURIs.add(entityURI);
		}
	}

	private String extractAttributes(PatternTreeNode patternTreeNode, HashSet<ConstantLiteral> match, Vertex currentMatchedVertex) {
		String entityURI = null;
		String centerVertexType = patternTreeNode.getPattern().getCenterVertexType();
		String patternVertexType = new ArrayList<>(currentMatchedVertex.getTypes()).get(0);
		for (ConstantLiteral activeAttribute : getActiveAttributesInPattern(patternTreeNode.getGraph().vertexSet(),true)) {
			if (!activeAttribute.getVertexType().equals(patternVertexType)) continue;
			for (String matchedAttrName : currentMatchedVertex.getAllAttributesNames()) {
				if (patternVertexType.equals(centerVertexType) && matchedAttrName.equals("uri")) {
					entityURI = currentMatchedVertex.getAttributeValueByName(matchedAttrName);
				}
				if (!activeAttribute.getAttrName().equals(matchedAttrName)) continue;
				String matchedAttrValue = currentMatchedVertex.getAttributeValueByName(matchedAttrName);
				ConstantLiteral xLiteral = new ConstantLiteral(patternVertexType, matchedAttrName, matchedAttrValue);
				match.add(xLiteral);
			}
		}
		return entityURI;
	}

	private int extractMatches(Iterator<GraphMapping<Vertex, RelationshipEdge>> iterator, ArrayList<HashSet<ConstantLiteral>> matches, PatternTreeNode patternTreeNode, HashSet<String> entityURIs) {
		int numOfMatches = 0;
		while (iterator.hasNext()) {
			numOfMatches++;
			GraphMapping<Vertex, RelationshipEdge> result = iterator.next();
			HashSet<ConstantLiteral> match = new HashSet<>();
			extractMatch(result, patternTreeNode, match, entityURIs);
			// ensures that the match is not empty and contains more than just the uri attribute
			if (match.size() <= patternTreeNode.getGraph().vertexSet().size()) continue;
			matches.add(match);
		}
		matches.sort(new Comparator<HashSet<ConstantLiteral>>() {
			@Override
			public int compare(HashSet<ConstantLiteral> o1, HashSet<ConstantLiteral> o2) {
				return o1.size() - o2.size();
			}
		});
		return numOfMatches;
	}

	public void getMatchesForPattern2(ArrayList<GraphLoader> graphs, PatternTreeNode patternTreeNode, ArrayList<ArrayList<HashSet<ConstantLiteral>>> matchesPerTimestamps) {

		patternTreeNode.getPattern().setDiameter(this.getCurrentVSpawnLevel());

		TGFD dummyTgfd = new TGFD();
		dummyTgfd.setName(patternTreeNode.getEdgeString());
		dummyTgfd.setPattern(patternTreeNode.getPattern());

		System.out.println("-----------Snapshot (1)-----------");
		long startTime=System.currentTimeMillis();
		List<TGFD> tgfds = Collections.singletonList(dummyTgfd);
		int numberOfMatchesFound = 0;
//		LocalDate currentSnapshotDate = LocalDate.parse("2015-10-01");
		GraphLoader graph = graphs.get(0);

		printWithTime("Load graph (1)", System.currentTimeMillis()-startTime);

//		HashMap<String, MatchCollection> matchCollectionHashMap = new HashMap<>();
//		for (TGFD tgfd : tgfds) {
//			matchCollectionHashMap.put(tgfd.getName(), new MatchCollection(tgfd.getPattern(), tgfd.getDependency(), Duration.ofDays(183)));
//		}

		// Now, we need to find the matches for each snapshot.
		// Finding the matches...
		HashSet<String> entityURIs = new HashSet<>();

		for (TGFD tgfd : tgfds) {
//			VF2SubgraphIsomorphism VF2 = new VF2SubgraphIsomorphism();
			System.out.println("\n###########" + tgfd.getName() + "###########");
//			Iterator<GraphMapping<Vertex, RelationshipEdge>> results = VF2.execute(dbpedia.getGraph(), tgfd.getPattern(), false);

			//Retrieving and storing the matches of each timestamp.
//			System.out.println("Retrieving the matches");
//			startTime=System.currentTimeMillis();
//			matchCollectionHashMap.get(tgfd.getName()).addMatches(currentSnapshotDate, results);
//			printWithTime("Match retrieval", System.currentTimeMillis()-startTime);
			final long searchStartTime = System.currentTimeMillis();
			VF2AbstractIsomorphismInspector<Vertex, RelationshipEdge> results = new VF2SubgraphIsomorphism().execute2(graph.getGraph(), patternTreeNode.getPattern(), false);
			ArrayList<HashSet<ConstantLiteral>> matches = new ArrayList<>();
			if (results.isomorphismExists()) {
				extractMatches(results.getMappings(), matches, patternTreeNode, entityURIs);
			}
			numberOfMatchesFound += matches.size();
			matchesPerTimestamps.get(0).addAll(matches);
			printWithTime("Search Cost", (System.currentTimeMillis() - searchStartTime));
		}

		//Load the change files
//		List<String> paths = Arrays.asList("changes_t1_t2_film_starring_person_"+this.graphSize+"_full_test.json", "changes_t2_t3_film_starring_person_"+this.graphSize+"_full_test.json");
//		List<LocalDate> snapshotDates = Arrays.asList(LocalDate.parse("2016-04-01"), LocalDate.parse("2016-10-01"));
//		for (int i = 0; i < paths.size(); i++) {
		for (int i = 0; i < 2; i++) {
			System.out.println("-----------Snapshot (" + (i+2) + ")-----------");

//			currentSnapshotDate = snapshotDates.get(i);
//			ChangeLoader changeLoader = new ChangeLoader(paths.get(i));
//			List<Change> changes = changeLoader.getAllChanges();
			List<HashMap<Integer,HashSet<Change>>> changes = new ArrayList<>();
			List<Change> allChangesAsList=new ArrayList<>();
			for (String edgeString : patternTreeNode.getAllEdgeStrings()) {
				String path = "changes_t"+(i+1)+"_t"+(i+2)+"_"+edgeString.replace(" ", "_")+"_"+this.graphSize+".json";
				if (!this.changeFilesMap.containsKey(path)) {
					JSONParser parser = new JSONParser();
					Object json;
					org.json.simple.JSONArray jsonArray = new JSONArray();
					try {
						json = parser.parse(new FileReader(path));
						jsonArray = (org.json.simple.JSONArray) json;
					} catch (Exception e) {
						e.printStackTrace();
					}
					System.out.println("Storing " + path + " in memory");
					this.changeFilesMap.put(path, jsonArray);
				}
				startTime = System.currentTimeMillis();
				ChangeLoader changeLoader = new ChangeLoader(this.changeFilesMap.get(path));
				HashMap<Integer,HashSet<Change>> newChanges = changeLoader.getAllGroupedChanges();
				printWithTime("Load changes (" + path + ")", System.currentTimeMillis()-startTime);
				System.out.println("Total number of changes in changefile: " + newChanges.size());
				changes.add(newChanges);
				allChangesAsList.addAll(changeLoader.getAllChanges());
			}

//			printWithTime("Load changes ("+paths.get(i) + ")", System.currentTimeMillis()-startTime);
			System.out.println("Total number of changes: " + changes.size());

			// Now, we need to find the matches for each snapshot.
			// Finding the matches...

			startTime=System.currentTimeMillis();
			System.out.println("Updating the graph");
			IncUpdates incUpdatesOnDBpedia = new IncUpdates(graph.getGraph(), tgfds);
			incUpdatesOnDBpedia.AddNewVertices(allChangesAsList);

//			HashMap<String, ArrayList<String>> newMatchesSignaturesByTGFD = new HashMap<>();
//			HashMap<String, ArrayList<String>> removedMatchesSignaturesByTGFD = new HashMap<>();
			HashMap<String, TGFD> tgfdsByName = new HashMap<>();
			for (TGFD tgfd : tgfds) {
//				newMatchesSignaturesByTGFD.put(tgfd.getName(), new ArrayList<>());
//				removedMatchesSignaturesByTGFD.put(tgfd.getName(), new ArrayList<>());
				tgfdsByName.put(tgfd.getName(), tgfd);
			}
			ArrayList<HashSet<ConstantLiteral>> newMatches = new ArrayList<>();
			ArrayList<HashSet<ConstantLiteral>> removedMatches = new ArrayList<>();
			int numOfNewMatchesFoundInSnapshot = 0;
			for (HashMap<Integer,HashSet<Change>> changesByFile:changes) {
				for (int changeID : changesByFile.keySet()) {

					//System.out.print("\n" + change.getId() + " --> ");
					HashMap<String, IncrementalChange> incrementalChangeHashMap = incUpdatesOnDBpedia.updateGraphByGroupOfChanges(changesByFile.get(changeID), tgfdsByName);
					if (incrementalChangeHashMap == null)
						continue;
					for (String tgfdName : incrementalChangeHashMap.keySet()) {
//					newMatchesSignaturesByTGFD.get(tgfdName).addAll(incrementalChangeHashMap.get(tgfdName).getNewMatches().keySet());
//					removedMatchesSignaturesByTGFD.get(tgfdName).addAll(incrementalChangeHashMap.get(tgfdName).getRemovedMatchesSignatures());
//					matchCollectionHashMap.get(tgfdName).addMatches(currentSnapshotDate, incrementalChangeHashMap.get(tgfdName).getNewMatches());
						for (GraphMapping<Vertex, RelationshipEdge> mapping : incrementalChangeHashMap.get(tgfdName).getNewMatches().values()) {
							numOfNewMatchesFoundInSnapshot++;
							HashSet<ConstantLiteral> match = new HashSet<>();
							extractMatch(mapping, patternTreeNode, match, entityURIs);
							if (match.size() <= patternTreeNode.getGraph().vertexSet().size()) continue;
							newMatches.add(match);
						}

						for (GraphMapping<Vertex, RelationshipEdge> mapping : incrementalChangeHashMap.get(tgfdName).getRemovedMatches().values()) {
							HashSet<ConstantLiteral> match = new HashSet<>();
							extractMatch(mapping, patternTreeNode, match, entityURIs);
							if (match.size() <= patternTreeNode.getGraph().vertexSet().size()) continue;
							removedMatches.add(match);
						}
					}
				}
			}
			System.out.println("Number of new matches found: " + numOfNewMatchesFoundInSnapshot);
			System.out.println("Number of new matches found that contain active attributes: " + newMatches.size());
			System.out.println("Number of removed matched: " + removedMatches.size());

			matchesPerTimestamps.get(i+1).addAll(newMatches);

			int numOfOldMatchesFoundInSnapshot = 0;
			for (HashSet<ConstantLiteral> previousMatch : matchesPerTimestamps.get(i)) {
//				if(removedMatches.contains(previousMatch) || newMatches.contains(previousMatch)) { /*previousMatch not in newMatches && previousMatch not in removedMatches*/
//					continue;
//				}
				boolean skip = false;
				for (HashSet<ConstantLiteral> removedMatch : removedMatches) {
//					if (removedMatch.equals(previousMatch)) {
					if (equalsLiteral(removedMatch, previousMatch)) {
						skip = true;
					}
				}
				if (skip) continue;
				for (HashSet<ConstantLiteral> newMatch : newMatches) {
//					if (newMatch.equals(previousMatch)) {
					if (equalsLiteral(newMatch, previousMatch)) {
						skip = true;
					}
				}
				if (skip) continue;
				matchesPerTimestamps.get(i+1).add(previousMatch);
				numOfOldMatchesFoundInSnapshot++;
			}
			System.out.println("Number of valid old matches that are not new or removed: " + numOfOldMatchesFoundInSnapshot);
			System.out.println("Total number of matches with active attributes found in this snapshot: " + matchesPerTimestamps.get(i+1).size());

			numberOfMatchesFound += matchesPerTimestamps.get(i+1).size();

			matchesPerTimestamps.get(i+1).sort(new Comparator<HashSet<ConstantLiteral>>() {
				@Override
				public int compare(HashSet<ConstantLiteral> o1, HashSet<ConstantLiteral> o2) {
					return o1.size() - o2.size();
				}
			});
//			for (TGFD tgfd : tgfds) {
//				matchCollectionHashMap.get(tgfd.getName()).addTimestamp(currentSnapshotDate,
//						newMatchesSignaturesByTGFD.get(tgfd.getName()), removedMatchesSignaturesByTGFD.get(tgfd.getName()));
//				System.out.println("New matches (" + tgfd.getName() + "): " +
//						newMatchesSignaturesByTGFD.get(tgfd.getName()).size() + " ** " + removedMatchesSignaturesByTGFD.get(tgfd.getName()).size());
//			}
			printWithTime("Update and retrieve matches", System.currentTimeMillis()-startTime);
			//myConsole.print("#new matches: " + newMatchesSignatures.size()  + " - #removed matches: " + removedMatchesSignatures.size());
		}

		System.out.println("-------------------------------------");
		System.out.println("Total number of matches found in all snapshots: " + numberOfMatchesFound);
		calculatePatternSupport(entityURIs.size(), patternTreeNode);
	}

	private boolean equalsLiteral(HashSet<ConstantLiteral> match1, HashSet<ConstantLiteral> match2) {
		HashSet<String> uris1 = new HashSet<>();
		for (ConstantLiteral match1Attr : match1) {
			if (match1Attr.getAttrName().equals("uri")) {
				uris1.add(match1Attr.getAttrValue());
			}
		}
		HashSet<String> uris2 = new HashSet<>();
		for (ConstantLiteral match2Attr: match2) {
			if (match2Attr.getAttrName().equals("uri")) {
				uris2.add(match2Attr.getAttrValue());
			}
		}
		return uris1.equals(uris2);
	}

	public static void printWithTime(String message, long runTimeInMS)
	{
		System.out.println(message + " time: " + runTimeInMS + "(ms) ** " +
				TimeUnit.MILLISECONDS.toSeconds(runTimeInMS) + "(sec) ** " +
				TimeUnit.MILLISECONDS.toMinutes(runTimeInMS) +  "(min)");
	}
}

