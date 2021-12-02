import Infra.*;
import TgfdDiscovery.TgfdDiscovery;
import graphLoader.GraphLoader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;

import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;

public class testSubgraph {
    public static void main(String[] args) {
        Options options = TgfdDiscovery.initializeCmdOptions();
        CommandLine cmd = TgfdDiscovery.parseArgs(options, args);

        String timeAndDateStamp = ZonedDateTime.now(ZoneId.systemDefault()).format(DateTimeFormatter.ofPattern("uuuu.MM.dd.HH.mm.ss"));

        String experimentName = null;
        if (cmd.hasOption("name")) {
            experimentName = cmd.getOptionValue("name");
        } else  {
            experimentName = "experiment";
        }

        boolean noMinimalityPruning = cmd.hasOption("noMinimalityPruning");
        boolean noSupportPruning = cmd.hasOption("noSupportPruning");
        boolean dontSortHistogram = cmd.hasOption("dontSortHistogram");
        boolean interestingTGFDs = cmd.hasOption("interesting");
        boolean useChangeFile = cmd.hasOption("changefile");
        boolean useSubgraph = cmd.hasOption("subgraph");
        boolean generatek0Tgfds = cmd.hasOption("k0");
        boolean skipK1 = cmd.hasOption("skipK1");

        String graphSize = null;
        if (cmd.getOptionValue("g") != null) {
            graphSize = cmd.getOptionValue("g");
        }
        int gamma = cmd.getOptionValue("a") == null ? TgfdDiscovery.DEFAULT_GAMMA : Integer.parseInt(cmd.getOptionValue("a"));
        double theta = cmd.getOptionValue("theta") == null ? TgfdDiscovery.DEFAULT_THETA : Double.parseDouble(cmd.getOptionValue("theta"));
        int k = cmd.getOptionValue("k") == null ? TgfdDiscovery.DEFAULT_K : Integer.parseInt(cmd.getOptionValue("k"));
        double patternSupportThreshold = cmd.getOptionValue("p") == null ? TgfdDiscovery.DEFAULT_PATTERN_SUPPORT_THRESHOLD : Double.parseDouble(cmd.getOptionValue("p"));

        TgfdDiscovery tgfdDiscovery = new TgfdDiscovery(experimentName, k, theta, gamma, graphSize, patternSupportThreshold, noMinimalityPruning, interestingTGFDs, useChangeFile, noSupportPruning, dontSortHistogram, useSubgraph, generatek0Tgfds, skipK1);

        ArrayList<GraphLoader> graphs = tgfdDiscovery.loadDBpediaSnapshotsFromPath(cmd.getOptionValue("path"));
        tgfdDiscovery.histogram(graphs);

        ArrayList<ArrayList<HashSet<ConstantLiteral>>> matchesPerTimestamps = new ArrayList<>();
        for (int timestamp = 0; timestamp < tgfdDiscovery.getNumOfSnapshots(); timestamp++) {
            matchesPerTimestamps.add(new ArrayList<>());
        }
        VF2PatternGraph patternGraph = new VF2PatternGraph();
        PatternVertex soccerplayer = new PatternVertex("soccerplayer");
        patternGraph.addVertex(soccerplayer);
        PatternVertex soccerclub = new PatternVertex("soccerclub");
        patternGraph.addVertex(soccerclub);
        PatternVertex sportsteammember = new PatternVertex("sportsteammember");
        patternGraph.addVertex(sportsteammember);
        PatternVertex careerstation = new PatternVertex("careerstation");
        patternGraph.addVertex(careerstation);
        RelationshipEdge e1 = new RelationshipEdge("team");
        RelationshipEdge e2 = new RelationshipEdge("team");
        RelationshipEdge e3 = new RelationshipEdge("team");
        patternGraph.addEdge(soccerplayer, soccerclub, e1);
        patternGraph.addEdge(sportsteammember, soccerclub, e2);
        patternGraph.addEdge(careerstation, soccerclub, e3);
        PatternTreeNode patternTreeNode = new PatternTreeNode(patternGraph, 1.0);

        System.out.println();
        System.out.println(patternTreeNode.getPattern().getCenterVertexType());
        System.out.println(patternTreeNode.getPattern().getDiameter());
        tgfdDiscovery.getMatchesUsingCenterVertices(graphs, patternTreeNode, matchesPerTimestamps);

        matchesPerTimestamps = new ArrayList<>();
        for (int timestamp = 0; timestamp < tgfdDiscovery.getNumOfSnapshots(); timestamp++) {
            matchesPerTimestamps.add(new ArrayList<>());
        }
        patternTreeNode.getPattern().centerVertexType = "soccerclub";
        patternTreeNode.getPattern().setDiameter(2);
        System.out.println();
        System.out.println(patternTreeNode.getPattern().getCenterVertexType());
        System.out.println(patternTreeNode.getPattern().getDiameter());
        tgfdDiscovery.getMatchesUsingCenterVertices(graphs, patternTreeNode, matchesPerTimestamps);

        matchesPerTimestamps = new ArrayList<>();
        for (int timestamp = 0; timestamp < tgfdDiscovery.getNumOfSnapshots(); timestamp++) {
            matchesPerTimestamps.add(new ArrayList<>());
        }
        System.out.println();
        tgfdDiscovery.getMatchesForPattern(graphs, patternTreeNode, matchesPerTimestamps);
    }

}
