import Infra.*;
import TgfdDiscovery.TgfdDiscovery;

import java.util.ArrayList;
import java.util.HashSet;

public class testSubgraph {
    public static void main(String[] args) {

        TgfdDiscovery tgfdDiscovery = new TgfdDiscovery(args);

        tgfdDiscovery.setDBpediaTimestampsAndFilePaths(tgfdDiscovery.getPath());
        tgfdDiscovery.loadGraphsAndComputeHistogram(tgfdDiscovery.getTimestampToFilesMap());

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
        PatternTreeNode patternTreeNode = new PatternTreeNode(patternGraph);

        System.out.println();
        System.out.println(patternTreeNode.getPattern().getCenterVertexType());
        System.out.println(patternTreeNode.getPattern().getDiameter());
        tgfdDiscovery.findMatchesUsingCenterVertices(tgfdDiscovery.getGraphs(), patternTreeNode, matchesPerTimestamps);

        matchesPerTimestamps = new ArrayList<>();
        for (int timestamp = 0; timestamp < tgfdDiscovery.getNumOfSnapshots(); timestamp++) {
            matchesPerTimestamps.add(new ArrayList<>());
        }
        patternTreeNode.getPattern().centerVertexType = "soccerclub";
        patternTreeNode.getPattern().setDiameter(2);
        System.out.println();
        System.out.println(patternTreeNode.getPattern().getCenterVertexType());
        System.out.println(patternTreeNode.getPattern().getDiameter());
        tgfdDiscovery.findMatchesUsingCenterVertices(tgfdDiscovery.getGraphs(), patternTreeNode, matchesPerTimestamps);

        matchesPerTimestamps = new ArrayList<>();
        for (int timestamp = 0; timestamp < tgfdDiscovery.getNumOfSnapshots(); timestamp++) {
            matchesPerTimestamps.add(new ArrayList<>());
        }
        System.out.println();
        tgfdDiscovery.getMatchesForPattern(tgfdDiscovery.getGraphs(), patternTreeNode, matchesPerTimestamps);
    }

}
