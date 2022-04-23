import Infra.TGFD;
import Infra.VF2DataGraph;
import Infra.Vertex;
import TgfdDiscovery.TgfdDiscovery;
import changeExploration.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import graphLoader.DBPediaLoader;
import graphLoader.GraphLoader;
import graphLoader.IMDBLoader;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import util.Config;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class testDiffExtractor {

    public static void main(String[] args) throws FileNotFoundException {

        Options options = new Options();
        options.addOption("loader", true, "type of graph loader to use");
        options.addOption("path", true, "location of files");
        options.addOption("t", true, "number of snapshots to consider");
        options.addOption("percent", true, "percentage of changes to keep");
        options.addOption("type", false,"generate changefiles based on frequent types");
        options.addOption("simplifySuperVertex", true, "simplify vertices that have an in-degree greater than the specified value");
        CommandLine cmd = TgfdDiscovery.parseArgs(options, args);

        String loader;
        if (cmd.hasOption("loader"))
            loader = cmd.getOptionValue("loader");
        else
            throw new IllegalArgumentException("loader not specified");

        String path = null;
        if (cmd.hasOption("path")) {
            path = cmd.getOptionValue("path").replaceFirst("^~", System.getProperty("user.home"));
            if (!Files.isDirectory(Path.of(path)))
                throw new IllegalArgumentException(Path.of(path) + " is not a valid directory.");
        }
        boolean basedOnType = cmd.hasOption("type");

        double percent;
        if (cmd.hasOption("percent"))
            percent = Double.parseDouble(cmd.getOptionValue("percent"));
        else
            throw new IllegalArgumentException("percent not specified");

        Integer superVertexDegree = null;
        if (cmd.hasOption("simplifySuperVertex"))
            superVertexDegree = Integer.parseInt(cmd.getOptionValue("simplifySuperVertex"));

        Map<String, List<String>> timestampToFilesMap = new HashMap<>();
        switch(loader) {
            case "dbpedia" -> timestampToFilesMap = TgfdDiscovery.generateDbpediaTimestampToFilesMap(path);
            case "imdb" -> timestampToFilesMap = TgfdDiscovery.generateImdbTimestampToFilesMapFromPath(path);
            default -> throw new IllegalArgumentException("unsupported loader: "+loader);
        }

        ArrayList<Integer> logcaps = new ArrayList<>();
        List<Map.Entry<String,List<String>>> timestampsToFilesMap = new ArrayList<>(timestampToFilesMap.entrySet());
        timestampsToFilesMap.sort(Map.Entry.comparingByKey());
        System.out.println("Timestamps: "+timestampsToFilesMap);
        StringBuilder str = new StringBuilder();
        int index = 1;
        for (Map.Entry<String, List<String>> timestampEntry: timestampsToFilesMap) {
            String timestamp = null;
            switch (loader) {
                case "imdb" -> {
                    String regex = "^imdb-([0-9]{2})([0-9]{2})([0-9]{2})\\.nt$";
                    Pattern pattern = Pattern.compile(regex);
                    Matcher matcher = pattern.matcher(timestampEntry.getKey());
                    if (matcher.find()) {
                        timestamp = "20"+matcher.group(1)+'-'+matcher.group(2)+'-'+matcher.group(3);
                    }
                }
                case "dbpedia" -> timestamp = timestampEntry.getKey()+"-01-01";
                default -> throw new IllegalArgumentException("unsupported loader: "+loader);
            }
            str.append("-s" + index + " " + timestamp + "\n");
            System.out.println("Paths: " + timestampEntry.getValue());
            if (timestampEntry.getValue().size() > 1) {
                for (String filepath: timestampEntry.getValue()) {
                    if (filepath.contains("types")) {
                        str.append("-t" + index + " " + filepath + "\n");
                    } else {
                        str.append("-d" + index + " " + filepath + "\n");
                    }
                }
            } else {
                str.append("-t" + index + " " + timestampEntry.getValue().get(0) + "\n");
                str.append("-d" + index + " " + timestampEntry.getValue().get(0) + "\n");
            }
            index++;
            logcaps.add(1);
        }
        str.append("-logcap ");
        str.append(logcaps.stream().map(String::valueOf).collect(Collectors.joining(",")));

        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("conf.txt"));
            writer.write(str.toString());
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Test extract diffs over graph");

        Config.parse("conf.txt");

        System.out.println(Config.getAllDataPaths().keySet() + " *** " + Config.getAllDataPaths().values());
        System.out.println(Config.getAllTypesPaths().keySet() + " *** " + Config.getAllTypesPaths().values());

        System.out.println("Generating the diff files for snapshots in: " + path);
        System.out.println("Diff files:");
        System.out.println(Config.getAllDataPaths());
        generateChangeFiles(Path.of(path).getFileName().toString(), loader, null, percent, basedOnType, superVertexDegree);
    }

    private static void generateChangeFiles(String fileNameSuffix, String loader, List<TGFD> alltgfd, double percent, boolean basedOnType, Integer superVertexDegree) {
        final long dummyTgfdChangeFileGenerationTime = System.currentTimeMillis();
        Object[] ids= Config.getAllDataPaths().keySet().toArray();
        Arrays.sort(ids);

        GraphLoader first;
        GraphLoader second = null;
        List<Change> allChanges;
        int t1, t2 = 0;
        for (int i = 0; i < ids.length; i += 2) {

            System.out.println("===========Snapshot (" + ids[i] + ")===========");
            long startTime = System.currentTimeMillis();

            t1 = (int) ids[i];
            if (alltgfd != null)
                Config.optimizedLoadingBasedOnTGFD = true;

            switch(loader) {
                case "dbpedia" -> first = new DBPediaLoader(alltgfd, Config.getAllTypesPaths().get((int) ids[i]), Config.getAllDataPaths().get((int) ids[i]));
                case "imdb" -> first = new IMDBLoader(alltgfd, Config.getAllDataPaths().get((int) ids[i]));
                default -> throw new IllegalArgumentException("unsupported loader: "+loader);
            }

            if (alltgfd != null)
                Config.optimizedLoadingBasedOnTGFD = false;
            if (superVertexDegree != null)
                TgfdDiscovery.dissolveSuperVerticesBasedOnCount(first, superVertexDegree);

            printWithTime("Load graph " + ids[i] + " (" + Config.getTimestamps().get(ids[i]) + ")", System.currentTimeMillis() - startTime);

            if (second != null) {
                long changeDiscoveryTime = System.currentTimeMillis();
                ChangeFinder cFinder = new ChangeFinder(second, first, new ArrayList<>());
                allChanges = cFinder.findAllChanged();
                if (basedOnType) {
                    Set<String> types = new HashSet<>();
                    types.addAll(first.getTypes());
                    types.addAll(second.getTypes());
                    for (String type: types) {
                        List<Change> changesForThisType = allChanges.stream().filter(change -> change.getTypes().stream().anyMatch(s -> s.equals(type))).collect(Collectors.toList());
                        analyzeChanges(changesForThisType, new ArrayList<>(), second.getGraphSize(), cFinder.getNumberOfEffectiveChanges(), t2, t1, fileNameSuffix+'-'+type, Config.getDiffCaps(), percent);
                    }
                } else {
                    analyzeChanges(allChanges, new ArrayList<>(), second.getGraphSize(), cFinder.getNumberOfEffectiveChanges(), t2, t1, fileNameSuffix, Config.getDiffCaps(), percent);
                }
                printWithTime("Change discovery " + ids[i] + " (" + Config.getTimestamps().get(ids[i]) + ")", System.currentTimeMillis() - changeDiscoveryTime);
            }

            if (i + 1 >= ids.length)
                break;

            System.out.println("===========Snapshot (" + ids[i + 1] + ")===========");
            startTime = System.currentTimeMillis();

            t2 = (int) ids[i + 1];
            if (alltgfd != null)
                Config.optimizedLoadingBasedOnTGFD = true;
            switch(loader) {
                case "dbpedia" -> second = new DBPediaLoader(alltgfd, Config.getAllTypesPaths().get((int) ids[i+1]), Config.getAllDataPaths().get((int) ids[i+1]));
                case "imdb" -> second = new IMDBLoader(alltgfd, Config.getAllDataPaths().get((int) ids[i+1]));
                default -> throw new IllegalArgumentException("unsupported loader: "+loader);
            }
            if (alltgfd != null)
                Config.optimizedLoadingBasedOnTGFD = false;
            if (superVertexDegree != null)
                TgfdDiscovery.dissolveSuperVerticesBasedOnCount(second, superVertexDegree);

            printWithTime("Load graph " + ids[i + 1] + " (" + Config.getTimestamps().get(ids[i + 1]) + ")", System.currentTimeMillis() - startTime);

            long changeDiscoveryTime = System.currentTimeMillis();
            ChangeFinder cFinder = new ChangeFinder(first, second, new ArrayList<>());
            allChanges = cFinder.findAllChanged();
            if (basedOnType) {
                Set<String> types = new HashSet<>();
                types.addAll(first.getTypes());
                types.addAll(second.getTypes());
                for (String type: types) {
                    List<Change> changesForThisType = allChanges.stream().filter(change -> change.getTypes().stream().anyMatch(s -> s.equals(type))).collect(Collectors.toList());
                    analyzeChanges(changesForThisType, new ArrayList<>(), first.getGraphSize(), cFinder.getNumberOfEffectiveChanges(), t1, t2, fileNameSuffix+'-'+type, Config.getDiffCaps(), percent);
                }
            } else {
                analyzeChanges(allChanges, new ArrayList<>(), first.getGraphSize(), cFinder.getNumberOfEffectiveChanges(), t1, t2, fileNameSuffix, Config.getDiffCaps(), percent);
            }
            printWithTime("Load graph " + ids[i + 1] + " (" + Config.getTimestamps().get(ids[i + 1]) + ")", System.currentTimeMillis() - changeDiscoveryTime);
        }
        printWithTime("Changefile generation time for one edge", (System.currentTimeMillis() - dummyTgfdChangeFileGenerationTime));
    }

    private static void removeDisconnectedVertices(VF2DataGraph vf2DataGraph) {
        Set<Vertex> verticesToDelete = new HashSet<>();
        for (Vertex v: vf2DataGraph.getGraph().vertexSet()) {
            if (vf2DataGraph.getGraph().edgesOf(v).size() == 0) {
                verticesToDelete.add(v);
            }
        }
        System.out.println("Number of vertices marked for deletion: " + verticesToDelete.size());
        int numOfVerticesDeleted = 0;
        for (Vertex v: verticesToDelete) {
            if (vf2DataGraph.getGraph().removeVertex(v)) {
                numOfVerticesDeleted++;
            }
        }
        System.out.println("Number of vertices successfully deleted: " + numOfVerticesDeleted);
    }

    private static void analyzeChanges(List<Change> allChanges, List<TGFD> allTGFDs, int graphSize,
                                       int changeSize, int timestamp1, int timestamp2, String TGFDsName, ArrayList<Double> diffCaps, double percent) {
//        ChangeTrimmer trimmer=new ChangeTrimmer(allChanges,allTGFDs);
//        for (double i:diffCaps)
//        {
//            int allowedNumberOfChanges= (int) (i*graphSize);
//            if (allowedNumberOfChanges<changeSize)
//            {
//                List<Change> trimmedChanges=trimmer.trimChanges(allowedNumberOfChanges);
//                saveChanges(trimmedChanges,timestamp1,timestamp2,TGFDsName + "_" + i);
//            }
//            else
//            {
                saveChanges(allChanges, timestamp1, timestamp2, TGFDsName, percent);
//                return;
//            }
//        }
    }

    private static void printWithTime(String message, long runTimeInMS) {
        System.out.println(message + " time: " + runTimeInMS + "(ms) ** " +
                TimeUnit.MILLISECONDS.toSeconds(runTimeInMS) + "(sec) ** " +
                TimeUnit.MILLISECONDS.toMinutes(runTimeInMS) + "(min)");
    }

    private static void printStatistics(List<Change> allChanges) {
        int insertChangeEdge = 0;
        int insertChangeVertex = 0;
        int insertChangeAttribute = 0;
        int deleteChangeEdge = 0;
        int deleteChangeVertex = 0;
        int deleteChangeAttribute = 0;
        int changeAttributeValue = 0;

        for (Change c : allChanges) {
            if (c instanceof EdgeChange) {
                if (c.getTypeOfChange() == ChangeType.deleteEdge)
                    deleteChangeEdge++;
                else if (c.getTypeOfChange() == ChangeType.insertEdge)
                    insertChangeEdge++;
            } else if (c instanceof VertexChange) {
                if (c.getTypeOfChange() == ChangeType.deleteVertex)
                    deleteChangeVertex++;
                else if (c.getTypeOfChange() == ChangeType.insertVertex)
                    insertChangeVertex++;
            } else if (c instanceof AttributeChange) {
                if (c.getTypeOfChange() == ChangeType.deleteAttr)
                    deleteChangeAttribute++;
                else if (c.getTypeOfChange() == ChangeType.insertAttr)
                    insertChangeAttribute++;
                else
                    changeAttributeValue++;
            }
        }
        System.out.println("Total number of changes: " + allChanges.size());
        System.out.println("Edges: +" + insertChangeEdge + " ** -" + deleteChangeEdge);
        System.out.println("Vertices: +" + insertChangeVertex + " ** -" + deleteChangeVertex);
        System.out.println("Attributes: +" + insertChangeAttribute + " ** -" + deleteChangeAttribute + " ** updates: " + changeAttributeValue);
    }

    private static void saveChanges(List<Change> allChanges, int t1, int t2, String tgfdName, double percent) {

        System.out.println("Number of changes: " + allChanges.size());

        int numOfChangesToConsider = (int) (allChanges.size() * percent);
        System.out.println("Number of changes considered: " + numOfChangesToConsider);
        if (percent < 1.0) {
            Collections.shuffle(allChanges);
        }
        List<Change> changesToConsider = allChanges.subList(0, numOfChangesToConsider);

        final long printTime = System.currentTimeMillis();
        System.out.println("Printing the changes: " + t1 + " -> " + t2);

        TgfdDiscovery.sortChanges(changesToConsider);
        try {
            String fileName = "./changes_t" + t1 + "_t" + t2 + "_" + tgfdName + ".json";
            FileWriter file = new FileWriter(fileName);
            file.write("[");
            for (int index = 0; index < changesToConsider.size(); index++) {
                Change change = changesToConsider.get(index);
                final StringWriter sw = new StringWriter();
                final ObjectMapper mapper = new ObjectMapper();
                mapper.writeValue(sw, change);
                file.write(sw.toString());
                if (index < changesToConsider.size() - 1) {
                    file.write(",");
                }
                sw.close();
            }
            file.write("]");
            System.out.println("Successfully wrote to the file "+fileName);
            file.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        printWithTime("Printing", System.currentTimeMillis()-printTime);
        printStatistics(changesToConsider);
    }
}
