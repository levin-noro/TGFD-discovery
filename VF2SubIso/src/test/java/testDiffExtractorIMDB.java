import Infra.TGFD;
import TgfdDiscovery.TgfdDiscovery;
import changeExploration.*;
import com.fasterxml.jackson.databind.ObjectMapper;
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

public class testDiffExtractorIMDB {

    public static void main(String[] args) throws FileNotFoundException {

        Options options = TgfdDiscovery.initializeCmdOptions();
        options.addOption("type", false,"generate changefiles based on frequent types");
        CommandLine cmd = TgfdDiscovery.parseArgs(options, args);

        String path = null;
        if (cmd.hasOption("path")) {
            path = cmd.getOptionValue("path").replaceFirst("^~", System.getProperty("user.home"));
            if (!Files.isDirectory(Path.of(path))) {
                System.out.println(Path.of(path) + " is not a valid directory.");
                return;
            }
        }
        boolean basedOnType = cmd.hasOption("type");

        assert path != null;

        TgfdDiscovery tgfdDiscovery = new TgfdDiscovery();
        tgfdDiscovery.setImdbTimestampToFilesMapFromPath(path);
        tgfdDiscovery.setLoader("imdb");
        tgfdDiscovery.setStoreInMemory(false);

        tgfdDiscovery.loadGraphsAndComputeHistogram(tgfdDiscovery.getTimestampToFilesMap());

        ArrayList<TGFD> dummyTGFDs = tgfdDiscovery.getDummyVertexTypeTGFDs();

        System.out.println("Searching for IMDB snapshots in path: "+path);
        List<File> allFilesInDirectory = new ArrayList<>(List.of(Objects.requireNonNull(new File(path).listFiles(File::isFile))));
        System.out.println("Found files: "+allFilesInDirectory);
//		List<File> ntFilesInDirectory = allFilesInDirectory.stream().filter(file -> file.getName().endsWith("\\.nt")).sorted(Comparator.comparing(File::getName)).collect(Collectors.toList());
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
        List<String> timestamps = new ArrayList<>();
        for (File ntFile: ntFilesInDirectory) {
            String regex = "^imdb-([0-9]{2})([0-9]{2})([0-9]{2})\\.nt$";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = pattern.matcher(ntFile.getName());
            if (matcher.find()) {
                String timestamp = "20"+matcher.group(1)+'-'+matcher.group(2)+'-'+matcher.group(3);
                timestamps.add(timestamp);
            }
        }
        timestamps.sort(String::compareTo);
        System.out.println("Timestamps: "+timestamps);
        StringBuilder str = new StringBuilder();
        int index = 1;
        for (String timestamp: timestamps) {
            List<String> paths = new ArrayList<>();
            for (File ntFile : ntFilesInDirectory) {
                String fileTimestamp = timestamp.substring(2).replaceAll("-","");
                if (ntFile.getName().contains(fileTimestamp)) {
                    paths.add(ntFile.getPath());
                }
            }
            str.append("-s" + index + " " + timestamp + "\n");
            System.out.println("Paths: " + paths);
            for (String filepath: paths) {
                if (filepath.contains("types")) {
                    str.append("-t" + index + " " + filepath + "\n");
                } else {
                    str.append("-d" + index + " " + filepath + "\n");
                }
            }
            index++;
        }
        str.append("-logcap ");
        ArrayList<Integer> logcaps = new ArrayList<>();
        for (String ignored : timestamps) {
            logcaps.add(1);
        }
        str.append(logcaps.stream().map(String::valueOf).collect(Collectors.joining(",")));

        try {
            BufferedWriter writer = new BufferedWriter(new FileWriter("conf.txt"));
            writer.write(str.toString());
            writer.close();
        } catch (IOException e) {
            e.printStackTrace();
        }

        System.out.println("Test extract diffs over DBPedia graph");

        Config.parse("conf.txt");

        System.out.println(Config.getAllDataPaths().keySet() + " *** " + Config.getAllDataPaths().values());
        System.out.println(Config.getAllTypesPaths().keySet() + " *** " + Config.getAllTypesPaths().values());

        System.out.println("Generating the diff files for snapshots in: " + path);
        System.out.println("Diff files:");
        System.out.println(Config.getAllDataPaths());
        Config.optimizedLoadingBasedOnTGFD = true;
        if (basedOnType) {
            for (TGFD dummyTGFD : dummyTGFDs) {
                String tgfdName = dummyTGFD.getName();
                System.out.println("===========Dummy TGFD (" + tgfdName + ")===========");
                String fileNameSuffix = tgfdName.replaceAll(" ", "_");
                List<TGFD> alltgfd = Collections.singletonList(dummyTGFD);

                generateChangeFiles(fileNameSuffix, alltgfd);
            }
        } else {
            generateChangeFiles(tgfdDiscovery.getGraphSize(), dummyTGFDs);
        }
    }

    private static void generateChangeFiles(String fileNameSuffix, List<TGFD> alltgfd) {
        final long dummyTgfdChangeFileGenerationTime = System.currentTimeMillis();
        Object[] ids= Config.getAllDataPaths().keySet().toArray();
        Arrays.sort(ids);

        GraphLoader first, second = null;
        List<Change> allChanges;
        int t1, t2 = 0;
        for (int i = 0; i < ids.length; i += 2) {

            System.out.println("===========Snapshot (" + ids[i] + ")===========");
            long startTime = System.currentTimeMillis();

            t1 = (int) ids[i];
            first = new IMDBLoader(alltgfd, Config.getAllDataPaths().get((int) ids[i]));

            printWithTime("Load graph " + ids[i] + " (" + Config.getTimestamps().get(ids[i]) + ")", System.currentTimeMillis() - startTime);

            if (second != null) {
                ChangeFinder cFinder = new ChangeFinder(second, first, alltgfd);
                allChanges = cFinder.findAllChanged();
                analyzeChanges(allChanges, alltgfd, second.getGraphSize(), cFinder.getNumberOfEffectiveChanges(), t2, t1, fileNameSuffix, Config.getDiffCaps());
            }

            if (i + 1 >= ids.length)
                break;

            System.out.println("===========Snapshot (" + ids[i + 1] + ")===========");
            startTime = System.currentTimeMillis();

            t2 = (int) ids[i + 1];
            second = new IMDBLoader(alltgfd, Config.getAllDataPaths().get((int) ids[i + 1]));

            printWithTime("Load graph " + ids[i + 1] + " (" + Config.getTimestamps().get(ids[i + 1]) + ")", System.currentTimeMillis() - startTime);

            ChangeFinder cFinder = new ChangeFinder(first, second, alltgfd);
            allChanges = cFinder.findAllChanged();
            analyzeChanges(allChanges, alltgfd, first.getGraphSize(), cFinder.getNumberOfEffectiveChanges(), t1, t2, fileNameSuffix, Config.getDiffCaps());

        }
        printWithTime("Changefile generation time for one edge", (System.currentTimeMillis() - dummyTgfdChangeFileGenerationTime));
    }

    private static void analyzeChanges(List<Change> allChanges, List<TGFD> allTGFDs, int graphSize,
                                       int changeSize, int timestamp1, int timestamp2, String TGFDsName, ArrayList<Double> diffCaps) {
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
                saveChanges(allChanges, timestamp1, timestamp2, TGFDsName);
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

    private static void saveChanges(List<Change> allChanges, int t1, int t2, String tgfdName) {

        System.out.println("Number of changes: " + allChanges.size());

        System.out.println("Printing the changes: " + t1 + " -> " + t2);

        HashMap<ChangeType, Integer> map = new HashMap<>();
        map.put(ChangeType.deleteAttr, 1);
        map.put(ChangeType.insertAttr, 2);
        map.put(ChangeType.changeAttr, 2);
        map.put(ChangeType.deleteEdge, 3);
        map.put(ChangeType.insertEdge, 4);
        map.put(ChangeType.deleteVertex, 5);
        map.put(ChangeType.insertVertex, 5);
        allChanges.sort(new Comparator<Change>() {
            @Override
            public int compare(Change o1, Change o2) {
                return map.get(o1.getTypeOfChange()).compareTo(map.get(o2.getTypeOfChange()));
            }
        });
        try {
            FileWriter file = new FileWriter("./changes_t" + t1 + "_t" + t2 + "_" + tgfdName + ".json");
            file.write("[");
            for (int index = 0; index < allChanges.size(); index++) {
                Change change = allChanges.get(index);
                final StringWriter sw = new StringWriter();
                final ObjectMapper mapper = new ObjectMapper();
                mapper.writeValue(sw, change);
                file.write(sw.toString());
                if (index < allChanges.size() - 1) {
                    file.write(",");
                }
                sw.close();
            }
            file.write("]");
            System.out.println("Successfully wrote to the file.");
            file.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
        printStatistics(allChanges);
    }
}
