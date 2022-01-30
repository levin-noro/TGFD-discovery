import graphLoader.DBPediaLoader;
import graphLoader.IMDBLoader;
import org.apache.commons.cli.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;

import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static TgfdDiscovery.TgfdDiscovery.generateDbpediaTimestampToFilesMap;
import static TgfdDiscovery.TgfdDiscovery.generateImdbTimestampToFilesMapFromPath;

public class generateCustomImdb {

    public static final String TYPES = "types";
    public static final String LITERALS = "literals";
    public static final String OBJECTS = "objects";
    public static final double LOWER_THRESHOLD = 0.05;
    public static final double UPPER_THRESHOLD = 0.1;

    public static void main(String[] args) {
        Options options = new Options();
        options.addOption("path", true, "generate graphs using files from specified path");
        options.addOption("type", true, "generate graphs using type");
        options.addOption("count", true, "generate graphs based on vertex count");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        assert cmd != null;
        String path = null;
        if (cmd.hasOption("path")) {
            path = cmd.getOptionValue("path").replaceFirst("^~", System.getProperty("user.home"));
            if (!Files.isDirectory(Path.of(path))) {
                System.out.println(Path.of(path) + " is not a valid directory.");
                return;
            }
        }
        List<Map.Entry<String, List<String>>> timestampToFilesMap = new ArrayList<>(generateImdbTimestampToFilesMapFromPath(path).entrySet());
        timestampToFilesMap.sort(Map.Entry.comparingByKey());
//        if (cmd.hasOption("type")) {
//            String[] numOfTypes = cmd.getOptionValue("type").split(",");
//            generateCustomDBpediaBasedOnType(numOfTypes);
//        }
        if (cmd.hasOption("count")) {
            String[] sizes = cmd.getOptionValue("count").split(",");
            generateCustomDBpediaBasedOnSize2(sizes, timestampToFilesMap);
        }
    }

    public static void generateCustomDBpediaBasedOnSize2(String[] args, List<Map.Entry<String, List<String>>> timestampToFilesMap) {
        long[] sizes = new long[args.length];
        for (int index = 0; index < sizes.length; index++) {
            sizes[index] = Long.parseLong(args[index]);
        }

        for (Map.Entry<String, List<String>> timestampToFilesEntry : timestampToFilesMap) {
            Model model = ModelFactory.createDefaultModel();
            String filePath = "";
            for (String filename : timestampToFilesEntry.getValue()) {
                filePath = filename;
            }
            Map<String, Set<Statement>> vertexURIToStmtsMap = new HashMap<>();
            Map<String, Set<String>> vertexURIToTypesMap = new HashMap<>();
            Map<String, Set<Statement>> edgeTypeToStmtsMap = new HashMap<>();
            for (int j = 0; j < 2; j++) {
//                final String filePath = path + "/201" + (i + 5) + "/201" + (i + 5) + fileType + ".ttl";
                System.out.print("Processing " + filePath + ". ");
                Path input = Paths.get(filePath);
                model.read(input.toAbsolutePath().toString());
                StmtIterator stmtIterator = model.listStatements();
                switch (j) {
                    case 0 -> {
                        while (stmtIterator.hasNext()) {
                            Statement stmt = stmtIterator.nextStatement();
                            String subjectNodeURI = stmt.getSubject().getURI().toLowerCase();
                            if (subjectNodeURI.length() > 16) {
                                subjectNodeURI = subjectNodeURI.substring(16);
                            }

                            var temp = subjectNodeURI.split("/");
                            if (temp.length != 2) {
                                // Error!
                                continue;
                            }
                            String subjectType = temp[0];
                            vertexURIToTypesMap.putIfAbsent(subjectNodeURI, new HashSet<>());
                            vertexURIToTypesMap.get(subjectNodeURI).add(subjectType);
                            vertexURIToStmtsMap.putIfAbsent(subjectNodeURI, new HashSet<>());
                            vertexURIToStmtsMap.get(subjectNodeURI).add(stmt);
                            if (stmt.getObject().isLiteral()) continue;
                            String objectNodeURI = stmt.getObject().asResource().getURI().toLowerCase();
                            if (objectNodeURI.length() > 16) {
                                objectNodeURI = objectNodeURI.substring(16);
                            }

                            temp = objectNodeURI.split("/");
                            if (temp.length != 2) {
                                // Error!
                                continue;
                            }
                            String objectType = temp[0];
                            vertexURIToTypesMap.putIfAbsent(objectNodeURI, new HashSet<>());
                            vertexURIToTypesMap.get(objectNodeURI).add(objectType);
                        }
                    }
                    case 1 -> {
                        while (stmtIterator.hasNext()) {
                            Statement stmt = stmtIterator.nextStatement();
                            if (!stmt.getObject().isLiteral()) {
                                String subjectNodeURI = stmt.getSubject().getURI().toLowerCase();
                                if (subjectNodeURI.length() > 16) {
                                    subjectNodeURI = subjectNodeURI.substring(16);
                                }
                                Set<String> subjectTypes = vertexURIToTypesMap.get(subjectNodeURI);
                                String objectNodeURI = stmt.getSubject().getURI().toLowerCase();
                                if (objectNodeURI.length() > 16) {
                                    objectNodeURI = objectNodeURI.substring(16);
                                }
                                Set<String> objectTypes = vertexURIToTypesMap.get(objectNodeURI);
                                String predicateName = stmt.getPredicate().getLocalName();
                                if (subjectTypes == null || objectTypes == null) continue;
                                for (String subjectType : subjectTypes) {
                                    for (String objectType : objectTypes) {
                                        String edgeType = String.join(",", Arrays.asList(subjectType, predicateName, objectType));
                                        edgeTypeToStmtsMap.putIfAbsent(edgeType, new HashSet<>());
                                        edgeTypeToStmtsMap.get(edgeType).add(stmt);
                                    }
                                }
                            } else {
                                String subjectURI = stmt.getSubject().getURI().toLowerCase();
                                vertexURIToStmtsMap.putIfAbsent(subjectURI, new HashSet<>());
                                vertexURIToStmtsMap.get(subjectURI).add(stmt);
                            }
                        }
                    }
                }
            }
            System.out.println("Done");
            ArrayList<Map.Entry<String, Set<Statement>>> sortedList = new ArrayList<>(edgeTypeToStmtsMap.entrySet());
            sortedList.sort(new Comparator<Map.Entry<String, Set<Statement>>>() {
                @Override
                public int compare(Map.Entry<String, Set<Statement>> o1, Map.Entry<String, Set<Statement>> o2) {
                    return o1.getValue().size() - o2.getValue().size();
                }
            });
            List<Double> percentagesForThisTimestamp = new ArrayList<>();
            for (long size : sizes) {
                System.out.println("Size: " + size);
                double total = 0;
                double percent = 0.00;
                double increment = 0.01;
                while ((total < (size - (size * LOWER_THRESHOLD)) || total > (size + (size * UPPER_THRESHOLD))) && percent <= 1.0) {
                    if (total < (size - (size * LOWER_THRESHOLD))) {
                        System.out.println("Not enough edges");
                        percent += increment;
                    } else if (total > (size + (size * UPPER_THRESHOLD))) {
                        System.out.println("Too many edges");
                        percent -= increment;
                        increment /= 10;
                        percent += increment;
                    }
//                    percent += 0.01;
                    System.out.println("Trying percent: " + percent);
                    total = 0;
                    for (Map.Entry<String, Set<Statement>> entry : sortedList) {
                        Iterator<Statement> stmtIterator = entry.getValue().iterator();
                        int singleEdgeTypeCount = 0;
                        while (singleEdgeTypeCount + 1 < (entry.getValue().size() * percent) && stmtIterator.hasNext()) {
                            stmtIterator.next();
                            singleEdgeTypeCount++;
                        }
//                        total += (entry.getValue().size() * percent);
                        total += singleEdgeTypeCount;
                    }
                    System.out.println("Total: " + total);
                }
                percentagesForThisTimestamp.add(percent);
            }
            System.out.println(percentagesForThisTimestamp);
            for (int j = 0; j < sizes.length; j++) {
                long size = sizes[j];
                String directoryStructure = "imdb-" + size + "/";
                String newFileName = directoryStructure + "imdb-" + timestampToFilesEntry.getKey() + ".nt";
                System.out.println("Creating model for " + newFileName);
                double percentage = percentagesForThisTimestamp.get(j);
                Model newModel = ModelFactory.createDefaultModel();
                int totalEdgeCount = 0;
                int totalAttributeCount = 0;
                for (Map.Entry<String, Set<Statement>> edgeTypeToStmtsMapEntry : edgeTypeToStmtsMap.entrySet()) {
                    Iterator<Statement> stmtIterator = edgeTypeToStmtsMapEntry.getValue().iterator();
                    int singleEdgeTypeCount = 0;
                    while (singleEdgeTypeCount + 1 < (edgeTypeToStmtsMapEntry.getValue().size() * percentage) && stmtIterator.hasNext()) {
                        Statement stmt = stmtIterator.next();
                        String subjectURI = stmt.getSubject().getURI().toLowerCase();
                        String objectURI = stmt.getObject().asResource().getURI().toLowerCase();
                        newModel.add(stmt);
                        if (vertexURIToStmtsMap.containsKey(subjectURI)) {
                            Set<Statement> subjectStmts = vertexURIToStmtsMap.get(subjectURI);
                            for (Statement subjStmt : subjectStmts) {
                                if (subjStmt.getObject().isLiteral() || subjStmt.getPredicate().getLocalName().contains("type")) {
                                    newModel.add(subjStmt);
                                    if (!subjStmt.getPredicate().getLocalName().contains("type")) {
                                        totalAttributeCount++;
                                    }
                                }
                            }
                        }
                        if (vertexURIToStmtsMap.containsKey(objectURI)) {
                            Set<Statement> objectStmts = vertexURIToStmtsMap.get(objectURI);
                            for (Statement objStmt : objectStmts) {
                                if (objStmt.getObject().isLiteral() || objStmt.getPredicate().getLocalName().contains("type")) {
                                    newModel.add(objStmt);
                                    if (!objStmt.getPredicate().getLocalName().contains("type")) {
                                        totalAttributeCount++;
                                    }
                                }
                            }
                        }
                        singleEdgeTypeCount++;
                    }
                    totalEdgeCount += singleEdgeTypeCount;
                }
                System.out.println("Edge count = " + totalEdgeCount);
                System.out.println("Attribute count = " + totalAttributeCount);
                try {
                    System.out.print("Writing to " + newFileName + ". ");
                    Files.createDirectories(Paths.get(directoryStructure));
                    newModel.write(new PrintStream(newFileName), "N-TRIPLE");
                    System.out.println("Done.");
                    new IMDBLoader(new ArrayList<>(), Collections.singletonList(newModel));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
