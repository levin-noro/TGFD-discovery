import graphLoader.DBPediaLoader;
import org.apache.commons.cli.*;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.*;

import static TgfdDiscovery.TgfdDiscovery.generateDbpediaTimestampToFilesMap;

public class generateCustomDBpedia {

    public static final String TYPES = "types";
    public static final String LITERALS = "literals";
    public static final String OBJECTS = "objects";
    public static final double LOWER_THRESHOLD = 0.01;
    public static final double UPPER_THRESHOLD = 0.01;

    public static void main(String[] args) throws ParseException {
        Options options = new Options();
        options.addOption("path", true, "generate graphs using files from specified path");
        options.addOption("type", true, "generate graphs using type");
        options.addOption("count", true, "generate graphs based on vertex count");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = parser.parse(options, args);

        String path = null;
        if (cmd.hasOption("path")) {
            path = cmd.getOptionValue("path").replaceFirst("^~", System.getProperty("user.home"));
            if (!Files.isDirectory(Path.of(path))) {
                System.out.println(Path.of(path) + " is not a valid directory.");
                return;
            }
        }
        List<Map.Entry<String, List<String>>> timestampToFilesMap = new ArrayList<>(generateDbpediaTimestampToFilesMap(path).entrySet());
        timestampToFilesMap.sort(Map.Entry.comparingByKey());
        if (cmd.hasOption("type")) {
            String[] numOfTypes = cmd.getOptionValue("type").split(",");
            generateCustomDBpediaBasedOnType(numOfTypes);
        }
        if (cmd.hasOption("count")) {
            String[] sizes = cmd.getOptionValue("count").split(",");
            generateCustomDBpediaBasedOnSize2(sizes, timestampToFilesMap);
        }
    }

    public static void generateCustomDBpediaBasedOnType(String[] args) {
        String[] fileTypes = {TYPES, LITERALS, OBJECTS};
        Model[] typesModels = {ModelFactory.createDefaultModel(), ModelFactory.createDefaultModel(), ModelFactory.createDefaultModel()};
        Model[] objectsModels = {ModelFactory.createDefaultModel(), ModelFactory.createDefaultModel(), ModelFactory.createDefaultModel()};
        Model[] literalsModels = {ModelFactory.createDefaultModel(), ModelFactory.createDefaultModel(), ModelFactory.createDefaultModel()};
        int[] sizes = new int[args.length];
        for (int index = 0; index < sizes.length; index++) {
            sizes[index] = Integer.parseInt(args[index]);
        }
        HashMap<String, Integer> vertexTypesHistogram = new HashMap<>();
        HashMap<String, HashSet<String>> vertexMap = new HashMap<>();
        ArrayList<Map.Entry<String, Integer>> sortedVertexTypesHistogram = new ArrayList<>();

        for (int i = 0; i < 3; i++) {
            for (String fileType : fileTypes) {
                String fileName = "dbpedia/201"+ (i+5) + "/201" + (i+5) + fileType + ".ttl";
                System.out.println("Processing " + fileName);
                Path input = Paths.get(fileName);
                switch (fileType) {
                    case TYPES -> typesModels[i].read(input.toUri().toString());
                    case OBJECTS -> objectsModels[i].read(input.toUri().toString());
                    case LITERALS -> literalsModels[i].read(input.toUri().toString());
                }
                System.out.println("Processed " + fileName);
            }
        }

        System.out.println("Computing frequency of vertex types...");
        for (int i = 0; i < 3; i++) {
            System.out.println("Processing 201" + (i+5) + TYPES);
            StmtIterator stmtIterator = typesModels[i].listStatements();
            int counter = 0;
            while (stmtIterator.hasNext()){
                counter++;
                Statement stmt = stmtIterator.nextStatement();
                String vertexName = stmt.getSubject().asResource().getLocalName().toLowerCase();
                String vertexType = stmt.getObject().asResource().getLocalName().toLowerCase();
                if (vertexName.length() > 28) {
                    vertexName = vertexName.substring(28);
                }
                vertexTypesHistogram.merge(vertexType, 1, Integer::sum);
                if (vertexMap.containsKey(vertexName)) {
                    vertexMap.get(vertexName).add(vertexType);
                } else {
                    HashSet<String> vertexTypes = new HashSet<>();
                    vertexTypes.add(vertexType);
                    vertexMap.put(vertexName, vertexTypes);
                }
                if (counter % 100000 == 0) System.out.println("Processed "+counter+" statements for 201" + i + TYPES);
            }
            sortedVertexTypesHistogram = new ArrayList<>(vertexTypesHistogram.entrySet());
            sortedVertexTypesHistogram.sort((o1, o2) -> o2.getValue() - o1.getValue());
        }

        for (int numOfTypes : sizes) {

            List<Map.Entry<String, Integer>> typesEntriesToConsider = sortedVertexTypesHistogram.subList(0, numOfTypes);
            HashSet<String> typesToConsider = new HashSet<>();
            for (Map.Entry<String, Integer> typeEntry : typesEntriesToConsider) {
                typesToConsider.add(typeEntry.getKey());
            }
            System.out.println("Vertex types: "+typesToConsider);

            int G = 0;
            for (String types : typesToConsider) {
                G += vertexTypesHistogram.get(types);
            }
            G = G / 3;
            System.out.println("|G| = "+G);

//            Model[] newObjectsModels = {null, null, null};
            HashSet<String> edgesSet = new HashSet<>();
            for (int i = 0; i < 3; i++) {
                System.out.println("Processing 201" + (i+5) + OBJECTS);
                StmtIterator stmtIterator = objectsModels[i].listStatements();
                Model newModel = ModelFactory.createDefaultModel();
                int counter = 0;
                while (stmtIterator.hasNext()) {
                    Statement stmt = stmtIterator.nextStatement();
                    String subjectName = stmt.getSubject().asResource().getLocalName().toLowerCase();
                    HashSet<String> subjectTypes = vertexMap.containsKey(subjectName) ? vertexMap.get(subjectName) : new HashSet<>();
                    if (subjectTypes.size() == 0) continue;
                    String predicate = stmt.getPredicate().getLocalName().toLowerCase();
                    String objectName = stmt.getObject().asResource().getLocalName().toLowerCase();
                    HashSet<String> objectTypes = vertexMap.containsKey(objectName) ? vertexMap.get(objectName) : new HashSet<>();
                    if (objectTypes.size() == 0) continue;
                    if (typesToConsider.containsAll(subjectTypes) && typesToConsider.containsAll(objectTypes)) {
                        newModel.add(stmt);
                        edgesSet.add(subjectTypes + " " + predicate + " " + objectTypes);
                        counter++;
//                        if (counter % 100000 == 0) System.out.println("Processed "+counter+" statements for 201" + i + OBJECTS);
                    }
                }
                System.out.println("Number of statements = " + counter);
                try {
                    String newFileName = "201" + (i+5) + OBJECTS + "-" + G + ".ttl";
                    newModel.write(new PrintStream(newFileName), "N3");
                    System.out.println("Wrote to " + newFileName);
                } catch (FileNotFoundException e) {
                    e.printStackTrace();
                }
            }

            System.out.println("Number of edge types: "+edgesSet.size());

            for (int i = 5; i < 8; i++) {
                for (String fileType : Arrays.asList(TYPES, LITERALS)) {
                    Model model = ModelFactory.createDefaultModel();
                    switch (fileType) {
                        case TYPES -> model = typesModels[i-5];
//                        case OBJECTS -> model = objectsModels[i-5];
                        case LITERALS -> model = literalsModels[i-5];
                    }
                    StmtIterator stmtIterator = model.listStatements();
                    Model newModel = ModelFactory.createDefaultModel();
                    int counter = 0;
                    System.out.println("Processing statements for 201" + i + fileType + ".ttl");
                    while (stmtIterator.hasNext()) {
                        Statement stmt = stmtIterator.nextStatement();
                        switch (fileType) {
                            case TYPES -> {
                                String vertexType = stmt.getObject().asResource().getLocalName().toLowerCase();
                                if (!typesToConsider.contains(vertexType)) continue;
                            }
//                            case OBJECTS -> {
//                                String subjectName = stmt.getSubject().asResource().getLocalName().toLowerCase();
//                                String objectName = stmt.getObject().asResource().getLocalName().toLowerCase();
//                                boolean skip = (vertexMap.containsKey(subjectName) && !typesToConsider.containsAll(vertexMap.get(subjectName)))
//                                        || (vertexMap.containsKey(objectName) && !typesToConsider.containsAll(vertexMap.get(objectName)));
//                                if (skip) continue;
//                            }
                            case LITERALS -> { // TO-DO: use the active attributes set to reduce size of literals file?
                                String subjectName = stmt.getSubject().asResource().getLocalName().toLowerCase();
                                boolean skip = vertexMap.containsKey(subjectName) && !typesToConsider.containsAll(vertexMap.get(subjectName));
                                if (skip) continue;
                            }
                        }
                        newModel.add(stmt);
                        counter++;
//                        if (counter % 100000 == 0) System.out.println("Processed "+counter+" statements for 201" + i + fileType + ".ttl");
                    }
                    System.out.println("Number of statements = " + counter);
                    try {
                        String newFileName = "201" + i + fileType + "-" + G + ".ttl";
                        newModel.write(new PrintStream(newFileName), "N3");
                        System.out.println("Wrote to " + newFileName);
                    } catch (FileNotFoundException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public static void generateCustomDBpediaBasedOnSize(String[] args, String path) {
        String[] fileTypes = {OBJECTS, TYPES, LITERALS};
        long[] sizes = new long[args.length];
        for (int index = 0; index < sizes.length; index++) {
            sizes[index] = Long.parseLong(args[index]);
        }
        Set<String> allURIsInData = new HashSet<>();
        Map<String, Set<Statement>> edgeToStmtMap = new HashMap<>();
        System.out.println("Gathering set of unique URIs in data...");
        for (int i = 5; i < 8; i++) {
            Model model = ModelFactory.createDefaultModel();
            String fileName = path+"/201"+i+ "/201"+i+TYPES+".ttl";
            System.out.println("Processing " + fileName);
            Path input = Paths.get(fileName);
            model.read(input.toUri().toString());
            StmtIterator stmtIterator = model.listStatements();
            while (stmtIterator.hasNext()) {
                Statement stmt = stmtIterator.nextStatement();
                allURIsInData.add(stmt.getSubject().getURI().toLowerCase());
            }
        }
        System.out.println("Number of unique URIs in data: "+allURIsInData.size());
        for (int i = 5; i < 8; i++) {
            HashMap<Long,Set<String>> verticesConnectedByEdgesSet = new HashMap<>();
            for (String fileType : fileTypes) {
                Model model = ModelFactory.createDefaultModel();
                String fileName = path+"/201"+i+ "/201"+i+ fileType + ".ttl";
                System.out.println("Processing " + fileName);
                Path input = Paths.get(fileName);
                model.read(input.toUri().toString());
                for (long size : sizes) {
                    verticesConnectedByEdgesSet.putIfAbsent(size, new HashSet<>());
                    StmtIterator stmtIterator = model.listStatements();
                    System.out.println("Outputting size: " + size);
                    int limit = fileType.equals(OBJECTS) ? Math.toIntExact(size) : Integer.MAX_VALUE;
                    Model newModel = ModelFactory.createDefaultModel();
                    int counter = 0;
                    while (stmtIterator.hasNext() && counter <= limit) {
                        Statement stmt = stmtIterator.nextStatement();
                        switch (fileType) {
                            case TYPES, LITERALS -> {
                                if (!verticesConnectedByEdgesSet.get(size).contains(stmt.getSubject().getURI().toLowerCase())) {
                                    continue;
                                }
                            }
                            case OBJECTS -> {
                                String subjectURI = stmt.getSubject().getURI().toLowerCase();
                                String objectURI = stmt.getObject().asResource().getURI().toLowerCase();
                                String predicateName = stmt.getPredicate().getLocalName();
                                String edgeType = String.join(",",Arrays.asList(subjectURI, predicateName, objectURI));
                                edgeToStmtMap.putIfAbsent(edgeType, new HashSet<>());
                                edgeToStmtMap.get(edgeType).add(stmt);
                                if (!allURIsInData.contains(subjectURI) || !allURIsInData.contains(objectURI)) continue;
                                verticesConnectedByEdgesSet.get(size).add(subjectURI);
                                verticesConnectedByEdgesSet.get(size).add(objectURI);
                            }
                        }
                        newModel.add(stmt);
                        counter++;
                    }
                    System.out.println("Number of statements = " + counter);
                    try {
                        String directoryStructure = "dbpedia-"+size+"/201"+i+"/";
                        Files.createDirectories(Paths.get(directoryStructure));
                        String newFileName = directoryStructure+"201"+i+fileType+"-"+size+".ttl";
                        newModel.write(new PrintStream(newFileName), "N3");
                        System.out.println("Wrote to " + newFileName);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        }
    }

    public static void generateCustomDBpediaBasedOnSize2(String[] args, List<Map.Entry<String, List<String>>> timestampToFilesMap) {
        String[] fileTypes = {TYPES, OBJECTS, LITERALS};
        long[] sizes = new long[args.length];
        for (int index = 0; index < sizes.length; index++) {
            sizes[index] = Long.parseLong(args[index]);
        }
        for (int i = 0; i < 3; i++) {
            Map<String, Set<String>> vertexURIToTypesMap = new HashMap<>();
            Map<String, List<Statement>> edgeTypeToStmtsMap = new HashMap<>();
            Map<String, Set<Statement>> vertexURIToStmtsMap = new HashMap<>();

            Model model = ModelFactory.createDefaultModel();
            Map.Entry<String, List<String>> timestampToFilesEntry = timestampToFilesMap.get(i);
            for (String fileType : fileTypes) {
                String filePath = "";
                for (String filename: timestampToFilesEntry.getValue()) {
                    if (filename.contains(fileType)) {
                        filePath = filename;
                    }
                }
//                final String filePath = path + "/201" + (i + 5) + "/201" + (i + 5) + fileType + ".ttl";
                System.out.print("Processing "+filePath+". ");
                Path input = Paths.get(filePath);
                model.read(input.toAbsolutePath().toString());
                StmtIterator stmtIterator = model.listStatements();
                switch(fileType) {
                    case TYPES -> {
                        while (stmtIterator.hasNext()) {
                            Statement stmt = stmtIterator.nextStatement();
                            if (!stmt.getPredicate().getLocalName().equalsIgnoreCase("type")) continue;
                            String subjectURI = stmt.getSubject().getURI().toLowerCase();
                            String nodeType = stmt.getObject().asResource().getLocalName().toLowerCase();
                            vertexURIToTypesMap.putIfAbsent(subjectURI, new HashSet<>());
                            vertexURIToTypesMap.get(subjectURI).add(nodeType);
                            vertexURIToStmtsMap.putIfAbsent(subjectURI, new HashSet<>());
                            vertexURIToStmtsMap.get(subjectURI).add(stmt);
                        }
                    }
                    case LITERALS -> {
                        while (stmtIterator.hasNext()) {
                            Statement stmt = stmtIterator.nextStatement();
                            if (!stmt.getObject().isLiteral()) continue;
                            if (stmt.getPredicate().getLocalName().equalsIgnoreCase("type")) continue;
                            String subjectURI = stmt.getSubject().getURI().toLowerCase();
                            vertexURIToStmtsMap.putIfAbsent(subjectURI, new HashSet<>());
                            vertexURIToStmtsMap.get(subjectURI).add(stmt);
                        }
                    }
                    case OBJECTS -> {
                        while (stmtIterator.hasNext()) {
                            Statement stmt = stmtIterator.nextStatement();
                            if (stmt.getObject().isLiteral()) continue;
                            String subjectURI = stmt.getSubject().getURI().toLowerCase();
                            Set<String> subjectTypes = vertexURIToTypesMap.get(subjectURI);
                            String objectURI = stmt.getObject().asResource().getURI().toLowerCase();
                            Set<String> objectTypes = vertexURIToTypesMap.get(objectURI);
                            String predicateName = stmt.getPredicate().getLocalName();
                            if (subjectTypes == null || objectTypes == null) continue;
                            for (String subjectType: subjectTypes) {
                                for (String objectType: objectTypes) {
                                    String edgeType = String.join(",", Arrays.asList(subjectType, predicateName, objectType));
                                    edgeTypeToStmtsMap.putIfAbsent(edgeType, new ArrayList<>());
                                    edgeTypeToStmtsMap.get(edgeType).add(stmt);
                                }
                            }
                        }
                    }
                }
                System.out.println("Done");
            }
            for (Map.Entry<String, List<Statement>> entry: edgeTypeToStmtsMap.entrySet()) {
                entry.getValue().sort(new Comparator<Statement>() {
                    @Override
                    public int compare(Statement o1, Statement o2) {
                        int result = o1.getSubject().getURI().toLowerCase().compareTo(o2.getSubject().getURI().toLowerCase());
                        if (result == 0) {
                            result = o1.getPredicate().getLocalName().compareTo(o2.getPredicate().getLocalName());
                        }
                        if (result == 0) {
                            result = o1.getObject().asResource().getURI().toLowerCase().compareTo(o2.getObject().asResource().getURI().toLowerCase());
                        }
                        return result;
                    }
                });
            }
            ArrayList<Map.Entry<String, List<Statement>>> sortedList = new ArrayList<>(edgeTypeToStmtsMap.entrySet());
            sortedList.sort(new Comparator<Map.Entry<String, List<Statement>>>() {
                @Override
                public int compare(Map.Entry<String, List<Statement>> o1, Map.Entry<String, List<Statement>> o2) {
                    return o1.getValue().size() - o2.getValue().size();
                }
            });
            List<Double> percentagesForThisTimestamp = new ArrayList<>();
            for (long size : sizes) {
                System.out.println("Size: "+size);
                double total = 0;
                double percent = 0.00;
                double increment = 0.01;
                while ((total < (size-(size*LOWER_THRESHOLD)) || total > (size+(size*UPPER_THRESHOLD))) && percent<=1.0) {
                    if (total < (size-(size*LOWER_THRESHOLD))) {
                        System.out.println("Not enough edges");
                        percent += increment;
                    } else if (total > (size+(size*UPPER_THRESHOLD))) {
                        System.out.println("Too many edges");
                        percent -= increment;
                        increment /= 10;
                        percent += increment;
                    }
//                    percent += 0.01;
                    System.out.println("Trying percent: "+percent);
                    total = 0;
                    for (Map.Entry<String, List<Statement>> entry: sortedList) {
                        Iterator<Statement> stmtIterator = entry.getValue().iterator();
                        int singleEdgeTypeCount = 0;
                        while (singleEdgeTypeCount+1 < (entry.getValue().size()*percent) && stmtIterator.hasNext()) {
                            stmtIterator.next();
                            singleEdgeTypeCount++;
                        }
//                        total += (entry.getValue().size() * percent);
                        total += singleEdgeTypeCount;
                    }
                    System.out.println("Total: "+total);
                }
                percentagesForThisTimestamp.add(percent);
            }
            System.out.println(percentagesForThisTimestamp);
            for (int j = 0; j < sizes.length; j++) {
                long size = sizes[j];
                String directoryStructure = "dbpedia-"+size+"/201"+(i+5)+"/";
                String newFileName = directoryStructure+"201"+(i+5)+"-"+size+".ttl";
                System.out.println("Creating model for "+newFileName);
                double percentage = percentagesForThisTimestamp.get(j);
                Model newModel = ModelFactory.createDefaultModel();
                int totalEdgeCount = 0;
                int totalAttributeCount = 0;
                for(Map.Entry<String, List<Statement>> edgeTypeToStmtsMapEntry: edgeTypeToStmtsMap.entrySet()) {
                    Iterator<Statement> stmtIterator = edgeTypeToStmtsMapEntry.getValue().iterator();
                    int singleEdgeTypeCount = 0;
                    while (singleEdgeTypeCount+1 < (edgeTypeToStmtsMapEntry.getValue().size()*percentage) && stmtIterator.hasNext()) {
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
                System.out.println("Edge count = "+totalEdgeCount);
                System.out.println("Attribute count = "+totalAttributeCount);
                try {
                    System.out.print("Writing to "+newFileName+". ");
                    Files.createDirectories(Paths.get(directoryStructure));
                    newModel.write(new PrintStream(newFileName), "N3");
                    System.out.println("Done.");
                    new DBPediaLoader(new ArrayList<>(), Collections.singletonList(newModel), Collections.singletonList(newModel));
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}
