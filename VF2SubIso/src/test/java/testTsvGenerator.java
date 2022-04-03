import TgfdDiscovery.TgfdDiscovery;
import Infra.*;
import graphLoader.GraphLoader;
import org.apache.commons.cli.*;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class testTsvGenerator extends  TgfdDiscovery {

    public testTsvGenerator(String[] args) {
        super(args);
//        Options options = new Options();
//        options.addOption("simplifySuperVertex", true, "run experiment by collapsing super vertices");
//        options.addOption("simplifySuperVertexTypes", true, "run experiment by collapsing super vertex types");
//        options.addOption("path", true, "generate graphs using files from specified path");
//        options.addOption("loader", true, "run experiment using specified loader");
//        options.addOption("t", true, "run experiment using t number of snapshots");
//
//        CommandLineParser parser = new DefaultParser();
//        CommandLine cmd = null;
//        try {
//            cmd = parser.parse(options, args);
//        } catch (ParseException e) {
//            e.printStackTrace();
//        }
//        assert cmd != null;
//        if (cmd.hasOption("path")) {
//            this.setPath(cmd.getOptionValue("path").replaceFirst("^~", System.getProperty("user.home")));
//            if (!Files.isDirectory(Path.of(this.getPath()))) {
//                System.out.println(Path.of(this.getPath()) + " is not a valid directory.");
//                System.exit(1);
//            }
//            this.setGraphSize(Path.of(this.getPath()).getFileName().toString());
//        }
//        if (!cmd.hasOption("loader")) {
//            System.out.println("No specifiedLoader is specified.");
//            System.exit(1);
//        } else {
//            this.setLoader(cmd.getOptionValue("loader"));
//        }
//
//        this.setT(cmd.getOptionValue("t") == null ? TgfdDiscovery.DEFAULT_NUM_OF_SNAPSHOTS : Integer.parseInt(cmd.getOptionValue("t")));
//
//        assert this.getPath() != null;
//        if (cmd.hasOption("simplifySuperVertexTypes")) {
//            MEDIAN_SUPER_VERTEX_TYPE_INDEGREE_FLOOR = Double.parseDouble(cmd.getOptionValue("simplifySuperVertexTypes"));
//            this.setDissolveSuperVertexTypes(true);
//        } else if (cmd.hasOption("simplifySuperVertex")) {
//            INDIVIDUAL_SUPER_VERTEX_INDEGREE_FLOOR = Integer.valueOf(cmd.getOptionValue("simplifySuperVertex"));
//            this.setDissolveSuperVerticesBasedOnCount(true);
//        }
//
//        switch (this.getLoader().toLowerCase()) {
//            case "dbpedia" -> this.setDBpediaTimestampsAndFilePaths(this.getPath());
//            case "citation" -> this.setCitationTimestampsAndFilePaths();
//            case "imdb" -> this.setImdbTimestampToFilesMapFromPath(this.getPath());
//            default -> {
//                System.out.println("No valid loader specified.");
//                System.exit(1);
//            }
//        }
////        this.loadGraphsAndComputeHistogram(this.getTimestampToFilesMap());
//        this.loadGraphsAndComputeHistogram2();
    }
    public static void main(String []args)
    {
        TgfdDiscovery tgfdDiscovery = new testTsvGenerator(args);
        tgfdDiscovery.loadGraphsAndComputeHistogram2();

        for (int i = 0; i < tgfdDiscovery.getNumOfSnapshots(); i++) {
            GraphLoader loader = tgfdDiscovery.getGraphs().get(i);
            try {
                String fileName = tgfdDiscovery.getGraphSize()+"-"+tgfdDiscovery.getTimestampToFilesMap().get(i).getKey()+".tsv";
                FileWriter file = new FileWriter(fileName);
                for (Vertex v : loader.getGraph().getGraph().vertexSet()) {
                    StringBuilder sb = new StringBuilder();
                    DataVertex data_v = (DataVertex) v;
                    String v_type = data_v.getTypes().stream().findFirst().orElse("");
                    String vertexURI;
                    if (tgfdDiscovery.getLoader().equals("imdb")) {
                        if (data_v.getVertexURI().contains("/")) {
                            String[] info = data_v.getVertexURI().split("/");
                            vertexURI = info[1];
                        } else {
                            vertexURI = data_v.getVertexURI();
                        }
                    } else {
                        vertexURI = data_v.getVertexURI();
                    }
                    sb.append("L").append("\t").append(vertexURI).append("\t").append(v_type).append("\n");
                    for (Attribute attr : data_v.getAllAttributesList()) {
                        if (!attr.getAttrName().equals("uri")) {
                            String attrName = attr.getAttrName();
                            String attValue = attr.getAttrValue().replaceAll("\n", "\\n");
                            sb.append("A").append("\t").append(vertexURI).append("\t").append(attrName).append("\t").append(attValue).append("\n");
                        }
                    }
                    for (RelationshipEdge edge : loader.getGraph().getGraph().outgoingEdgesOf(v)) {
                        DataVertex out_v = (DataVertex) edge.getTarget();
                        sb.append("E").append("\t").append(vertexURI).append("\t").append(out_v.getVertexURI()).append("\t").append(edge.getLabel()).append("\n");
                    }
                    file.write(sb.toString());
                }

                file.close();
                System.out.println("Successfully wrote to the "+fileName+".");
                BufferedReader csvReader = new BufferedReader(new FileReader(fileName));
                String row;
                while ((row = csvReader.readLine()) != null) {
                    if (!row.contains("\t")){
                        System.out.println("Rows with errors:");
                        System.out.println(row);
                    }
                }
                csvReader.close();
            } catch (IOException e) {
                System.out.println("An error occurred.");
                e.printStackTrace();
            }
        }
    }

}
