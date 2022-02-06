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

public class testGFDMiner extends  TgfdDiscovery {

    public testGFDMiner(String[] args) {
        super();
        Options options = new Options();
        options.addOption("simplifySuperVertex", true, "run experiment by collapsing super vertices");
        options.addOption("simplifySuperVertexTypes", true, "run experiment by collapsing super vertex types");
        options.addOption("path", true, "generate graphs using files from specified path");
        CommandLineParser parser = new DefaultParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        assert cmd != null;
        if (cmd.hasOption("path")) {
            this.setPath(cmd.getOptionValue("path").replaceFirst("^~", System.getProperty("user.home")));
            if (!Files.isDirectory(Path.of(this.getPath()))) {
                System.out.println(Path.of(this.getPath()) + " is not a valid directory.");
                System.exit(1);
            }
            this.setGraphSize(Path.of(this.getPath()).getFileName().toString());
        }
        this.setLoader("dbpedia");
        assert this.getPath() != null;
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
        this.loadGraphsAndComputeHistogram(this.getTimestampToFilesMap());
    }
    public static void main(String []args)
    {
        TgfdDiscovery tgfdDiscovery = new testGFDMiner(args);

        int year = 2015;
        for (int i = 0; i < 3; i++) {
            GraphLoader loader = tgfdDiscovery.getGraphs().get(i);
            try {
                String fileName = tgfdDiscovery.getGraphSize()+"-"+(year+i)+".tsv";
                FileWriter file = new FileWriter(fileName);
                for (Vertex v : loader.getGraph().getGraph().vertexSet()) {
                    StringBuilder sb = new StringBuilder();
                    DataVertex data_v = (DataVertex) v;
                    String v_type = data_v.getTypes().stream().findFirst().orElse("");
                    sb.append("L").append("\t").append(data_v.getVertexURI()).append("\t").append(v_type).append("\n");
                    for (Attribute attr : data_v.getAllAttributesList()) {
                        if (!attr.getAttrName().equals("uri")) {
                            String vertexURI = data_v.getVertexURI();
                            String attrName = attr.getAttrName();
                            String attValue = attr.getAttrValue().replaceAll("\n", "\\n");
                            sb.append("A").append("\t").append(vertexURI).append("\t").append(attrName).append("\t").append(attValue).append("\n");
                        }
                    }
                    for (RelationshipEdge edge : loader.getGraph().getGraph().outgoingEdgesOf(v)) {
                        DataVertex out_v = (DataVertex) edge.getTarget();
                        sb.append("E").append("\t").append(data_v.getVertexURI()).append("\t").append(out_v.getVertexURI()).append("\t").append(edge.getLabel()).append("\n");
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
