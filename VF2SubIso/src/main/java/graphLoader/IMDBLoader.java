package graphLoader;

import Infra.Attribute;
import Infra.DataVertex;
import Infra.RelationshipEdge;
import Infra.TGFD;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import org.apache.jena.rdf.model.*;
import org.jetbrains.annotations.NotNull;
import util.Config;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.List;

public class IMDBLoader extends GraphLoader{

    public IMDBLoader(List <TGFD> alltgfd, List<?> paths) {

        super(alltgfd);
        for (Object obj:paths) {
            if (obj instanceof String) {
                loadIMDBGraphFromPath((String) obj);
            } else if (obj instanceof Model) {
                loadIMDBGraphFromModel((Model) obj);
            }
        }
    }

    private void loadIMDBGraphFromModel(Model model) {
        try {
            StmtIterator dataTriples = model.listStatements();
            while (dataTriples.hasNext()) {

                Statement stmt = dataTriples.nextStatement();
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
                String subjectID = temp[1];

                // ignore the node if the type is not in the validTypes and
                // optimizedLoadingBasedOnTGFD is true
//                if (Config.optimizedLoadingBasedOnTGFD && !validTypes.contains(subjectType))
//                    continue;

                //int nodeId = subject.hashCode();
//                DataVertex subjectVertex = getDataVertex(subjectType, subjectID);

                String predicate = stmt.getPredicate().getLocalName().toLowerCase();
                RDFNode object = stmt.getObject();
                String objectNodeURI;
                if (object.isLiteral()) {
                    if (Config.optimizedLoadingBasedOnTGFD) {
                        if (validTypes.size() > 1) {
                            if (!validTypes.contains(subjectType)) {
//                                System.out.println(stmt);
                                continue;
                            }
                        }
                    }
                    DataVertex subjectVertex = getDataVertex(subjectType, subjectNodeURI);
                    objectNodeURI = object.asLiteral().getString().toLowerCase();
                    subjectVertex.putAttributeIfAbsent(new Attribute(predicate, objectNodeURI));
                } else {

                    objectNodeURI = object.toString().toLowerCase();
                    if (objectNodeURI.length() > 16)
                        objectNodeURI = objectNodeURI.substring(16);

                    temp = objectNodeURI.split("/");
                    if (temp.length != 2) {
                        // Error!
                        continue;
                    }

                    String objectType = temp[0];
                    String objectID = temp[1];

                    // ignore the node if the type is not in the validTypes and
                    // optimizedLoadingBasedOnTGFD is true
//                    if (Config.optimizedLoadingBasedOnTGFD && !validTypes.contains(objectType))
//                        continue;
                    if (Config.optimizedLoadingBasedOnTGFD) {
                        if (validTypes.size() == 1) {
                            if (!validTypes.contains(subjectType)) {
//                                System.out.println(stmt);
                                continue;
                            }
                        } else if (validTypes.size() > 1) {
                            if (!validTypes.contains(subjectType) || !validTypes.contains(objectType)) {
                                continue;
                            }
                        }
                    }
                        DataVertex subjectVertex = getDataVertex(subjectType, subjectNodeURI);
                        DataVertex objectVertex = getDataVertex(objectType, objectNodeURI);
                        graph.addEdge(subjectVertex, objectVertex, new RelationshipEdge(predicate));
                }
                graphSize++;
            }
            System.out.println("Done. Nodes: " + graph.getGraph().vertexSet().size() + ",  Edges: " + graph.getGraph().edgeSet().size());
            System.out.println("Number of types: " + types.size());
            System.out.println(String.join(" - ", types));
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
        }
    }

    @NotNull
    private DataVertex getDataVertex(String vertexType, String vertexID) {
        this.types.add(vertexType);

        DataVertex subjectVertex = (DataVertex) graph.getNode(vertexID);

        if (subjectVertex == null) {
            subjectVertex = new DataVertex(vertexID, vertexType);
            graph.addVertex(subjectVertex);
        } else {
            subjectVertex.addType(vertexType); // TO-DO: Should multiple types be disabled? It impacts optimized loading.
        }
        return subjectVertex;
    }

    private void loadIMDBGraphFromPath(String dataGraphFilePath) {

        if (dataGraphFilePath == null || dataGraphFilePath.length() == 0) {
            System.out.println("No Input Graph Data File Path!");
            return;
        }
        System.out.println("Loading IMDB Graph: "+dataGraphFilePath);

        S3Object fullObject = null;
        BufferedReader br=null;
        try
        {
            Model model = ModelFactory.createDefaultModel();

            if(Config.Amazon)
            {
                AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                        .withRegion(Config.region)
                        //.withCredentials(new ProfileCredentialsProvider())
                        //.withCredentials(DefaultAWSCredentialsProviderChain.getInstance())
                        .build();
                //TODO: Need to check if the path is correct (should be in the form of bucketName/Key )
                String bucketName=dataGraphFilePath.substring(0,dataGraphFilePath.lastIndexOf("/"));
                String key=dataGraphFilePath.substring(dataGraphFilePath.lastIndexOf("/")+1);
                System.out.println("Downloading the object from Amazon S3 - Bucket name: " + bucketName +" - Key: " + key);
                fullObject = s3Client.getObject(new GetObjectRequest(bucketName, key));

                br = new BufferedReader(new InputStreamReader(fullObject.getObjectContent()));
                model.read(br,null, Config.language);
            }
            else
            {
                Path input= Paths.get(dataGraphFilePath);
                model.read(input.toUri().toString());
            }

           loadIMDBGraphFromModel(model);

            if (fullObject != null) {
                fullObject.close();
            }
            if (br != null) {
                br.close();
            }
            model.close();
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
        }
    }

}
