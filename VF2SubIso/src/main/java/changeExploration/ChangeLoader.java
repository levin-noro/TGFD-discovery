package changeExploration;

import Infra.Attribute;
import Infra.DataVertex;
import org.jetbrains.annotations.NotNull;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.*;

public class ChangeLoader {

    private List<Change> allChanges;

    private HashMap<Integer,HashSet<Change>> allGroupedChanges;

    public ChangeLoader(JSONArray jsonArray, Set<String> vertexSets, Map<String, Set<String>> typeChangeURIs, boolean considerEdgeChanges)
    {
        this.allChanges = new ArrayList<>();
        allGroupedChanges = new HashMap<>();
        loadChanges(jsonArray, vertexSets, typeChangeURIs, considerEdgeChanges);
    }

    public List<Change> getAllChanges() {
        return allChanges;
    }

    private void loadChanges(JSONArray jsonArray, Set<String> vertexSets, Map<String, Set<String>> typeChangeURIs, boolean considerEdgeChanges) {
        for (Object o : jsonArray) {
            JSONObject object = (JSONObject) o;

            org.json.simple.JSONArray allRelevantTGFDs = (org.json.simple.JSONArray) object.get("tgfds");
            HashSet <String> relevantTGFDs = new HashSet<>();
            for (Object TGFDName : allRelevantTGFDs)
                relevantTGFDs.add((String) TGFDName);

            org.json.simple.JSONArray allRelevantTypes = (org.json.simple.JSONArray) object.get("types");
            Set<String> relevantTypes = new HashSet<>();
            for (Object type : allRelevantTypes)
                relevantTypes.add((String) type);

            ChangeType type = ChangeType.valueOf((String) object.get("typeOfChange"));
            int id = Integer.parseInt(object.get("id").toString());
            if (considerEdgeChanges && (type == ChangeType.deleteEdge || type == ChangeType.insertEdge)) {
                String srcURI = (String) object.get("src");
                String dstURI = (String) object.get("dst");
                if (vertexSets != null && vertexSets.stream().noneMatch(s -> relevantTypes.contains(s)))
                    if (notURIofInterest(srcURI, vertexSets, typeChangeURIs))
                        continue;
                    else if (notURIofInterest(dstURI, vertexSets, typeChangeURIs))
                        continue;
                String label = (String) object.get("label");
                Change change = new EdgeChange(type, id, srcURI, dstURI, label);
                change.addTGFD(relevantTGFDs);
                change.addTypes(relevantTypes);
                allChanges.add(change);
                if (!allGroupedChanges.containsKey(change.getId()))
                    allGroupedChanges.put(change.getId(), new HashSet<>());
                allGroupedChanges.get(change.getId()).add(change);
            } else if (type == ChangeType.changeAttr || type == ChangeType.deleteAttr || type == ChangeType.insertAttr) {
                String uri = (String) object.get("uri");
                if (vertexSets != null && vertexSets.stream().noneMatch(s -> relevantTypes.contains(s)))
                    if (notURIofInterest(uri, vertexSets, typeChangeURIs))
                        continue;
                JSONObject attrObject = (JSONObject) object.get("attribute");
                String attrName = (String) attrObject.get("attrName");
                String attrValue = (String) attrObject.get("attrValue");
                Change change = new AttributeChange(type, id, uri, new Attribute(attrName, attrValue));
                change.addTGFD(relevantTGFDs);
                change.addTypes(relevantTypes);
                allChanges.add(change);
                if (!allGroupedChanges.containsKey(change.getId()))
                    allGroupedChanges.put(change.getId(), new HashSet<>());
                allGroupedChanges.get(change.getId()).add(change);
            } else if (type == ChangeType.changeType) {
                JSONObject previousVertexObj = (JSONObject) object.get("previousVertex");
                DataVertex previousVertex = getDataVertex(previousVertexObj);
                JSONObject newVertexObj = (JSONObject) object.get("newVertex");
                DataVertex newVertex = getDataVertex(newVertexObj);
                String uri = (String) object.get("uri");
                if (vertexSets != null && vertexSets.stream().noneMatch(s -> relevantTypes.contains(s)))
                    if (notURIofInterest(uri, vertexSets, typeChangeURIs))
                        continue;
                Change change = new TypeChange(type, id, previousVertex, newVertex, uri);
                change.addTGFD(relevantTGFDs);
                change.addTypes(relevantTypes);
                allChanges.add(change);
                if (!allGroupedChanges.containsKey(change.getId()))
                    allGroupedChanges.put(change.getId(), new HashSet<>());
                allGroupedChanges.get(change.getId()).add(change);
            } else if (type == ChangeType.deleteVertex || type == ChangeType.insertVertex) {
                JSONObject vertexObj = (JSONObject) object.get("vertex");
//                String uri = (String) vertexObj.get("vertexURI");
//                if (vertexSets != null && vertexSets.stream().noneMatch(s -> relevantTypes.contains(s)))
//                    if (!isURIofInterest(uri, vertexSets, typeChangeURIs))
//                        continue;
                DataVertex dataVertex = getDataVertex(vertexObj);
                Change change = new VertexChange(type, id, dataVertex);
                change.addTGFD(relevantTGFDs);
                change.addTypes(relevantTypes);
                allChanges.add(change);
                if (!allGroupedChanges.containsKey(change.getId()))
                    allGroupedChanges.put(change.getId(), new HashSet<>());
                allGroupedChanges.get(change.getId()).add(change);
            }

        }
    }

    private boolean notURIofInterest(String uri, Set<String> vertexSets, Map<String, Set<String>> typeChangeURIs) {
        if (typeChangeURIs.containsKey(uri))
            return vertexSets.stream().noneMatch(s -> typeChangeURIs.get(uri).contains(s));
        else
            return true;
    }

    @NotNull
    private DataVertex getDataVertex(JSONObject vertexObj) {
        String uri=(String) vertexObj.get("vertexURI");
        org.json.simple.JSONArray allTypes=(org.json.simple.JSONArray) vertexObj.get("types");

        ArrayList<String> types=new ArrayList<>();
        for (Object allType : allTypes)
            types.add((String) allType);

        ArrayList<Attribute> allAttributes=new ArrayList<>();
        org.json.simple.JSONArray allAttributeLists=(org.json.simple.JSONArray) vertexObj.get("allAttributesList");
        for (Object allAttributeList : allAttributeLists) {
            JSONObject attrObject = (JSONObject) allAttributeList;
            String attrName = (String) attrObject.get("attrName");
            String attrValue = (String) attrObject.get("attrValue");
            allAttributes.add(new Attribute(attrName, attrValue));
        }

        DataVertex dataVertex=new DataVertex(uri,types.get(0));
        for (int i=1;i<types.size();i++)
            dataVertex.addType(types.get(i));
        for (Attribute attribute:allAttributes) {
            dataVertex.putAttribute(attribute);
        }
        return dataVertex;
    }

    public HashMap<Integer, HashSet<Change>> getAllGroupedChanges() {
        return allGroupedChanges;
    }

}
