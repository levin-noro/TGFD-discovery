package changeExploration;

import graphLoader.GraphLoader;
import Infra.*;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;

/** This class will find all the change logs between two data graph */
public class ChangeFinder {

    //region --[Fields: Private]-----------------------------------------

    /** First and second data graph. */
    private final VF2DataGraph g1;
    private final VF2DataGraph g2;

    /** Map of the relevant TGFDs for each entity type */
    private final HashMap<String, HashSet<String>> relaventTGFDs=new HashMap <>();

    /** List of all changes to return */
    private final List<Change> allChanges = Collections.synchronizedList(new ArrayList<>());

    /** Unique id to assign to a change log. */
    private static final AtomicInteger changeID = new AtomicInteger(1);

    /** number of changes except the vertex change  */
    private final AtomicInteger numberOfEffectiveChanges = new AtomicInteger();

    //endregion

    //region --[Constructor]-----------------------------------------

    /**
     * @param db1 First data graph
     * @param db2 Second data graph
     * @param tgfds List of TGFDs
     */
    public ChangeFinder(GraphLoader db1, GraphLoader db2, List<TGFD> tgfds)
    {
        g1=db1.getGraph();
        g2=db2.getGraph();

        for (TGFD tgfd:tgfds) {
            extractValidTypesFromTGFD(tgfd);
        }
    }

    //endregion

    //region --[Public Methods]-----------------------------------------

    /**
     * This function will compute the changes over g1 and g2
     * @return List of all changes
     */
    public List<Change> findAllChanged()
    {
        System.out.println("Looking for deletions and changes...");
        findChanges(g1,g2, ChangeType.deleteEdge, ChangeType.deleteVertex, ChangeType.deleteAttr, ChangeType.changeAttr, ChangeType.changeType);
        System.out.println("Looking for insertions...");
        findChanges(g2,g1, ChangeType.insertEdge, ChangeType.insertVertex, ChangeType.insertAttr,null, null);
        return allChanges;
    }

    //endregion

    //region --[Private Methods]-----------------------------------------

    /**
     * Extracts all the types being used in a TGFD from from X->Y dependency and the graph pattern
     * For each type, add the TGFD name to the HashMap so we know
     * what TGFDs are affected if a an entity of a specific type had a change
     * @param tgfd input TGFD
     */
    private void extractValidTypesFromTGFD(TGFD tgfd)
    {
        for (Literal x:tgfd.getDependency().getX()) {
            if(x instanceof ConstantLiteral)
                addRelevantType(((ConstantLiteral) x).getVertexType(),tgfd.getName());
            else if(x instanceof VariableLiteral)
            {
                addRelevantType(((VariableLiteral) x).getVertexType_1(),tgfd.getName());
                addRelevantType(((VariableLiteral) x).getVertexType_2(),tgfd.getName());
            }
        }
        for (Literal y:tgfd.getDependency().getY()) {
            if(y instanceof ConstantLiteral)
                addRelevantType(((ConstantLiteral) y).getVertexType(),tgfd.getName());
            else if(y instanceof VariableLiteral)
            {
                addRelevantType(((VariableLiteral) y).getVertexType_1(),tgfd.getName());
                addRelevantType(((VariableLiteral) y).getVertexType_2(),tgfd.getName());
            }
        }
        for (Vertex v:tgfd.getPattern().getGraph().vertexSet()) {
            if(v instanceof PatternVertex)
                for (String type:v.getTypes())
                    addRelevantType(type,tgfd.getName());
        }
    }

    /**
     * This method adds the TGFD name to the HashSet of the input type
     * @param type input type
     * @param TGFDName input TGFD name
     */
    private void addRelevantType(String type, String TGFDName)
    {
        if(!relaventTGFDs.containsKey(type))
            relaventTGFDs.put(type,new HashSet <>());
        relaventTGFDs.get(type).add(TGFDName);
    }

    /**
     * This function will find all the relevant TGFDs for the set of types as input
     * @param types Set of types
     * @return Set of TGFD names that are relevant to that types
     */
    private Collection <String> findRelevantTGIFDs(Collection<String> types)
    {
        HashSet<String> TGFDNames=new HashSet <>();

        if(types!=null)
            for (String type:types)
                if(relaventTGFDs.containsKey(type))
                    TGFDNames.addAll(relaventTGFDs.get(type));

        return TGFDNames;
    }

    /**
     * This function will do (first - second) and extract all the changes that are in first but not in second
     * When we input first:t1 and second:t2, then t1-t2 means the changes that are removed
     * When we input first:t2 and second:t1, then t2-t1 means the changes that are inserted
     * For attributes that their value is updated (no remove or insertion), we only
     * pass "ChangeType.changeAttr" once and pass null for the second time to prevent redundant change logs
     * @param first The first data graph
     * @param second The second data graph
     * @param edgeType Type of edge change (either remove or insert)
     * @param vertexType Type of vertex change (either remove or insert)
     * @param attrType Type of attribute change (either remove or insert)
     * @param attrChange Type of attribute change (either update or null)
     */
    private void findChanges(VF2DataGraph first, VF2DataGraph second, ChangeType edgeType,
                             ChangeType vertexType, ChangeType attrType, ChangeType attrChange, ChangeType typeChange)
    {
        System.out.println("Looking for edge changes...");
        AtomicInteger numOfVerticesProcessed = new AtomicInteger();
        first.getGraph().vertexSet().parallelStream().forEach(v -> {
            DataVertex v1=(DataVertex) v;
            for (RelationshipEdge e: first.getGraph().outgoingEdgesOf(v)) {
                DataVertex dst=(DataVertex)e.getTarget();
                if(second.getNode(v1.getVertexURI())==null)
                {
                    Change eChange=new EdgeChange(edgeType,changeID.getAndIncrement() ,v1.getVertexURI(),dst.getVertexURI(),e.getLabel());
                    eChange.addTGFD(findRelevantTGIFDs(v1.getTypes()));
                    eChange.addTypes(v1.getTypes());
                    eChange.addTGFD(findRelevantTGIFDs(dst.getTypes()));
                    eChange.addTypes(dst.getTypes());
                    allChanges.add(eChange);
                    numberOfEffectiveChanges.getAndIncrement();
                }
                else if(second.getNode(dst.getVertexURI())==null)
                {
                    Change eChange=new EdgeChange(edgeType,changeID.getAndIncrement() ,v1.getVertexURI(),dst.getVertexURI(),e.getLabel());
                    eChange.addTGFD(findRelevantTGIFDs(v1.getTypes()));
                    eChange.addTypes(v1.getTypes());
                    eChange.addTGFD(findRelevantTGIFDs(dst.getTypes()));
                    eChange.addTypes(dst.getTypes());
                    allChanges.add(eChange);
                    numberOfEffectiveChanges.getAndIncrement();
                }
                else
                {
                    boolean exist=false;
                    Vertex v1_prime= second.getNode(v1.getVertexURI());
                    for (RelationshipEdge e2: second.getGraph().outgoingEdgesOf(v1_prime)) {
                        DataVertex dst2=(DataVertex) e2.getTarget();
                        if(e.getLabel().equals(e2.getLabel()) && dst2.getVertexURI().equals(dst.getVertexURI()))
                        {
                            exist=true;
                            break;
                        }
                    }
                    if(!exist)
                    {
                        Change eChange=new EdgeChange(edgeType,changeID.getAndIncrement() ,v1.getVertexURI(),dst.getVertexURI(),e.getLabel());
                        eChange.addTGFD(findRelevantTGIFDs(v1.getTypes()));
                        eChange.addTypes(v1.getTypes());
                        eChange.addTGFD(findRelevantTGIFDs(dst.getTypes()));
                        eChange.addTypes(dst.getTypes());
                        allChanges.add(eChange);
                        numberOfEffectiveChanges.getAndIncrement();
                    }
                }
            }
            numOfVerticesProcessed.getAndIncrement();
            int n = numOfVerticesProcessed.get();
            if (n % 100000 == 0)
                System.out.println("Processed "+n+" vertices");
        });

        System.out.println("Looking for other changes...");
        numOfVerticesProcessed.set(0);
        first.getGraph().vertexSet().parallelStream().forEach(v -> {
            DataVertex v1=(DataVertex) v;
            DataVertex v2= (DataVertex) second.getNode(v1.getVertexURI());
            boolean skipChange = false;
            if(v2==null)
            {
                Change vChange=new VertexChange(vertexType,changeID.getAndIncrement() ,v1);
                vChange.addTGFD(findRelevantTGIFDs(v1.getTypes()));
                vChange.addTypes(v1.getTypes());
                allChanges.add(vChange);
                skipChange = true;
            }
            if (!skipChange) {
                int groupedId = changeID.getAndIncrement();
                if (!v2.getTypes().equals(v1.getTypes()) && typeChange == ChangeType.changeType) {
                    for (String v1Type : v1.getTypes()) {
                        if (!v2.getTypes().contains(v1Type)) {
                            Change vChange1 = new TypeChange(typeChange, groupedId, v1, v2, v1.getVertexURI());
                            vChange1.addTGFD(findRelevantTGIFDs(v1.getTypes()));
                            vChange1.addTypes(v1.getTypes());
                            vChange1.addTypes(v2.getTypes());
                            allChanges.add(vChange1);
                        }
                    }
                }
                for (Attribute attr : v.getAllAttributesList()) {
                    if (attrChange != null) {
                        if (!v2.hasAttribute(attr.getAttrName())) {
                            Change changeOfAttr = new AttributeChange(ChangeType.deleteAttr, groupedId, v1.getVertexURI(), attr);
                            changeOfAttr.addTGFD(findRelevantTGIFDs(v1.getTypes()));
                            changeOfAttr.addTypes(v1.getTypes());
                            changeOfAttr.addTypes(v2.getTypes());
                            allChanges.add(changeOfAttr);
                            numberOfEffectiveChanges.getAndIncrement();
                        } else if (!v2.getAttributeValueByName(attr.getAttrName()).equals(attr.getAttrValue())) {
                            Change changeOfAttr = new AttributeChange(ChangeType.changeAttr, groupedId, v1.getVertexURI(), v2.getAllAttributesHashMap().get(attr.getAttrName()));
                            changeOfAttr.addTGFD(findRelevantTGIFDs(v1.getTypes()));
                            changeOfAttr.addTypes(v1.getTypes());
                            changeOfAttr.addTypes(v2.getTypes());
                            allChanges.add(changeOfAttr);
                            numberOfEffectiveChanges.getAndIncrement();
                        }
                    }
                }
                for (Attribute attr : v2.getAllAttributesList()) {
                    if (attrChange != null) {
                        if (!v1.hasAttribute(attr.getAttrName())) {
                            Change changeOfAttr = new AttributeChange(ChangeType.insertAttr, groupedId, v2.getVertexURI(), attr);
                            changeOfAttr.addTGFD(findRelevantTGIFDs(v2.getTypes()));
                            changeOfAttr.addTypes(v1.getTypes());
                            changeOfAttr.addTypes(v2.getTypes());
                            allChanges.add(changeOfAttr);
                            numberOfEffectiveChanges.getAndIncrement();
                        }
                    }
                }
            }
            numOfVerticesProcessed.getAndIncrement();
            int n = numOfVerticesProcessed.get();
            if (n % 100000 == 0)
                System.out.println("Processed "+n+" vertices");
        });
    }

    //endregion

    //region --[Properties: Public]------------------------------------

    /**
     * @return number of changes except the vertex change
     */
    public int getNumberOfEffectiveChanges() {
        return numberOfEffectiveChanges.get();
    }

    //endregion
}
