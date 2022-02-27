package IncrementalRunner;

import Infra.*;
import org.jgrapht.GraphMapping;

import java.util.*;

public class IncrementalChange {

    //region Fields: Private
    private VF2PatternGraph pattern;
    private HashMap<String, Set<ConstantLiteral>> newMatches;
    private HashMap<String, Set<ConstantLiteral>> removedMatches;
    private ArrayList <String> removedMatchesSignatures;
    private HashMap<String, Set<ConstantLiteral>> afterMatches;
    private HashMap<String, Set<ConstantLiteral>> beforeMatches;
    //endregion

    //region Constructors
    public IncrementalChange(Iterator<GraphMapping<Vertex, RelationshipEdge>> beforeMatchIterator,VF2PatternGraph pattern)
    {
        newMatches=new HashMap<>();
        removedMatchesSignatures=new ArrayList <>();
        removedMatches=new HashMap<>();
        this.pattern=pattern;
        computeBeforeMatches(beforeMatchIterator);
    }
    //endregion

    //region Public Functions

    public Set<ConstantLiteral> extractMatch(GraphMapping<Vertex, RelationshipEdge> mapping) {
        Set<ConstantLiteral> match = new HashSet<>();
        for (Vertex v: pattern.getPattern().vertexSet()) {
            Vertex currentMatchedVertex = mapping.getVertexCorrespondence(v, false);
            if (currentMatchedVertex == null) continue;
            String patternVertexType = v.getTypes().iterator().next();
            for (String matchedAttrName : currentMatchedVertex.getAllAttributesNames()) {
                String matchedAttrValue = currentMatchedVertex.getAttributeValueByName(matchedAttrName);
                ConstantLiteral xLiteral = new ConstantLiteral(patternVertexType, matchedAttrName, matchedAttrValue);
                match.add(xLiteral);
            }
        }
        return match;
    }
    public String addAfterMatches(Iterator<GraphMapping<Vertex, RelationshipEdge>> afterMatchIterator)
    {
        afterMatches=new HashMap<>();
        if(afterMatchIterator!=null) {
            while (afterMatchIterator.hasNext()) {
                var mapping = afterMatchIterator.next();
                var signatureFromPattern = Match.signatureFromPattern(pattern, mapping);
                Set<ConstantLiteral> match = extractMatch(mapping);
                afterMatches.put(signatureFromPattern, match);
            }
        }

        for (String key:afterMatches.keySet()) {
            if(!beforeMatches.containsKey(key)) {
                newMatches.put(key, afterMatches.get(key));
            } else if (!beforeMatches.get(key).equals(afterMatches.get(key))) {
                newMatches.put(key, afterMatches.get(key));
            }
        }

        for (String key:beforeMatches.keySet()) {
            if(!afterMatches.containsKey(key)) {
                removedMatchesSignatures.add(key);
                removedMatches.put(key,beforeMatches.get(key));
            } else if (!afterMatches.get(key).equals(beforeMatches.get(key))) {
                removedMatchesSignatures.add(key);
                removedMatches.put(key,beforeMatches.get(key));
            }
        }

        return beforeMatches.size() + " - " +afterMatches.size();
        //System.out.print(beforeMatchesSignatures.size() + " -- " + newMatches.size() + " -- " + removedMatchesSignatures.size());
    }
    //endregion

    //region Private Functions
    private void computeBeforeMatches(Iterator<GraphMapping<Vertex, RelationshipEdge>> beforeMatchIterator)
    {
        beforeMatches=new HashMap<>();
        if (beforeMatchIterator!=null)
        {
            while (beforeMatchIterator.hasNext())
            {
                var mapping = beforeMatchIterator.next();
                var signatureFromPattern = Match.signatureFromPattern(pattern, mapping);
                Set<ConstantLiteral> match = extractMatch(mapping);
                beforeMatches.put(signatureFromPattern, match);
            }
        }
    }
    //endregion

    //region Getters
    public HashMap<String, Set<ConstantLiteral>> getNewMatches() {
        return newMatches;
    }

    public ArrayList<String> getRemovedMatchesSignatures() {
        return removedMatchesSignatures;
    }

    public HashMap<String, Set<ConstantLiteral>> getRemovedMatches() {
        return removedMatches;
    }

    //endregion
}
