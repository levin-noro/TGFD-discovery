package changeExploration;

import Infra.DataVertex;

public class TypeChange extends Change {


    private DataVertex previousVertex;
    private DataVertex newVertex;
    String uri;

    public TypeChange(ChangeType cType, int id, DataVertex previousVertex, DataVertex newVertex, String uri) {
        super(cType,id);
        this.previousVertex = previousVertex;
        this.newVertex = newVertex;
        this.uri=uri;
    }

    @Override
    public String toString() {
        return "TypeChange ("+getTypeOfChange()+"){" +
                "previousVertex=" + getPreviousVertex() +
                ", newVertex=" + getNewVertex() +
                ", uri=" + uri +
                '}';
    }

    public String getUri() {
        return uri;
    }

    public DataVertex getNewVertex() {
        return newVertex;
    }

    public DataVertex getPreviousVertex() {
        return previousVertex;
    }
}
