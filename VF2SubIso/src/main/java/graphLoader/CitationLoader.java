package graphLoader;

import Infra.DataVertex;
import Infra.RelationshipEdge;
import Infra.TGFD;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.TypeAdapterFactory;
import com.google.gson.annotations.JsonAdapter;
import com.google.gson.annotations.SerializedName;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class CitationLoader extends GraphLoader {

    /**
     * @param filePath Path to the Citation dataset file
     * @param v11 Is file v11 or or v12+
     */
    public CitationLoader(String filePath, boolean v11)
    {
        super(new ArrayList<>());

        if (filePath == null || filePath.length() == 0) {
            System.out.println("No Input File Path!");
            return;
        }

        loadJSON(filePath, v11);

    }

    public CitationLoader(TGFD dummyTGFD, String filePath, boolean v11) {
        super(Collections.singletonList(dummyTGFD));

        if (filePath == null || filePath.length() == 0) {
            System.out.println("No Input File Path!");
            return;
        }

        loadJSON(filePath, v11);
    }

    private void parse(JsonReader jsonReader) throws Exception {
        jsonReader.beginArray();
        jsonReader.setLenient(true);
        Gson gson = new GsonBuilder().registerTypeAdapter(Integer.class,(new IntegerTypeAdapter())).create();

        while (jsonReader.hasNext()) {
            Paper paper = gson.fromJson(jsonReader, Paper.class);
            loadDataIntoGraph(paper);
        }
    }

    private static class IntegerTypeAdapter extends TypeAdapter<Integer> {

        @Override
        public void write(JsonWriter jsonWriter, Integer value) throws IOException {
            if (value == null) {
                jsonWriter.nullValue();
                return;
            }
            jsonWriter.value(value);
        }

        @Override
        public Integer read(JsonReader reader) throws IOException {
            if (reader.peek() == JsonToken.NULL) {
                reader.nextNull();
                return null;
            }
            String stringValue = reader.nextString();
            Pattern pattern = Pattern.compile("NumberInt\\(([0-9]+)\\)", Pattern.CASE_INSENSITIVE);
            Matcher matcher = pattern.matcher(stringValue);
            if (matcher.find()) {
                try {
                    return Integer.valueOf(matcher.group(1));
                } catch (NumberFormatException e) {
                    return null;
                }

            } else {
                try {
                    return Integer.valueOf(stringValue);
                } catch (NumberFormatException e) {
                    return null;
                }
            }
        }
    }

    private static class ListTypeAdapterFactory implements TypeAdapterFactory {
        @Override
        public <T> TypeAdapter<T> create(Gson gson, TypeToken<T> typeToken) {
            Type type = typeToken.getType();
            if (!List.class.isAssignableFrom(typeToken.getRawType())) {
                return null;
            }
            Type elementType;
            if ( !(type instanceof ParameterizedType) ) {
                elementType = Object.class;
            } else {
                elementType = ((ParameterizedType) type).getActualTypeArguments()[0];
            }
            TypeAdapter<?> elementAdapter = gson.getAdapter(TypeToken.get(elementType));
            TypeAdapter<T> listTypeAdapter = (TypeAdapter<T>) new ListTypeAdapter<>(elementAdapter);
            return listTypeAdapter;
        }
    }

    private static final class ListTypeAdapter<E> extends TypeAdapter<List<E>> {

        private final TypeAdapter<E> elementAdapter;

        private ListTypeAdapter(final TypeAdapter<E> elementAdapter) {
            this.elementAdapter = elementAdapter;
        }

        @Override
        public void write(JsonWriter jsonWriter, List<E> list) throws IOException {
            if (list == null) {
                jsonWriter.nullValue();
                return;
            }
            jsonWriter.beginArray();
            for (E element: list) {
                this.elementAdapter.write(jsonWriter, element);
            }
        }

        @Override
        public List<E> read(JsonReader jsonReader) throws IOException {
            List<E> list = new ArrayList<>();
            JsonToken token = jsonReader.peek();
            switch (token) {
                case BEGIN_ARRAY -> {
                    jsonReader.beginArray();
                    while (jsonReader.hasNext()) {
                        list.add(this.elementAdapter.read(jsonReader));
                    }
                    jsonReader.endArray();
                }
                case BEGIN_OBJECT, STRING, NUMBER, BOOLEAN -> list.add(elementAdapter.read(jsonReader));
                default -> throw new AssertionError("Unexpected token: " + token);
            }
            return list;
        }
    }

    /**
     * Load newline-delimited JSON file
     * @param filePath Path to the data file
     * @param v11
     */
    private void loadJSON(String filePath, boolean v11) {

        try {
            System.out.println("Loading file: " + filePath);

            Path input= Paths.get(filePath);
            InputStream inputStream = Files.newInputStream(input);
            JsonReader jsonReader = new JsonReader(new InputStreamReader(inputStream));

            if (v11) {
                parse_v11(jsonReader);
            } else {
                parse(jsonReader);
            }

            System.out.println("Done. Number of vertices: " + graph.getSize());
        }
        catch (Exception e)
        {
            System.out.println(e.getMessage());
            e.printStackTrace();
        }
    }

    private void parse_v11(JsonReader jsonReader) throws Exception {
        while (jsonReader.peek() != JsonToken.END_DOCUMENT) {
            jsonReader.setLenient(true);
            Paper paper = new Gson().fromJson(jsonReader, Paper.class);
            loadDataIntoGraph(paper);
        }
    }

    private void loadDataIntoGraph(Paper paper) {
        String paperId = paper.getId();

        if (paperId == null || paperId.length() == 0) {
            return;
        }

        DataVertex subjectVertex = (DataVertex) graph.getNode(paperId);

        if (subjectVertex==null) {
            subjectVertex = new DataVertex(paperId, "paper");
            graph.addVertex(subjectVertex);
        }

        if (paper.getTitle() != null && paper.getTitle().length() > 0) {
            subjectVertex.addAttribute("title", paper.getTitle());
        }

        if (paper.getAuthors() != null) {
            for (Author author : paper.getAuthors()) {
                if (author.getId() == null) continue;
                String authorId = author.getId();
                DataVertex objectVertex = (DataVertex) graph.getNode(authorId);
                if (objectVertex == null) {
                    objectVertex = new DataVertex(authorId, "author");
                    graph.addVertex(objectVertex);
                }
                if (author.getName() != null) {
                    objectVertex.addAttribute("name", author.getName());
                }
                if (author.getOrg() != null) {
                    objectVertex.addAttribute("org", author.getOrg());
                }
                graph.addEdge(subjectVertex, objectVertex, new RelationshipEdge("authored_by"));
            }
        }

        if (paper.getVenue() != null) {
            Venue venue = paper.getVenue();
            if (venue.getId() != null) {
                String venueId = venue.getId();
                DataVertex objectVertex = (DataVertex) graph.getNode(venueId);
                if (objectVertex == null) {
                    objectVertex = new DataVertex(venueId, "venue");
                    graph.addVertex(objectVertex);
                }
                if (venue.getName_d() != null) {
                    objectVertex.addAttribute("name", venue.getName_d());
                } else if (venue.getRaw() != null) {
                    objectVertex.addAttribute("name", venue.getRaw());
                }
                graph.addEdge(subjectVertex, objectVertex, new RelationshipEdge("paper_venue"));
            }
        }

        if (paper.getYear() != null) {
            subjectVertex.addAttribute("year", String.valueOf(paper.getYear()));
        }

        if (paper.getKeywords() != null) {
            for (String keyword: paper.getKeywords()) {
                DataVertex objectVertex = (DataVertex) graph.getNode(keyword);
                if (objectVertex == null) {
                    objectVertex = new DataVertex(keyword, "keyword");
                    graph.addVertex(objectVertex);
                }
                objectVertex.addAttribute("name", keyword);
                graph.addEdge(subjectVertex, objectVertex, new RelationshipEdge("has_keyword"));
            }
        }

        if (paper.getFos() != null) {
            for (Object obj : paper.getFos()) {
                if (obj instanceof FoS) {
                    FoS fos = (FoS) obj;
                    if (fos.getName() == null) continue;
                    if (fos.getName().length() == 0) continue;
                    String fosName = fos.getName();
                    DataVertex objectVertex = (DataVertex) graph.getNode(fosName);
                    if (objectVertex == null) {
                        objectVertex = new DataVertex(fosName, "field");
                        graph.addVertex(objectVertex);
                    }
                    objectVertex.addAttribute("name", fosName);
                    if (fos.getW() != null) {
                        objectVertex.addAttribute("weight", String.valueOf(fos.getW()));
                    }
                    graph.addEdge(subjectVertex, objectVertex, new RelationshipEdge("field_of_study"));
                } else if (obj instanceof String) {
                    String fosName = (String) obj;
                    if (fosName.length() == 0) continue;
                    DataVertex objectVertex = (DataVertex) graph.getNode(fosName);
                    if (objectVertex == null) {
                        objectVertex = new DataVertex(fosName, "field");
                        graph.addVertex(objectVertex);
                    }
                    objectVertex.addAttribute("name", fosName);
                    graph.addEdge(subjectVertex, objectVertex, new RelationshipEdge("field_of_study"));
                }
            }
        }

        if (paper.getReferences() != null) {
            for (String reference: paper.getReferences()) {
                DataVertex objectVertex = (DataVertex) graph.getNode(reference);
                if (objectVertex == null) {
                    objectVertex = new DataVertex(reference, "paper");
                    graph.addVertex(objectVertex);
                }
                graph.addEdge(subjectVertex, objectVertex, new RelationshipEdge("field_of_study"));
            }
        }

        if (paper.getN_citation() != null) {
            subjectVertex.addAttribute("citation_count", String.valueOf(paper.getN_citation()));
        }

        if (paper.getPage_start() != null && paper.getPage_start().length() > 0) {
            subjectVertex.addAttribute("page_start", paper.getPage_start());
        }

        if (paper.getPage_end() != null && paper.getPage_end().length() > 0) {
            subjectVertex.addAttribute("page_end", paper.getPage_end());
        }

        if (paper.getDoc_type() != null && paper.getDoc_type().length() > 0) {
            subjectVertex.addAttribute("doc_type", paper.getDoc_type());
        }

        if (paper.getLang() != null && paper.getLang().length() > 0) {
            subjectVertex.addAttribute("language", paper.getLang());
        }

        if (paper.getPublisher() != null && paper.getPublisher().length() > 0) {
            subjectVertex.addAttribute("publisher", paper.getPublisher());
        }

        if (paper.getVolume() != null && paper.getVolume().length() > 0) {
            subjectVertex.addAttribute("volume", paper.getVolume());
        }

        if (paper.getIssue() != null && paper.getIssue().length() > 0) {
            subjectVertex.addAttribute("issue", paper.getIssue());
        }

        if (paper.getIssn() != null && paper.getIssn().length() > 0) {
            subjectVertex.addAttribute("issn", paper.getIssn());
        }

        if (paper.getIsbn() != null && paper.getIsbn().length() > 0) {
            subjectVertex.addAttribute("isbn", paper.getIsbn());
        }

        if (paper.getDoi() != null && paper.getDoi().length() > 0) {
            subjectVertex.addAttribute("doi", paper.getDoi());
        }

        if (paper.getPdf() != null && paper.getPdf().length() > 0) {
            subjectVertex.addAttribute("pdf", paper.getPdf());
        }

        if (paper.getUrl() != null) {
            for (String url: paper.getUrl()) {
                DataVertex objectVertex = (DataVertex) graph.getNode(url);
                if (objectVertex == null) {
                    objectVertex = new DataVertex(url, "link");
                    graph.addVertex(objectVertex);
                }
                objectVertex.addAttribute("url", url);
                graph.addEdge(subjectVertex, objectVertex, new RelationshipEdge("has_url"));
            }
        }

        if (paper.get_abstract() != null && paper.get_abstract().length() > 0) {
            subjectVertex.addAttribute("abstract", paper.get_abstract());
        }

        // indexed abstract ?
    }

    private class Paper {
        @SerializedName(value="id", alternate={"_id"})
        private String id;
        private String title;
        private List<Author> authors;
        private Venue venue;
        private Integer year;
        private List<String> keywords;
        @JsonAdapter(ListTypeAdapterFactory.class)
        private List<Object> fos;
        private List<String> references;
        private Integer n_citation;
        private String page_start;
        private String page_end;
        private String doc_type;
        private String lang;
        private String publisher;
        private String volume;
        private String issue;
        private String issn;
        private String isbn;
        private String doi;
        private String pdf;
        private List<String> url;
        @SerializedName("abstract")
        private String _abstract;
        private IndexedAbstract indexed_abstract;

        public String getId() {
            return id;
        }

        public String getTitle() {
            return title;
        }

        public List<Author> getAuthors() {
            return authors;
        }

        public Venue getVenue() {
            return venue;
        }

        public Integer getYear() {
            return year;
        }

        public List<String> getKeywords() {
            return keywords;
        }

        public List<Object> getFos() {
            return fos;
        }

        public List<String> getReferences() {
            return references;
        }

        public Integer getN_citation() {
            return n_citation;
        }

        public String getPage_start() {
            return page_start;
        }

        public String getPage_end() {
            return page_end;
        }

        public String getDoc_type() {
            return doc_type;
        }

        public String getLang() {
            return lang;
        }

        public String getPublisher() {
            return publisher;
        }

        public String getVolume() {
            return volume;
        }

        public String getIssue() {
            return issue;
        }

        public String getIssn() {
            return issn;
        }

        public String getIsbn() {
            return isbn;
        }

        public String getDoi() {
            return doi;
        }

        public String getPdf() {
            return pdf;
        }

        public List<String> getUrl() {
            return url;
        }

        public String get_abstract() {
            return _abstract;
        }

        public IndexedAbstract getIndexed_abstract() {
            return indexed_abstract;
        }
    }

    private class Author {
        private String name;
        private String org;
        @SerializedName(value="id", alternate={"_id"})
        private String id;

        public String getName() {
            return name;
        }

        public String getOrg() {
            return org;
        }

        public String getId() {
            return id;
        }

    }

    private class Venue {
        @SerializedName(value="id", alternate={"_id"})
        private String id;
        private String raw;
        private String name_d;

        public String getId() {
            return id;
        }

        public String getRaw() {
            return raw;
        }

        public String getName_d() {
            return name_d;
        }
    }

    private class FoS {
        private String name;
        private Float w;

        public String getName() {
            return name;
        }

        public Float getW() {
            return w;
        }
    }

    private class IndexedAbstract {
        int IndexLength;
        Map<String,List<Integer>> InvertedIndex;
    }

}
