package org.gradoop.examples.io.dblp;

import com.koloboke.collect.map.hash.HashObjObjMap;
import com.koloboke.collect.map.hash.HashObjObjMaps;
import org.dblp.datastructures.DblpElement;
import org.dblp.datastructures.DblpElementType;
import org.dblp.parser.DblpElementProcessor;
import org.dblp.parser.DblpParser;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.examples.io.dblp.callback.FilterDblpProcessor;
import org.gradoop.examples.io.dblp.callback.SimpleDblpProcessor;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

import java.util.*;

/**
 * Beside the ordinary graph structure which connects a publication with it's authors, this graph contains
 * the co-author relations. The graph is build by using INPROCEEDINGS and possibly only a set of predefined conferences.
 */
public class InprocCoAuthorGraph {
    private final String LABEL_AUTHOR = "author";
    private final String LABEL_AUTHOR_NAME = "name";
    private final String EDGE_LABEL_COAUTHOR = "coauthor";
    private final String EDGE_LABEL_AUTHOR = "authorof";
    private final String EDGE_PROPERTY_COLABS = "colaborations";
    private final String PROP_TITLE = "title";

    private HashObjObjMap<String, ImportVertex> vertices = HashObjObjMaps.newMutableMap();
    private HashObjObjMap<String, ImportEdge> edges = HashObjObjMaps.newMutableMap();

    private List<DblpElement> parseData(String uri, long numElements, List<String> conferences) {

        FilterDblpProcessor processor =
                new FilterDblpProcessor(numElements, "booktitle", conferences, EnumSet.of(DblpElementType.INPROCEEDINGS), false);
        DblpParser.load(processor, uri);

        return processor.getElementList();
    }

    private void createGraphStructure(DblpElement dblpElement) {
        // create new publication and author vertex if not present
        if (!vertices.containsKey(dblpElement.key)) {
            // create publication vertex
            ImportVertex<String> publicationVertex = new ImportVertex<>(dblpElement.key, dblpElement.getType().name());
            // add properties to the publication vertex
            Properties props = new Properties();
            for (String attrKey : dblpElement.attributes.keySet()) {
                dblpElement.attributes.get(attrKey)
                        .forEach(attribute -> props.set(attrKey, attribute));
            }
            if(dblpElement.title != null && !dblpElement.title.isEmpty()) {
                props.set(PROP_TITLE, PropertyValue.create(dblpElement.title));
            }
            publicationVertex.setProperties(props);
            vertices.put(dblpElement.key, publicationVertex);

            // create all author vertices that do not exist yet
            for (String author : dblpElement.authors) {
                if (!vertices.containsKey(author)) {
                    Properties authorProps = new Properties();
                    authorProps.set(LABEL_AUTHOR_NAME, author);

                    ImportVertex<String> authorVertex = new ImportVertex<>(author, LABEL_AUTHOR, authorProps);
                    vertices.put(author, authorVertex);
                }
            }

            // create edges between authors and publication
            for (String author : dblpElement.authors) {
                // co-author relations
                for(String author2 : dblpElement.authors) {
                    if(author == author2) {
                        continue;
                    }
                    String edgeKey = author + "|" + author2;
                    if(!edges.containsKey(edgeKey)) {
                        Properties authorProps = new Properties();
                        authorProps.set(EDGE_PROPERTY_COLABS, 1);
                        authorProps.set("connected_authors", author + "|" + author2);
                        ImportEdge<String> coAuthorEdge = new ImportEdge<>(edgeKey, author, author2, EDGE_LABEL_COAUTHOR, authorProps);
                        edges.put(edgeKey, coAuthorEdge);
                    } else {
                        ImportEdge importEdge = edges.get(edgeKey);
                        importEdge.getProperties()
                                .set(EDGE_PROPERTY_COLABS, importEdge.getProperties().get(EDGE_PROPERTY_COLABS).getInt() +1);
                    }
                }

                // add edges between author and publication
                String edgeKey = author + "|" + dblpElement.key;
                ImportEdge<String> edge = new ImportEdge<>(edgeKey, author, dblpElement.key, EDGE_LABEL_AUTHOR);
                edges.put(edgeKey, edge);
            }
        }
    }


    private void writeGraph(String outPath) throws Exception {
        GraphCreationHelper.writeGraph(vertices, edges, outPath);
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 3) {
            System.out.println(
                    "Parameters: PathToDblpFile NumElementsToParse OutputPath_Head OutputPath_Vertices OutputPath_Edges "
                    + "[Conference List (substring check)]");
            System.out.println("If NumElements is set to '0', all elements are parsed.");
            System.out.println("If no Conferences are stated all are parsed.");
            System.exit(0);
        }

        List<String> conferences = new ArrayList<>();
        if(args.length > 3) {
            Collections.addAll(conferences, Arrays.copyOfRange(args, 5, args.length));
        }
        System.out.println("Conferences: " + conferences);

        // get data from dblp xml file
        InprocCoAuthorGraph graphCreator = new InprocCoAuthorGraph();
        List<DblpElement> dblpElements = graphCreator.parseData(args[0], Long.parseLong(args[1]), conferences);

        System.out.println("Dblp Elements: " + dblpElements.size());

        // filter data which we don't want to add, e.g. no authors included etc.
        dblpElements.forEach(graphCreator::createGraphStructure);
        graphCreator.writeGraph(args[2]);
    }
}
