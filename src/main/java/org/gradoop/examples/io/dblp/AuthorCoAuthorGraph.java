package org.gradoop.examples.io.dblp;

import com.koloboke.collect.map.hash.HashObjObjMap;
import com.koloboke.collect.map.hash.HashObjObjMaps;
import org.dblp.datastructures.DblpElement;
import org.dblp.datastructures.DblpElementType;
import org.dblp.parser.DblpParser;
import org.gradoop.common.model.impl.properties.*;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.examples.io.dblp.callback.FilterDblpProcessor;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

import java.util.*;


/**
 * Creates a graph based on a given author with all his co-authors and publications in DBLP.
 */
public class AuthorCoAuthorGraph {

    private HashObjObjMap<String, ImportVertex> vertices = HashObjObjMaps.newMutableMap();
    private HashObjObjMap<String, ImportEdge> edges = HashObjObjMaps.newMutableMap();

    private List<DblpElement> parseData(String uri, List<String> authors) {

        FilterDblpProcessor processor =
                new FilterDblpProcessor(0, "author", authors, EnumSet.allOf(DblpElementType.class), true);
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
            publicationVertex.setProperties(props);
            vertices.put(dblpElement.key, publicationVertex);

            // create all author vertices that do not exist yet
            for (String author : dblpElement.authors) {
                if (!vertices.containsKey(author)) {
                    org.gradoop.common.model.impl.properties.Properties authorProps = new org.gradoop.common.model.impl.properties.Properties();
                    String LABEL_AUTHOR_NAME = "name";
                    authorProps.set(LABEL_AUTHOR_NAME, author);

                    String LABEL_AUTHOR = "author";
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
                    String EDGE_PROPERTY_COLABS = "collaborations";
                    if(!edges.containsKey(edgeKey)) {
                        org.gradoop.common.model.impl.properties.Properties authorProps = new org.gradoop.common.model.impl.properties.Properties();
                        authorProps.set(EDGE_PROPERTY_COLABS, 1);
                        authorProps.set("connected_authors", author + "|" + author2);
                        String EDGE_LABEL_COAUTHOR = "coauthor";
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
                String EDGE_LABEL_AUTHOR = "authorof";
                ImportEdge<String> edge = new ImportEdge<>(edgeKey, author, dblpElement.key, EDGE_LABEL_AUTHOR);
                edges.put(edgeKey, edge);
            }
        }
    }

    private void writeGraph(String outputPath) throws Exception {
        GraphCreationHelper.writeGraph(vertices, edges, outputPath + "graphHead",outputPath + "vertices", outputPath + "edges");
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.out.println(
                    "Parameters: PathToDblpFile OutputFolder [Author List (substring check)]");
            System.out.println("At least one Author has to be stated.");
            System.exit(0);
        }

        List<String> authors = new ArrayList<>();
        Collections.addAll(authors, Arrays.copyOfRange(args, 2, args.length));
        System.out.println("Authors: " + authors);

        // get data from dblp xml file
        AuthorCoAuthorGraph graphCreator = new AuthorCoAuthorGraph();
        List<DblpElement> dblpElements = graphCreator.parseData(args[0], authors);

        System.out.println("Dblp Elements: " + dblpElements.size());

        // filter data which we don't want to add, e.g. no authors included etc.
        dblpElements.forEach(graphCreator::createGraphStructure);
        graphCreator.writeGraph(args[1]);
    }
}
