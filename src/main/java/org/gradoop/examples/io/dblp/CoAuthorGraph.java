package org.gradoop.examples.io.dblp;

import com.koloboke.collect.map.hash.HashObjObjMap;
import com.koloboke.collect.map.hash.HashObjObjMaps;
import org.dblp.datastructures.DblpElement;
import org.dblp.datastructures.DblpElementType;
import org.dblp.parser.DblpParser;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.examples.io.dblp.callback.SimpleDblpProcessor;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

import java.util.*;

/**
 * Beside the ordinary graph structure which connects a publication with it#s authors, this graph contains
 * the co-author relations.
 */
public class CoAuthorGraph  {
    private final String LABEL_AUTHOR = "author";
    private final String LABEL_AUTHOR_NAME = "name";
    private final String EDGE_LABEL_COAUTHOR = "co-author";
    private final String EDGE_LABEL_AUTHOR = "author-of";

    private HashObjObjMap<String, ImportVertex> vertices = HashObjObjMaps.newMutableMap();
    private HashObjObjMap<String, ImportEdge> edges = HashObjObjMaps.newMutableMap();

    private List<DblpElement> parseData(String uri, long numElements, Set<DblpElementType> publicationTypes) {
        SimpleDblpProcessor processor = new SimpleDblpProcessor(numElements, publicationTypes);
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
                    String edgeKey = author + "|" + author2;
                    ImportEdge<String> coAuthorEdge = new ImportEdge<>(edgeKey, author, author2, EDGE_LABEL_COAUTHOR);
                    edges.put(edgeKey, coAuthorEdge);
                }

                // add edges between author and publication
                String edgeKey = author + "|" + dblpElement.key;
                ImportEdge<String> edge = new ImportEdge<>(edgeKey, author, dblpElement.key, EDGE_LABEL_AUTHOR);
                edges.put(edgeKey, edge);
            }
        }
    }


    private void writeGraph(String graphHeadPath, String vertexPath, String edgePath) throws Exception {
        GraphCreationHelper.writeGraph(vertices, edges, graphHeadPath, vertexPath, edgePath);
    }

    public static void main(String[] args) throws Exception {

        if (args.length < 5) {
            System.out.println(
                    "Parameters: PathToDblpFile NumElementsToParse OutputPath_Head OutputPath_Vertices OutputPath_Edges "
                    + "[Publication Types (INPROCEEDINGS, PROCEEDINGS, ARTICLE, INCOLLECTION, WEBSITE, BOOK)]");
            System.out.println("If NumElements is set to '0', all elements are parsed.");
            System.out.println("If no Publication Type is stated all types are parsed.");
            System.exit(0);
        }

        Set<DblpElementType> publicationTypes;
        if(args.length > 5) {
            publicationTypes = new HashSet<>();
            for(String pubType : Arrays.copyOfRange(args, 5, args.length)) {
                publicationTypes.add(DblpElementType.valueOf(pubType));
            }
        } else {
            publicationTypes = EnumSet.allOf(DblpElementType.class);
        }
        System.out.println("PubTypes: " + publicationTypes);

        // get data from dblp xml file
        CoAuthorGraph graphCreator = new CoAuthorGraph();
        List<DblpElement> dblpElements = graphCreator.parseData(args[0], Long.parseLong(args[1]), publicationTypes);

        System.out.println("Dblp Elements: " + dblpElements.size());

        // filter data which we don't want to add, e.g. no authors included etc.
        dblpElements.stream()
//                .filter(ele -> !publicationTypes.contains(ele.type))
                .filter(ele -> ele.key != null)
                .filter(ele -> ele.authors.size() != 0)
                .filter(ele -> ele.title != null && !ele.title.equals(""))
                .forEach(graphCreator::createGraphStructure);


        graphCreator.writeGraph(args[2], args[3], args[4]);
    }
}
