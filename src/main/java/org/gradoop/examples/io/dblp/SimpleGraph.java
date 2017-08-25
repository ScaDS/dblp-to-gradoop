package org.gradoop.examples.io.dblp;

import com.koloboke.collect.map.hash.HashObjObjMap;
import com.koloboke.collect.map.hash.HashObjObjMaps;
import org.dblp.datastructures.DblpElement;
import org.dblp.parser.DblpParser;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.common.model.impl.properties.PropertyValue;
import org.gradoop.examples.io.dblp.callback.SimpleDblpProcessor;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

import java.util.List;

public class SimpleGraph {
    private final String LABEL_AUTHOR = "author";
    private final String LABEL_AUTHOR_NAME = "name";
    private final String PROP_TITLE = "title";

    private HashObjObjMap<String, ImportVertex> vertices = HashObjObjMaps.newMutableMap();
    private HashObjObjMap<String, ImportEdge> edges = HashObjObjMaps.newMutableMap();

    private List<DblpElement> parseData(String uri, long numElements) {
        SimpleDblpProcessor processor = new SimpleDblpProcessor(numElements);
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
            props.set(PROP_TITLE, PropertyValue.create(dblpElement.title));
            publicationVertex.setProperties(props);
            vertices.put(dblpElement.key, publicationVertex);

            for (String author : dblpElement.authors) {
                if (!vertices.containsKey(author)) {
                    // create author vertex if not existing
                    Properties authorProps = new Properties();
                    authorProps.set(LABEL_AUTHOR_NAME, author);

                    ImportVertex<String> authorVertex = new ImportVertex<>(author, LABEL_AUTHOR, authorProps);
                    vertices.put(author, authorVertex);
                }

                // add edges between author and publication
                String edgeKey = author + "|" + dblpElement.key;
                ImportEdge<String> edge = new ImportEdge<>(edgeKey, author, dblpElement.key);
                edges.put(edgeKey, edge);
            }
        } else {
            // if this case happens, the same key was found twice in dblp
            System.out.println("================================================================================");
            System.out.println("Key Duplication!");
            System.out.println("Element 1: " + vertices.get(dblpElement.key));
            System.out.println("Element 2: " + dblpElement);
            System.out.println("================================================================================");
        }
    }

    private void writeGraph(String graphHeadPath, String vertexPath, String edgePath) throws Exception {
        GraphCreationHelper.writeGraph(vertices, edges, graphHeadPath, vertexPath, edgePath);
    }


    public static void main(String[] args) throws Exception {

        if (args.length != 5) {
            System.out.println("Parameters: PathToDblpFile ElementsToParse OutputPath_Head OutputPath_Vertices, OutputPath_Edges");
            System.exit(0);
        }


        // get data from dblp xml file
        SimpleGraph dblp = new SimpleGraph();
        List<DblpElement> dblpElements = dblp.parseData(args[0], Long.parseLong(args[1]));

        System.out.println("Dblp Elements: " + dblpElements.size());

        // filter data which we don't want to add, e.g. no authors included etc.
        dblpElements.stream()
                .filter(ele -> ele.key != null)
                .filter(ele -> ele.authors.size() != 0)
                .filter(ele -> ele.title != null && !ele.title.equals(""))
                .forEach(dblp::createGraphStructure);


        dblp.writeGraph(args[2], args[3], args[4]);
    }
}
