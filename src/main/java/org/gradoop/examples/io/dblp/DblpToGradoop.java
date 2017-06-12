package org.gradoop.examples.io.dblp;

import com.koloboke.collect.map.hash.HashObjObjMap;
import com.koloboke.collect.map.hash.HashObjObjMaps;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.dblp.datastructures.DblpElement;
import org.dblp.parser.DblpParser;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.List;

public class DblpToGradoop {
    private final String LABEL_AUTHOR = "author";

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
            publicationVertex.setProperties(props);
            vertices.put(dblpElement.key, publicationVertex);

            for (String author : dblpElement.authors) {
                if (!vertices.containsKey(author)) {
                    // create author vertex if not existing
                    Properties authorProps = new Properties();
                    authorProps.set("name", author);

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
        // translate to flink datastructures
        final ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();

        // create default Gradoop config
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);

        System.out.println("Vertices: " + vertices.size());
        System.out.println("Edges   : " + edges.size());

        DataSet<ImportVertex> v = env.fromCollection(vertices.values());
        DataSet<ImportEdge> e = env.fromCollection(edges.values());

        DataSource gds = new GraphDataSource(v, e, config);

        // read logical graph
        LogicalGraph logicalGraph = gds.getLogicalGraph();

//        DataSink ds1 = new DOTDataSink("/home/kricke/IdeaProjects/asd.dot", true);
//        ds1.write(logicalGraph);

        DataSink ds = new JSONDataSink(graphHeadPath, vertexPath, edgePath, config);
        ds.write(logicalGraph);

        env.execute();
    }


    public static void main(String[] args) throws Exception {

        if (args.length != 5) {
            System.out.println("Parameters: PathToDblpFile ElementsToParse OutputPath_Head OutputPath_Vertices, OutputPath_Edges");
            System.exit(0);
        }


        // get data from dblp xml file
        DblpToGradoop dblp = new DblpToGradoop();
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
