package org.gradoop.examples.io.dblp.stream;

import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.dblp.datastructures.DblpElement;
import org.dblp.parser.DblpParser;
import org.gradoop.examples.io.dblp.callback.SimpleDblpProcessor;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.model.api.epgm.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A replacement for {@link org.gradoop.examples.io.dblp.SimpleGraph} that uses Flink.
 */
public class FlinkSimpleGraph {

    private static List<DblpElement> parseData(String uri, long numElements) {
        SimpleDblpProcessor processor = new SimpleDblpProcessor(numElements);
        DblpParser.load(processor, uri);
        return processor.getElementList();
    }

    public static void main(String[] args) throws Exception {
        if (args.length != 3) {
            System.err.println("Usage: FlinkSimpleGraph INPUT_PATH ELEMENT_COUNT OUTPUT_PATH");
            return;
        }
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        GradoopFlinkConfig config = GradoopFlinkConfig.createConfig(env);
        List<DblpImportElement> elementsIt = parseData(args[0], Long.parseLong(args[1])).stream()
                .filter(ele -> ele.key != null)
                .filter(ele -> ele.authors.size() != 0)
                .filter(ele -> ele.title != null && !ele.title.equals(""))
                .map(DblpImportElement::fromElement).collect(Collectors.toCollection(ArrayList::new));
        DataSet<DblpImportElement> elements = env.fromCollection(elementsIt);
        DataSet<ImportEdge<String>> edges = elements.flatMap(new DblpElementToEdge());
        DataSet<ImportVertex<String>> vertices = elements.flatMap(new DblpElementToVertex());
        LogicalGraph graph = new GraphDataSource<>(vertices, edges, config).getLogicalGraph();
        new JSONDataSink(args[2], config).write(graph, true);
        env.execute();
    }
}
