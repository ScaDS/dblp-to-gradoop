package org.gradoop.examples.io.dblp;

import com.koloboke.collect.map.hash.HashObjObjMap;
import org.apache.flink.api.java.DataSet;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.gradoop.flink.io.api.DataSink;
import org.gradoop.flink.io.api.DataSource;
import org.gradoop.flink.io.impl.graph.GraphDataSource;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;
import org.gradoop.flink.io.impl.json.JSONDataSink;
import org.gradoop.flink.model.impl.LogicalGraph;
import org.gradoop.flink.util.GradoopFlinkConfig;

/**
 * Simple shared utility functions.
 */
public class GraphCreationHelper {
    public static void writeGraph(HashObjObjMap<String, ImportVertex> vertices, HashObjObjMap<String, ImportEdge> edges,
                                  String graphHeadPath, String vertexPath, String edgePath) throws Exception {
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

        DataSink ds = new JSONDataSink(graphHeadPath, vertexPath, edgePath, config);
        ds.write(logicalGraph, true);

        env.execute();
    }
}
