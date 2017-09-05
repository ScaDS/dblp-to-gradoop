package org.gradoop.examples.io.dblp.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.common.model.impl.properties.Properties;
import org.gradoop.flink.io.impl.graph.tuples.ImportVertex;

/**
 * Extract {@link ImportVertex ImportVertices} from {@link DblpImportElement}s.
 */
public class DblpElementToVertex implements FlatMapFunction<DblpImportElement, ImportVertex<String>> {

    private static final String LABEL_AUTHOR_NAME = "name";
    private static final String PROP_TITLE = "title";

    @Override
    public void flatMap(DblpImportElement value, Collector<ImportVertex<String>> out) {
        // Publication vertex.
        out.collect(new ImportVertex<>(value.getKey(), value.getType(), value.getProperties()));
        for (String author : value.getAuthors()) {
            Properties authorProp = Properties.create();
            authorProp.set(LABEL_AUTHOR_NAME, author);
            out.collect(new ImportVertex<>(author, LABEL_AUTHOR_NAME, authorProp));
        }

    }
}
