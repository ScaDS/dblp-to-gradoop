package org.gradoop.examples.io.dblp.stream;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;
import org.gradoop.flink.io.impl.graph.tuples.ImportEdge;

/**
 * Extract {@link ImportEdge}s from {@link DblpImportElement}s.
 */
public class DblpElementToEdge implements FlatMapFunction<DblpImportElement, ImportEdge<String>> {

    private final String LABEL_AUTHOR = "author";

    @Override
    public void flatMap(DblpImportElement value, Collector<ImportEdge<String>> out) {
        for (String author : value.getAuthors()) {
            out.collect(new ImportEdge<>(LABEL_AUTHOR + '|' + value.getKey(), author, value.getKey()));
        }
    }
}
