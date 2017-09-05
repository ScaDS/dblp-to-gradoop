package org.gradoop.examples.io.dblp.stream;

import org.apache.flink.api.java.tuple.Tuple5;
import org.dblp.datastructures.DblpElement;
import org.gradoop.common.model.impl.properties.Properties;

/**
 * A replacement for {@link org.dblp.datastructures.DblpElement} as a {@link org.apache.flink.api.java.tuple.Tuple}.
 * T0: key,
 * T1: typeName,
 * T2: title,
 * T3: authors,
 * T4: properties
 */
public class DblpImportElement extends Tuple5<String, String, String, String[], Properties> {

    private static final String PROP_TITLE = "title";

    /**
     * Constructor for serialization.
     */
    public DblpImportElement() {

    }

    public DblpImportElement(String key, String typeName, String title, String[] authors, Properties properties) {
        super();
        f0 = key;
        f1 = typeName;
        f2 = title;
        f3 = authors;
        f4 = properties;
    }

    /**
     * Convert a {@link DblpElement} to a {@link DblpImportElement}.
     *
     * @param element The source element.
     * @return The target element.
     */
    public static DblpImportElement fromElement(DblpElement element) {
        Properties prop = new Properties();
        for (String attrKey : element.attributes.keySet()) {
            element.attributes.get(attrKey)
                    .forEach(attribute -> prop.set(attrKey, attribute));
        }
        prop.set(PROP_TITLE, element.title);
        return new DblpImportElement(element.key, element.getType().name(), element.title,
                element.authors.toArray(new String[element.authors.size()]), prop);
    }

    public String[] getAuthors() {
        return f3;
    }

    public String getKey() {
        return f0;
    }

    public Properties getProperties() {
        return f4;
    }

    public String getTitle() {
        return f2;
    }

    public String getType() {
        return f1;
    }
}
