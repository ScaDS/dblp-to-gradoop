package org.gradoop.examples.io.dblp.callback;

import org.dblp.datastructures.DblpElement;
import org.dblp.datastructures.DblpElementType;
import org.dblp.datastructures.converter.DblpElementConverter;
import org.dblp.parser.DblpElementProcessor;
import org.dblp.parser.ParsingTerminationException;
import org.xml.sax.SAXException;

import java.util.*;

/**
 * A DBLP element processor which . Just parsing and converting the elements and storing them in a list
 */
public class FilterDblpProcessor implements DblpElementProcessor {
    private List<DblpElement> elementList;
    private final long maxElementsToParse;
    private final Set<DblpElementType> acceptedTypes;
    private final String fieldName;
    private final List<String> acceptedText;
    private final boolean exactMatch;

    public FilterDblpProcessor(long elements, String fieldName, List<String> acceptedText, Set<DblpElementType> acceptedTypes, boolean exactMatch) {
        this.elementList = new ArrayList<>();
        this.maxElementsToParse = elements;
        this.acceptedTypes = acceptedTypes;
        this.fieldName = fieldName;
        this.acceptedText = acceptedText;
        this.exactMatch = exactMatch;
    }

    @Override
    public void process(DblpElement dblpElement) throws SAXException {
        if(acceptedTypes.contains(dblpElement.getType())) {

            // if one of the values of the specified field contains an accepted substring text, we add the element
            boolean accept;
            if(exactMatch) {
                accept = dblpElement.attributes.get(fieldName).stream().anyMatch(s -> acceptedText.stream().anyMatch(a -> s.toLowerCase().equals(a.toLowerCase())));
            } else {
                accept = dblpElement.attributes.get(fieldName).stream().anyMatch(s -> acceptedText.stream().anyMatch(a -> s.toLowerCase().contains(a.toLowerCase())));
            }

            if(accept) {
                DblpElement element = dblpElement.getType().getConverter().convertEssentials(dblpElement);
                if(element.key != null && element.authors.size() != 0
                        && element.title != null && !element.title.equals("")) {

                    elementList.add(element);
                }
            }

            if(maxElementsToParse != 0 && elementList.size() >= maxElementsToParse) {
                throw new ParsingTerminationException();
            }
        }
    }

    public List<DblpElement> getElementList() {
        return elementList;
    }
}
