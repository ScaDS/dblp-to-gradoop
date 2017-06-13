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

    public FilterDblpProcessor(long elements, String fieldName, List<String> acceptedText, Set<DblpElementType> acceptedTypes) {
        this.elementList = new ArrayList<>();
        this.maxElementsToParse = elements;
        this.acceptedTypes = acceptedTypes;
        this.fieldName = fieldName;
        this.acceptedText = acceptedText;
    }

    @Override
    public void process(DblpElement dblpElement) throws SAXException {
        if(acceptedTypes.contains(dblpElement.getType())) {
            DblpElementConverter conv = dblpElement.getType().getConverter();
            DblpElement element = conv.convertEssentials(dblpElement);

            if(element.key != null && element.authors.size() != 0
                && element.title != null && !element.title.equals("")) {

                // if one of the values of the specified field contains an accepted substring text, we add the element
                if(element.attributes.get(fieldName).stream().anyMatch(s -> acceptedText.stream().anyMatch(a -> s.toLowerCase().contains(a.toLowerCase())))) {
                    elementList.add(element);
                }


                if(maxElementsToParse != 0 && elementList.size() >= maxElementsToParse) {
                    throw new ParsingTerminationException();
                }
            }
        }
    }

    public List<DblpElement> getElementList() {
        return elementList;
    }
}
