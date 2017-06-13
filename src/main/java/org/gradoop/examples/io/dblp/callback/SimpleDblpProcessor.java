package org.gradoop.examples.io.dblp.callback;

import org.dblp.datastructures.DblpElement;
import org.dblp.datastructures.DblpElementType;
import org.dblp.datastructures.converter.DblpElementConverter;
import org.dblp.parser.DblpElementProcessor;
import org.dblp.parser.ParsingTerminationException;
import org.xml.sax.SAXException;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * A simple DBLP element processor. Just parsing and converting the elements and storing them in a list
 */
public class SimpleDblpProcessor implements DblpElementProcessor {
    private List<DblpElement> elementList;
    private final long maxElementsToParse;
    private final Set<DblpElementType> acceptedTypes;

    public SimpleDblpProcessor(long elements) {
        this.elementList = new ArrayList<>();
        this.maxElementsToParse = elements;
        this.acceptedTypes = EnumSet.allOf(DblpElementType.class);
    }

    public SimpleDblpProcessor(long elements, Set<DblpElementType> acceptedTypes) {
        this.elementList = new ArrayList<>();
        this.maxElementsToParse = elements;
        this.acceptedTypes = acceptedTypes;
    }

    @Override
    public void process(DblpElement dblpElement) throws SAXException {
        if(acceptedTypes.contains(dblpElement.getType())) {
            DblpElementConverter conv = dblpElement.getType().getConverter();
            DblpElement element = conv.convertEssentials(dblpElement);

//            if(element.key != null && element.authors.size() != 0
//                && element.title != null && !element.title.equals("")) {

                elementList.add(element);

                if(maxElementsToParse != 0 && elementList.size() >= maxElementsToParse) {
                    throw new ParsingTerminationException();
                }
//            }
        }
    }

    public List<DblpElement> getElementList() {
        return elementList;
    }
}
