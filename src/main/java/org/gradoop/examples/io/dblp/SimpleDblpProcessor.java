package org.gradoop.examples.io.dblp;

import org.dblp.datastructures.DblpElement;
import org.dblp.datastructures.converter.DblpElementConverter;
import org.dblp.parser.DblpElementProcessor;
import org.dblp.parser.ParsingTerminationException;
import org.xml.sax.SAXException;

import java.util.ArrayList;
import java.util.List;

/**
 * A simple DBLP element processor. Just parsing and converting the elements and storing them in a list
 */
public class SimpleDblpProcessor implements DblpElementProcessor {
    private List<DblpElement> elementList;
    private final long maxElementsToParse;

    SimpleDblpProcessor(long elements) {
        this.elementList = new ArrayList<>();
        this.maxElementsToParse = elements;
    }

    @Override
    public void process(DblpElement dblpElement) throws SAXException {
        DblpElementConverter conv = dblpElement.getType().getConverter();
        elementList.add(conv.convertEssentials(dblpElement));

        if(maxElementsToParse != 0 && elementList.size() >= maxElementsToParse) {
            throw new ParsingTerminationException();
        }
    }

    List<DblpElement> getElementList() {
        return elementList;
    }
}
