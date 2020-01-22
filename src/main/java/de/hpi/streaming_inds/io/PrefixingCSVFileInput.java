package de.hpi.streaming_inds.io;

import org.apache.commons.csv.CSVRecord;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Equivalent to the CSVFileInput, except it prefixes all rows with an additional metadata column specifying it to be an insert
 */
public class PrefixingCSVFileInput extends CSVFileInput {


    PrefixingCSVFileInput(String filename, char delimiter, boolean skipFirstLine) throws IOException {
        super(filename, delimiter, skipFirstLine);
    }

    PrefixingCSVFileInput(String filename, char delimiter, boolean skipFirstLine, boolean ignoreSurroundingSpaces) throws IOException {
        super(filename, delimiter, skipFirstLine, ignoreSurroundingSpaces);
    }


    PrefixingCSVFileInput(String filename, char delimiter, boolean skipFirstLine, boolean ignoreSurroundingSpaces, boolean trim) throws IOException {
        super(filename, delimiter, skipFirstLine, ignoreSurroundingSpaces, trim);
    }

    @Override
    public List<String> next() {
        if (!this.hasNext()) { //FIXME: dirty hack but should keep IND validity the same
            return this.currentLine;
        }

        List<String> next = new ArrayList<>();

        next.add("insert");

        CSVRecord nextRecord = this.iterator.next();
        for (String column: nextRecord) {
            next.add(column);
        }
        this.currentLine = next;
        return next;
    }
}
