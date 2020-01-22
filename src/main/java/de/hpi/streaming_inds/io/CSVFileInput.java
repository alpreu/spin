package de.hpi.streaming_inds.io;

import de.metanome.algorithm_integration.input.RelationalInput;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class CSVFileInput implements RelationalInput {
    private final CSVParser parser;
    final Iterator<CSVRecord> iterator;
    List<String> currentLine;

    CSVFileInput(String filename, char delimiter, boolean skipFirstLine) throws IOException {
        CSVFormat csvFormat = CSVFormat
                .DEFAULT
                .withEscape('\\')
                .withDelimiter(delimiter);
        this.parser = CSVParser.parse(new BufferedReader(new FileReader(filename)), csvFormat);
        this.iterator = this.parser.iterator();
        if (skipFirstLine) iterator.next();
    }

    CSVFileInput(String filename, char delimiter, boolean skipFirstLine, boolean ignoreSurroundingSpaces) throws IOException {
        CSVFormat csvFormat = CSVFormat
                .DEFAULT
                .withEscape('\\')
                .withDelimiter(delimiter)
                .withIgnoreSurroundingSpaces(ignoreSurroundingSpaces);
        this.parser = CSVParser.parse(new BufferedReader(new FileReader(filename)), csvFormat);
        this.iterator = this.parser.iterator();
        if (skipFirstLine) iterator.next();
    }

    CSVFileInput(String filename, char delimiter, boolean skipFirstLine, boolean ignoreSurroundingSpaces, boolean trim) throws IOException {
        CSVFormat csvFormat = CSVFormat
                .DEFAULT
                .withEscape('\\')
                .withDelimiter(delimiter)
                .withIgnoreSurroundingSpaces(ignoreSurroundingSpaces)
                .withTrim(trim);
        this.parser = CSVParser.parse(new BufferedReader(new FileReader(filename)), csvFormat);
        this.iterator = this.parser.iterator();
        if (skipFirstLine) iterator.next();
    }


    @Override
    public boolean hasNext() {
        return this.iterator.hasNext();
    }

    @Override
    public List<String> next() {
        if (!this.hasNext()) { //FIXME: dirty hack but should keep IND validity the same
            return this.currentLine;
        }

        List<String> next = new ArrayList<>();
        CSVRecord nextRecord = this.iterator.next();
        for (String column: nextRecord) {
            next.add(column);
        }
        this.currentLine = next;
        return next;
    }

    @Override
    public int numberOfColumns()  {
        return currentLine.size();
    }

    @Override
    public String relationName() {
        throw new RuntimeException("not implemented");
    }

    @Override
    public List<String> columnNames() {
        throw new RuntimeException("not implemented");
    }

    @Override
    public void close() throws Exception {
        this.parser.close();
    }
}
