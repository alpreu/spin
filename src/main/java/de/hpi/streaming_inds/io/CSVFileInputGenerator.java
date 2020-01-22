package de.hpi.streaming_inds.io;

import de.metanome.algorithm_integration.input.RelationalInput;
import de.metanome.algorithm_integration.input.RelationalInputGenerator;

import java.io.IOException;

public class CSVFileInputGenerator implements RelationalInputGenerator {

    private RelationalInput relationalInput;

    public CSVFileInputGenerator(String filename, char delimiter, boolean skipFirstLine,
                                 boolean needsPrefixing, boolean ignoreSurroundingSpaces, boolean trim) throws IOException {
        if (needsPrefixing) {
            if (ignoreSurroundingSpaces) {
                if (trim) {
                    this.relationalInput = new PrefixingCSVFileInput(filename, delimiter, skipFirstLine, true, true);
                } else {
                    this.relationalInput = new PrefixingCSVFileInput(filename, delimiter, skipFirstLine, true);
                }
            } else {
                this.relationalInput = new PrefixingCSVFileInput(filename, delimiter, skipFirstLine);
            }
        } else {
            if (ignoreSurroundingSpaces) {
                if (trim) {
                    this.relationalInput = new CSVFileInput(filename, delimiter, skipFirstLine, true, true);
                } else {
                    this.relationalInput = new CSVFileInput(filename, delimiter, skipFirstLine, true);
                }
            } else {
                this.relationalInput = new CSVFileInput(filename, delimiter, skipFirstLine);
            }
        }
    }

    @Override
    public RelationalInput generateNewCopy() {
        return relationalInput;
    }

    @Override
    public void close() throws Exception {
        this.relationalInput.close();
    }
}
