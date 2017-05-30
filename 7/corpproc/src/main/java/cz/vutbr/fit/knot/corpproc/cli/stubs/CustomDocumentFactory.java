package cz.vutbr.fit.knot.corpproc.cli.stubs;

import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.document.DocumentFactory;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import java.io.IOException;
import java.io.InputStream;
import java.io.Serializable;

public class CustomDocumentFactory implements DocumentFactory, Serializable {

    private static final long serialVersionUID = 2L;
    
    private String fieldName;

    @Override
    public int numberOfFields() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public String fieldName(int field) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public int fieldIndex(String fieldName) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public FieldType fieldType(int field) {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public Document getDocument(InputStream rawContent, Reference2ObjectMap<Enum<?>, Object> metadata) throws IOException {
        throw new UnsupportedOperationException("Not supported yet.");
    }

    @Override
    public DocumentFactory copy() {
        throw new UnsupportedOperationException("Not supported yet.");
    }
}
