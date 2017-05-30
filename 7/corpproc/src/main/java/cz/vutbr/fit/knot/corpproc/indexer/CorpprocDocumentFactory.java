package cz.vutbr.fit.knot.corpproc.indexer;

import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.document.IdentityDocumentFactory;
import it.unimi.di.big.mg4j.document.PropertyBasedDocumentFactory;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectOpenHashMap;
import it.unimi.dsi.io.FastBufferedReader;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.nio.charset.StandardCharsets;

public class CorpprocDocumentFactory extends PropertyBasedDocumentFactory {

    private static final long serialVersionUID = 2L;

    public CorpprocDocumentFactory() {
        super(new Reference2ObjectOpenHashMap<>(
                new PropertyBasedDocumentFactory.MetadataKeys[]{PropertyBasedDocumentFactory.MetadataKeys.ENCODING},
                new String[]{"UTF-8"}
        ));
    }

    @Override
    public IdentityDocumentFactory copy() {
        return new IdentityDocumentFactory(defaultMetadata);
    }

    @Override
    public int numberOfFields() {
        return 1;
    }

    @Override
    public String fieldName(final int field) {
        ensureFieldIndex(field);
        return "text";
    }

    @Override
    public int fieldIndex(final String fieldName) {
        return fieldName.equals("text") ? 0 : -1;
    }

    @Override
    public FieldType fieldType(final int field) {
        ensureFieldIndex(field);
        return null; // This should be FieldType.TOKEN, though that isn't available in mg4j-big
    }

    private class CustomIterator implements TokenIterator {

        private final InputStreamReader reader;
        private int actual = 0;
        private boolean atEnd = false;

        public CustomIterator(InputStream stream) {
            reader = new InputStreamReader(stream, StandardCharsets.UTF_8);
        }

        @Override
        public InputStreamReader getContent() {
            return reader;
        }

        @Override
        public int nextInt() {
            return actual;
        }

        @Override
        public int skip(int arg0) {
            return 0;
        }

        @Override
        public boolean hasNext() {
            return !atEnd;
        }

        @Override
        public Integer next() {
            return null;
        }

        @Override
        public void remove() {
        }

        @Override
        public MutableString token() {
            try {
                StringBuilder builder = new StringBuilder();
                char c = 0;
                int data;
                while ((data = reader.read()) != -1) {
                    c = (char) data;
                    if (Character.isWhitespace(c) || c == '|') {
                        break;
                    }
                    builder.append(c);
                }
                if (data == -1) {
                    atEnd = true;
                    reader.close();
                }
                if (c != '|') {
                    actual++;
                }
                return new MutableString(builder.toString());
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }
    }

    public static class WhitespaceWordReader extends FastBufferedReader {

        @Override
        protected boolean isWordConstituent(final char c) {
            return !Character.isWhitespace(c);
        }
    }

    @Override
    public Document getDocument(final InputStream rawContent, final Reference2ObjectMap<Enum<?>, Object> metadata) {
        return new Document() {

            @Override
            public CharSequence title() {
                return (CharSequence) resolve(PropertyBasedDocumentFactory.MetadataKeys.TITLE, metadata);
            }

            @Override
            public String toString() {
                return title().toString();
            }

            @Override
            public CharSequence uri() {
                return (CharSequence) resolve(PropertyBasedDocumentFactory.MetadataKeys.URI, metadata);
            }

            @Override
            public Object content(final int field) {
                ensureFieldIndex(field);
                return new CustomIterator(rawContent);
            }

            @Override
            public WordReader wordReader(final int field) {
                ensureFieldIndex(field);
                return new WhitespaceWordReader();
            }

            @Override
            public void close() {
            }
        };
    }

    private void readObject(final ObjectInputStream s) throws IOException, ClassNotFoundException {
        s.defaultReadObject();
    }
}
