package cz.vutbr.fit.knot.corpproc.indexer;

import it.unimi.di.big.mg4j.document.AbstractDocumentCollection;
import it.unimi.di.big.mg4j.document.AbstractDocumentIterator;
import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.document.DocumentFactory;
import it.unimi.di.big.mg4j.document.DocumentIterator;
import it.unimi.di.big.mg4j.document.PropertyBasedDocumentFactory.MetadataKeys;
import it.unimi.dsi.fastutil.bytes.ByteArrays;
import it.unimi.dsi.fastutil.io.FastBufferedInputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayInputStream;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2ObjectArrayMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import it.unimi.dsi.io.MultipleInputStream;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.sux4j.util.EliasFanoMonotoneLongBigList;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.Serializable;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.zip.GZIPInputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CorpprocDocumentCollection extends AbstractDocumentCollection implements Serializable {

    private static final Logger LOGGER = LoggerFactory.getLogger(CorpprocDocumentCollection.class);

    private static final long serialVersionUID = 1L;

    private static final byte[] META_MARKER = "%%#".getBytes(StandardCharsets.UTF_8);
    private static final byte[] DOC_MARKER = "%%#DOC".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PAGE_MARKER = "%%#PAGE".getBytes(StandardCharsets.UTF_8);
    private static final byte[] SENTENCE_MARKER = "%%#SEN".getBytes(StandardCharsets.UTF_8);
    private static final byte[] PARAGRAPH_MARKER = "%%#PAR".getBytes(StandardCharsets.UTF_8);

    private final String[] files;

    private boolean gzipped;
    private int numFields;

    private final DocumentFactory factory;
    /**
     * A list of lists of pointers parallel to {@link #files}. Each list
     * contains the starting pointer of each document (within its file), plus a
     * final pointer at the end of the file.
     */
    private final ObjectArrayList<EliasFanoMonotoneLongBigList> pointers;
    /**
     * The number of documents in this collection.
     */
    private final int size;
    /**
     * Whether this index contains phrases (as opposed to documents).
     */
    private final boolean phrase;
    /**
     * An array parallel to {@link #files} containing the index of the first
     * document within each file, plus a final entry equal to {@link #size}.
     */
    private final long[] firstDocument;
    /**
     * Byte array buffers used to reconstruct each field for random access.
     */
    private transient byte[][] buffer;
    /**
     * An array parallel to {@link #buffer} specifying the number of valid
     * bytes.
     */
    private transient int[] bufferSize;

    private transient byte[] lineBuffer;
    private transient Reference2ObjectMap<Enum<?>, Object> lastDocumentMetadata;
    private transient int lastDocumentRead = -1;

    private void initBuffers() {
        bufferSize = new int[numFields];
        buffer = new byte[numFields][];
        for (int i = 0; i < numFields; i++) {
            buffer[i] = ByteArrays.EMPTY_ARRAY;
        }
        lineBuffer = ByteArrays.EMPTY_ARRAY;
        lastDocumentMetadata = new Reference2ObjectArrayMap<>();
        lastDocumentRead = -1;
    }

    /**
     * Builds a document collection corresponding to a given set of custom files
     * specified as an array.
     *
     * <p>
     * <strong>Beware.</strong> This class is not guaranteed to work if files
     * are deleted or modified after creation!
     *
     * @param file an array containing the files that will be contained in the
     * collection.
     * @param factory the factory that will be used to create documents.
     * @param phrase whether phrases should be indexed instead of documents.
     * @param numFields
     * @throws java.io.IOException
     */
    public CorpprocDocumentCollection(String[] file, DocumentFactory factory, boolean phrase, int numFields) throws IOException {
        this(file, factory, phrase, false, numFields);
    }

    /**
     * Builds a document collection corresponding to a given set of (possibly
     * gzip'd) Custom files specified as an array.
     *
     * <p>
     * <strong>Beware.</strong> This class is not guaranteed to work if files
     * are deleted or modified after creation!
     *
     * @param files an array containing the files that will be contained in the
     * collection.
     * @param factory the factory that will be used to create documents.
     * @param phrase whether phrases should be indexed instead of documents.
     * @param gzipped the files in <code>file</code> are gzip'd.
     * @throws java.io.IOException
     */
    public CorpprocDocumentCollection(final String[] files, final DocumentFactory factory, final boolean phrase, final boolean gzipped, int numFields) throws IOException {
        this.files = files;
        this.factory = factory;
        this.gzipped = gzipped;
        this.phrase = phrase;
        this.numFields = numFields;

        initBuffers();

        pointers = new ObjectArrayList<>(files.length);
        firstDocument = new long[files.length + 1];

        final ProgressLogger logger = new ProgressLogger(LOGGER, "files");
        logger.expectedUpdates = files.length;
        logger.start("Scanning files...");

        // Scan files and retrieve page pointers
        int count = 0;
        LongArrayList p = new LongArrayList();
        for (String f : files) {
            p.clear();
            try (FastBufferedInputStream fbis = new FastBufferedInputStream(
                    gzipped ? new GZIPInputStream(new FileInputStream(f)) : new FileInputStream(f))) {
                while (true) {
                    long position = fbis.position();
                    if (readLine(fbis) == -1) {
                        break;
                    }
                    if (startsWith(lineBuffer, DOC_MARKER) || (phrase && startsWith(lineBuffer, SENTENCE_MARKER))) {
                        p.add(position);
                    }
                }
                count += p.size();
                p.add(fbis.position());
            }

            pointers.add(new EliasFanoMonotoneLongBigList(p));
            firstDocument[pointers.size()] = count;

            logger.update();
        }

        logger.done();

        size = count;
    }

    protected CorpprocDocumentCollection(String[] file, DocumentFactory factory, ObjectArrayList<EliasFanoMonotoneLongBigList> pointers, int size, long[] firstDocument, boolean phrase, boolean gzipped) {
        this.files = file;
        this.factory = factory;
        this.pointers = pointers;
        this.size = size;
        this.firstDocument = firstDocument;
        this.gzipped = gzipped;
        this.phrase = phrase;
        initBuffers();
    }

    private int readLine(final FastBufferedInputStream fbis) throws IOException {
        int start = 0, len;
        while ((len = fbis.readLine(lineBuffer, start, lineBuffer.length - start, FastBufferedInputStream.ALL_TERMINATORS)) == lineBuffer.length - start) {
            start += len;
            lineBuffer = ByteArrays.grow(lineBuffer, lineBuffer.length + 1);
        }

        if (len != -1) {
            start += len;
        }
        return len == -1 ? -1 : start;
    }

    private static boolean startsWith(byte[] array, byte[] pattern) {
        if (array.length < pattern.length) {
            return false;
        }
        for (int i = 0; i < pattern.length; i++) {
            if (array[i] != pattern[i]) {
                return false;
            }
        }
        return true;
    }

    @Override
    public DocumentFactory factory() {
        return factory;
    }

    @Override
    public long size() {
        return size;
    }

    @Override
    public Reference2ObjectMap<Enum<?>, Object> metadata(final long index) throws IOException {
        readDocument(index, -1);
        if (!lastDocumentMetadata.containsKey(MetadataKeys.TITLE)) {
            lastDocumentMetadata.put(MetadataKeys.TITLE, "Sentence #" + (index + 1));
        }
        return lastDocumentMetadata;
    }

    @Override
    public Document document(final long index) throws IOException {
        return factory.getDocument(stream(index), metadata(index));
    }

    @Override
    public InputStream stream(final long index) throws IOException {
        readDocument(index, -1);
        FastByteArrayInputStream[] is = new FastByteArrayInputStream[numFields];
        for (int i = 0; i < numFields; i++) {
            is[i] = new FastByteArrayInputStream(buffer[i], 0, bufferSize[i]);
        }
        return MultipleInputStream.getStream(is);
    }

    @Override
    public DocumentIterator iterator() throws IOException {
        return new AbstractDocumentIterator() {
            private int index = 0;
            private int f = 0;
            private FastBufferedInputStream fbis = new FastBufferedInputStream(new FileInputStream(files[0]));

            @Override
            public void close() throws IOException {
                super.close();
                if (fbis != null) {
                    fbis.close();
                    fbis = null;
                }
            }

            @Override
            public Document nextDocument() throws IOException {
                if (index == size) {
                    return null;
                }
                if (index == firstDocument[f + 1]) {
                    fbis.close();
                    fbis = new FastBufferedInputStream(new FileInputStream(files[++f]));
                }
                readOpenDocument(index, f, fbis);
                return document(index++);
            }
        };
    }

    private void readDocument(final long index, int f) throws IOException {
        ensureDocumentIndex(index);
        if (index == lastDocumentRead) { // If we're reading the same document
            return;
        }

        f = Arrays.binarySearch(firstDocument, index);
        if (f < 0) {
            f = -f - 2;
        }
        try (FastBufferedInputStream fbis = new FastBufferedInputStream(new FileInputStream(files[f]))) {
            readOpenDocument(index, f, fbis);
        }
    }

    private void readOpenDocument(final long index, int f, FastBufferedInputStream fbis) throws IOException {
        ensureDocumentIndex(index);
        if (index == lastDocumentRead) { // If we're reading the same document
            return;
        }

        lastDocumentMetadata.clear();

        long start = pointers.get(f).getLong(index - firstDocument[f]);
        final long end = pointers.get(f).getLong(index - firstDocument[f] + 1);

        Arrays.fill(bufferSize, 0);

        fbis.position(start);
        while (fbis.position() < end) {
            int l = readLine(fbis);
            if (!startsWith(lineBuffer, META_MARKER)) {
                for (int i = 0, field = 0; i < l; i++) {
                    if (lineBuffer[i] == '\t') {
                        field++;
                        continue;
                    }
                    buffer[field] = ByteArrays.grow(buffer[field], bufferSize[field] + 2);
                    buffer[field][bufferSize[field]++] = lineBuffer[i];

                    if (i == l - 1 || lineBuffer[i + 1] == '\t') {
                        buffer[field][bufferSize[field]++] = ' ';
                    }
                }
            }
            if (startsWith(lineBuffer, DOC_MARKER) && phrase) {
                return;
            }
            if (startsWith(lineBuffer, PAGE_MARKER)) {
                //Get page title & url without PAGE_MARKER
                String[] titleWithUri = new String(lineBuffer,
                        Math.min(PAGE_MARKER.length + 1, l),
                        Math.max(l - PAGE_MARKER.length - 1, 0),
                        "UTF-8").trim().split("\t");
                if (titleWithUri.length >= 2) {
                    lastDocumentMetadata.put(MetadataKeys.TITLE, titleWithUri[0]);
                    lastDocumentMetadata.put(MetadataKeys.URI, URLEncoder.encode(titleWithUri[1], "UTF-8"));
                } else {
                    lastDocumentMetadata.put(MetadataKeys.TITLE, "Unknown");
                    lastDocumentMetadata.put(MetadataKeys.URI, URLEncoder.encode(titleWithUri[0], "UTF-8"));
                }
                continue;
            }
            if (startsWith(lineBuffer, SENTENCE_MARKER) && !phrase) {
                for (int i = 0; i < numFields; i++) {
                    // Add a pilcrow sign (UTF-8: 0xC2 0xB6).
                    buffer[i] = ByteArrays.grow(buffer[i], bufferSize[i] + 3);
                    buffer[i][bufferSize[i]++] = (byte) 0xc2;
                    buffer[i][bufferSize[i]++] = (byte) 0xb6;
                    buffer[i][bufferSize[i]++] = '\n';
                }
                continue;
            }
            if (startsWith(lineBuffer, PARAGRAPH_MARKER) && !phrase) {
                for (int i = 0; i < numFields; i++) {
                    // Add a section sign (UTF-8: 0xC2 0xA7).
                    buffer[i] = ByteArrays.grow(buffer[i], bufferSize[i] + 3);
                    buffer[i][bufferSize[i]++] = (byte) 0xc2;
                    buffer[i][bufferSize[i]++] = (byte) 0xa7;
                    buffer[i][bufferSize[i]++] = '\n';
                }
            }
        }
    }

    @Override
    public CorpprocDocumentCollection copy() {
        return new CorpprocDocumentCollection(files, factory.copy(), pointers, size, firstDocument, phrase, gzipped);
    }

    private void readObject(final ObjectInputStream s) throws IOException, ClassNotFoundException {
        s.defaultReadObject();
        initBuffers();
    }
}
