package cz.vutbr.fit.knot.corpproc.indexer;

/*		 
 * MG4J: Managing Gigabytes for Java (big)
 *
 * Copyright (C) 2005-2013 Sebastiano Vigna 
 *
 *  This library is free software; you can redistribute it and/or modify it
 *  under the terms of the GNU Lesser General Public License as published by the Free
 *  Software Foundation; either version 3 of the License, or (at your option)
 *  any later version.
 *
 *  This library is distributed in the hope that it will be useful, but
 *  WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 *  or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU Lesser General Public License
 *  for more details.
 *
 *  You should have received a copy of the GNU Lesser General Public License
 *  along with this program; if not, see <http://www.gnu.org/licenses/>.
 *
 */
import it.unimi.di.big.mg4j.document.ConcatenatedDocumentCollection;
import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.document.DocumentCollection;
import it.unimi.di.big.mg4j.document.DocumentCollectionBuilder;
import it.unimi.di.big.mg4j.document.DocumentFactory;
import it.unimi.di.big.mg4j.document.DocumentIterator;
import it.unimi.di.big.mg4j.document.DocumentSequence;
import it.unimi.di.big.mg4j.index.BitStreamIndexWriter;
import it.unimi.di.big.mg4j.index.CompressionFlags;
import it.unimi.di.big.mg4j.index.CompressionFlags.Coding;
import it.unimi.di.big.mg4j.index.CompressionFlags.Component;
import it.unimi.di.big.mg4j.index.DiskBasedIndex;
import it.unimi.di.big.mg4j.index.FileIndex;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.IndexWriter;
import it.unimi.di.big.mg4j.index.NullTermProcessor;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.index.cluster.ContiguousDocumentalStrategy;
import it.unimi.di.big.mg4j.index.cluster.DocumentalCluster;
import it.unimi.di.big.mg4j.index.cluster.DocumentalConcatenatedCluster;
import it.unimi.di.big.mg4j.index.cluster.DocumentalMergedCluster;
import it.unimi.di.big.mg4j.index.cluster.IdentityDocumentalStrategy;
import it.unimi.di.big.mg4j.index.cluster.IndexCluster;
import it.unimi.di.big.mg4j.index.payload.Payload;
import it.unimi.di.big.mg4j.io.ByteArrayPostingList;
import it.unimi.di.big.mg4j.io.IOFactories;
import it.unimi.di.big.mg4j.io.IOFactory;
import it.unimi.di.big.mg4j.tool.Scan.Completeness;
import it.unimi.di.big.mg4j.tool.Scan.IndexingType;
import it.unimi.di.big.mg4j.tool.VirtualDocumentResolver;
import it.unimi.dsi.Util;
import static it.unimi.dsi.fastutil.Arrays.quickSort;
import it.unimi.dsi.fastutil.ints.AbstractIntComparator;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntBigArrays;
import it.unimi.dsi.fastutil.io.BinIO;
import it.unimi.dsi.fastutil.io.FastBufferedOutputStream;
import it.unimi.dsi.fastutil.io.FastByteArrayOutputStream;
import it.unimi.dsi.fastutil.longs.AbstractLongComparator;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongArrays;
import it.unimi.dsi.fastutil.objects.Object2ReferenceOpenHashMap;
import it.unimi.dsi.io.InputBitStream;
import it.unimi.dsi.io.OutputBitStream;
import it.unimi.dsi.io.WordReader;
import it.unimi.dsi.lang.MutableString;
import it.unimi.dsi.lang.ObjectParser;
import it.unimi.dsi.logging.ProgressLogger;
import it.unimi.dsi.util.Properties;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class CorpprocScan {

    private final static Logger LOGGER = LoggerFactory.getLogger(CorpprocScan.class);
    private final static boolean ASSERTS = false;

    /**
     * When available memory goes below this threshold, we try a compaction.
     */
    public static final int PERC_AVAILABLE_MEMORY_CHECK = 10;

    /**
     * If after compaction there is less memory (in percentage) than this value,
     * we will flush the current batch.
     */
    public static final int PERC_AVAILABLE_MEMORY_DUMP = 30;

    /**
     * The extension of the property file for the cluster associated with a
     * scan.
     */
    private static final String CLUSTER_STRATEGY_EXTENSION = ".cluster.strategy";

    /**
     * The extension of the strategy for the cluster associated with a scan.
     */
    public static final String CLUSTER_PROPERTIES_EXTENSION = ".cluster.properties";

    /**
     * The initial size in bytes of a byte array posting list. Most terms have a
     * very low frequency, so we keep it small.
     */
    private static final int BYTE_ARRAY_POSTING_LIST_INITIAL_SIZE = 8;

    /**
     * The frequency with which we report the current number of terms.
     */
    private static final int TERM_REPORT_STEP = 1000000;

    /**
     * The initial size of the term map.
     */
    public static final int INITIAL_TERM_MAP_SIZE = 1000;

    /**
     * The I/O factory that will be used to create files.
     */
    private final IOFactory ioFactory;

    /**
     * The current basename of the overall index (usually some basename
     * postfixed with the field name).
     */
    private final String basename;

    /**
     * A term processor to be applied during the indexing phase.
     */
    private final TermProcessor termProcessor;

    /**
     * The field name, if available.
     */
    private final String field;

    /**
     * The size of a buffer.
     */
    private final int bufferSize;

    /**
     * The directory where batches files will be created.
     */
    private final File batchDir;

    /**
     * The flag map for batches.
     */
    final Map<Component, Coding> flags;

    /**
     * A map containing the terms seen so far.
     */
    private Object2ReferenceOpenHashMap<MutableString, ByteArrayPostingList> termMap;

    /**
     * The output bit stream for size information. For
     * {@link IndexingType#STANDARD} indexing, the list of &gamma;-coded
     * document sizes. For {@link IndexingType#REMAPPED} indexing, a list of
     * &gamma;-coded document numbers and document sizes.
     */
    private OutputBitStream sizes;

    /**
     * The total number of occurrences.
     */
    private long totOccurrences;

    /**
     * The total number of postings (pairs term/document).
     */
    private long totPostings;

    /**
     * The total number of documents.
     */
    private long totDocuments;

    /**
     * Maximum occurrence count.
     */
    private int maxCount;

    /**
     * Maximum size in words of documents seen so far.
     */
    private int globMaxDocSize;

    /**
     * The number of documents indexed so far in the current batch.
     */
    private int documentCount;

    /**
     * The number of terms seen so far in the current batch.
     */
    private int numTerms;

    /**
     * Maximum size in words of documents seen so far in the current batch.
     */
    int maxDocSize;

    /**
     * The current batch.
     */
    private int batch;

    /**
     * The number of occurrences in the current batch.
     */
    private long numOccurrences;

    /**
     * If true, this class experienced an {@link OutOfMemoryError} during some
     * buffer reallocation.
     */
    public boolean outOfMemoryError;

    /**
     * Whether {@link #indexingType} is {@link IndexingType#STANDARD}.
     */
    private final boolean indexingIsStandard;

    /**
     * Whether {@link #indexingType} is {@link IndexingType#REMAPPED}.
     */
    private final boolean indexingIsRemapped;

    /**
     * Whether {@link #indexingType} is {@link IndexingType#VIRTUAL}.
     */
    private final boolean indexingIsVirtual;

    /**
     * The number of occurrences generated by the current document.
     */
    private int occsInCurrDoc;

    /**
     * A big array containing the current maximum size for each document, if the
     * field indexed is virtual.
     */
    protected int[][] currSize;

    /**
     * The maximum document pointer ever seen (could be different from the last
     * document indexed if {@link #indexingType} is not
     * {@link IndexingType#STANDARD}).
     */
    private long maxDocInBatch;

    /**
     * The width of the artificial gap introduced between virtual-document
     * fragments.
     */
    protected int virtualDocumentGap;

    /**
     * A builder that will be used to zip the document sequence while we pass
     * through it.
     */
    private final DocumentCollectionBuilder builder;

    /**
     * The cutpoints of the batches (for building later a
     * {@link it.unimi.di.big.mg4j.index.cluster.ContiguousDocumentalStrategy}).
     */
    protected final LongArrayList cutPoints;
    /**
     * The completeness level required to this instance.
     */
    private final Completeness completeness;

    /**
     * Creates a new scanner instance using the
     * {@link IOFactory#FILESYSTEM_FACTORY}.
     *
     * @param basename the basename (usually a global filename followed by the
     * field name, separated by a dash).
     * @param field the field to be indexed.
     * @param termProcessor the term processor for this index.
     * @param documentsAreInOrder if true, documents will be served in
     * increasing order.
     * @param bufferSize the buffer size used in all I/O.
     * @param builder a builder used to create a compressed document collection
     * on the fly.
     * @param batchDir a directory for batch files; batch names will be
     * relativised to this directory if it is not <code>null</code>.
     * @throws java.io.IOException
     */
    @Deprecated
    public CorpprocScan(final String basename, final String field, final TermProcessor termProcessor, final boolean documentsAreInOrder, final int bufferSize, final DocumentCollectionBuilder builder,
            final File batchDir) throws IOException {
        this(basename, field, Completeness.POSITIONS, termProcessor, documentsAreInOrder ? IndexingType.STANDARD : IndexingType.VIRTUAL, 0, 0, bufferSize, builder, batchDir);
    }

    @Deprecated
    public CorpprocScan(final String basename, final String field, final TermProcessor termProcessor, final IndexingType indexingType, final int bufferSize, final DocumentCollectionBuilder builder,
            final File batchDir) throws IOException {
        this(basename, field, Completeness.POSITIONS, termProcessor, indexingType, 0, 0, bufferSize, builder, batchDir);
    }

    /**
     * Creates a new scanner instance using the
     * {@link IOFactory#FILESYSTEM_FACTORY}.
     *
     * @param basename the basename (usually a global filename followed by the
     * field name, separated by a dash).
     * @param field the field to be indexed.
     * @param completeness
     * @param termProcessor the term processor for this index.
     * @param indexingType the type of indexing procedure.
     * @param numVirtualDocs the number of virtual documents that will be used,
     * in case of a virtual index; otherwise, immaterial.
     * @param virtualDocumentGap the artificial gap introduced between virtual
     * documents fragments, in case of a virtual index; otherwise, immaterial.
     * @param bufferSize the buffer size used in all I/O.
     * @param builder a builder used to create a compressed document collection
     * on the fly.
     * @param batchDir a directory for batch files; batch names will be
     * relativised to this directory if it is not <code>null</code>.
     * @throws java.io.IOException
     */
    public CorpprocScan(final String basename, final String field, final Completeness completeness, final TermProcessor termProcessor, final IndexingType indexingType, final int numVirtualDocs, final int virtualDocumentGap, final int bufferSize,
            final DocumentCollectionBuilder builder, final File batchDir) throws IOException {
        this(IOFactory.FILESYSTEM_FACTORY, basename, field, completeness, termProcessor, indexingType, numVirtualDocs, virtualDocumentGap, bufferSize, builder, batchDir);
    }

    /**
     * Creates a new scanner instance.
     *
     * @param ioFactory the factory that will be used to perform I/O.
     * @param basename the basename (usually a global filename followed by the
     * field name, separated by a dash).
     * @param field the field to be indexed.
     * @param completeness
     * @param termProcessor the term processor for this index.
     * @param indexingType the type of indexing procedure.
     * @param numVirtualDocs the number of virtual documents that will be used,
     * in case of a virtual index; otherwise, immaterial.
     * @param virtualDocumentGap the artificial gap introduced between virtual
     * documents fragments, in case of a virtual index; otherwise, immaterial.
     * @param bufferSize the buffer size used in all I/O.
     * @param builder a builder used to create a compressed document collection
     * on the fly.
     * @param batchDir a directory for batch files; batch names will be
     * relativised to this directory if it is not <code>null</code>.
     * @throws java.io.IOException
     */
    public CorpprocScan(final IOFactory ioFactory, final String basename, final String field, final Completeness completeness, final TermProcessor termProcessor, final IndexingType indexingType, final long numVirtualDocs, final int virtualDocumentGap, final int bufferSize,
            final DocumentCollectionBuilder builder, final File batchDir) throws IOException {
        this.ioFactory = ioFactory;
        this.basename = basename;
        this.field = field;
        this.completeness = completeness;
        this.termProcessor = termProcessor;
        this.bufferSize = bufferSize;
        this.builder = builder;
        this.batchDir = batchDir;
        this.virtualDocumentGap = virtualDocumentGap;
        this.cutPoints = new LongArrayList();
        this.cutPoints.add(0);

        termMap = new Object2ReferenceOpenHashMap<>(INITIAL_TERM_MAP_SIZE);

        flags = new EnumMap<>(CompressionFlags.DEFAULT_STANDARD_INDEX);
        if (completeness.compareTo(Completeness.POSITIONS) < 0) {
            flags.remove(Component.POSITIONS);
        }
        if (completeness.compareTo(Completeness.COUNTS) < 0) {
            flags.remove(Component.COUNTS);
        }

        indexingIsStandard = indexingType == IndexingType.STANDARD;
        indexingIsRemapped = indexingType == IndexingType.REMAPPED;
        indexingIsVirtual = indexingType == IndexingType.VIRTUAL;
        if (indexingIsVirtual && virtualDocumentGap == 0) {
            throw new IllegalArgumentException("Illegal virtual document gap: " + virtualDocumentGap);
        }

        if (indexingIsVirtual) {
            currSize = IntBigArrays.newBigArray(numVirtualDocs);
        }
        maxDocInBatch = (currSize != null ? IntBigArrays.length(currSize) : 0) - 1;
        openSizeBitStream();
    }

    /**
     * Cleans all intermediate files generated by a run of this class.
     *
     * @param ioFactory the factory that will be used to perform I/O.
     * @param basename the basename of the run.
     * @param batches the number of generated batches.
     * @param batchDir if not <code>null</code>, a temporary directory where the
     * batches are located.
     * @throws java.io.IOException
     */
    public static void cleanup(final IOFactory ioFactory, final String basename, final int batches, final File batchDir) throws IOException {
        final String basepath = (batchDir != null ? new File(basename) : new File(basename)).getCanonicalPath();
        ioFactory.delete(basepath + CLUSTER_STRATEGY_EXTENSION);
        ioFactory.delete(basepath + CLUSTER_PROPERTIES_EXTENSION);
        for (int i = 0; i < batches; i++) {
            final String batchBasename = batchBasename(i, basename, batchDir);
            ioFactory.delete(batchBasename + DiskBasedIndex.FREQUENCIES_EXTENSION);
            ioFactory.delete(batchBasename + DiskBasedIndex.OCCURRENCIES_EXTENSION);
            ioFactory.delete(batchBasename + DiskBasedIndex.INDEX_EXTENSION);
            ioFactory.delete(batchBasename + DiskBasedIndex.OFFSETS_EXTENSION);
            ioFactory.delete(batchBasename + DiskBasedIndex.SIZES_EXTENSION);
            ioFactory.delete(batchBasename + DiskBasedIndex.STATS_EXTENSION);
            ioFactory.delete(batchBasename + DiskBasedIndex.PROPERTIES_EXTENSION);
            ioFactory.delete(batchBasename + DiskBasedIndex.POSITIONS_NUMBER_OF_BITS_EXTENSION);
            ioFactory.delete(batchBasename + DiskBasedIndex.TERMS_EXTENSION);
            ioFactory.delete(batchBasename + DiskBasedIndex.SUMS_MAX_POSITION_EXTENSION);
            ioFactory.delete(batchBasename + DiskBasedIndex.UNSORTED_TERMS_EXTENSION);
        }
    }

    /**
     * Returns the name of a batch.
     *
     * <p>
     * You can override this method if you prefer a different batch naming
     * scheme.
     *
     * @param batch the batch number.
     * @param basename the index basename.
     * @param batchDir if not <code>null</code>, a temporary directory for
     * batches.
     * @return simply <code>basename@batch</code>, if <code>batchDir</code> is
     * <code>null</code>; otherwise, we relativise the name to
     * <code>batchDir</code>.
     */
    protected static String batchBasename(int batch, String basename, final File batchDir) {
        return batchDir != null ? new File(batchDir, basename + "@" + batch).toString() : basename + "@" + batch;
    }

    /**
     * Dumps the current batch on disk as an index.
     *
     * @return the number of occurrences contained in the batch.
     * @throws java.io.IOException
     * @throws org.apache.commons.configuration.ConfigurationException
     */
    protected long dumpBatch() throws IOException, ConfigurationException {

        outOfMemoryError = false;
        final String batchBasename = batchBasename(batch, basename, batchDir);
        LOGGER.debug("Generating index " + batchBasename + "; documents: " + documentCount + "; terms: " + numTerms + "; occurrences: " + numOccurrences);

        // This is not strictly necessary, but nonetheless it frees enough memory for the subsequent allocation. 
        for (ByteArrayPostingList bapl : termMap.values()) {
            bapl.close();
        }
        // We write down all term in appearance order in termArray.
        MutableString[] termArray = termMap.keySet().toArray(new MutableString[numTerms]);

        if (ASSERTS) {
            assert numTerms == termMap.size();
        }
        if (!indexingIsVirtual) {
            sizes.close();
        }

        // We sort the terms appearing in the batch and write them on disk.
        Arrays.sort(termArray);
        try (PrintWriter pw = new PrintWriter(new OutputStreamWriter(new FastBufferedOutputStream(ioFactory.getOutputStream(batchBasename + DiskBasedIndex.TERMS_EXTENSION), bufferSize), "UTF-8"))) {
            for (MutableString t : termArray) {
                t.println(pw);
            }
        }

        try {
            if (indexingIsStandard) {
                /* For standard indexing, we exploit shamelessly the fact that the bistreams stored in memory
				 * are compatible with BitStreamIndexWriter's format, dumping directly the bitstreams on
				 * disk and simulating everything that a BitStreamIndexWriter would do. */

                final OutputBitStream index = new OutputBitStream(ioFactory.getOutputStream(batchBasename + DiskBasedIndex.INDEX_EXTENSION), false);
                final OutputBitStream offsets = new OutputBitStream(ioFactory.getOutputStream(batchBasename + DiskBasedIndex.OFFSETS_EXTENSION), false);
                final OutputBitStream posNumBits = new OutputBitStream(ioFactory.getOutputStream(batchBasename + DiskBasedIndex.POSITIONS_NUMBER_OF_BITS_EXTENSION), false);
                final OutputBitStream sumsMaxPos = new OutputBitStream(ioFactory.getOutputStream(batchBasename + DiskBasedIndex.SUMS_MAX_POSITION_EXTENSION), false);
                final OutputBitStream frequencies = new OutputBitStream(ioFactory.getOutputStream(batchBasename + DiskBasedIndex.FREQUENCIES_EXTENSION), false);
                final OutputBitStream occurrencies = new OutputBitStream(ioFactory.getOutputStream(batchBasename + DiskBasedIndex.OCCURRENCIES_EXTENSION), false);

                ByteArrayPostingList baps;
                int maxCount = 0;
                long frequency;
                long bitLength, postings = 0, prevOffset = 0;

                offsets.writeGamma(0);

                for (int i = 0; i < numTerms; i++) {
                    baps = termMap.get(termArray[i]);
                    frequency = baps.frequency;

                    if (maxCount < baps.maxCount) {
                        maxCount = baps.maxCount;
                    }
                    bitLength = baps.writtenBits();
                    baps.align();

                    postings += frequency;

                    index.writeLongGamma(frequency - 1);

                    // We need special treatment for terms appearing in all documents
                    if (frequency == documentCount) {
                        baps.stripPointers(index, bitLength);
                    } else {
                        index.write(baps.buffer, bitLength);
                    }

                    frequencies.writeLongGamma(frequency);
                    occurrencies.writeLongGamma(baps.occurrency);
                    offsets.writeLongGamma(index.writtenBits() - prevOffset);
                    posNumBits.writeLongGamma(baps.posNumBits);
                    sumsMaxPos.writeLongDelta(baps.sumMaxPos);
                    prevOffset = index.writtenBits();
                }

                totPostings += postings;
                if (this.maxCount < maxCount) {
                    this.maxCount = maxCount;
                }

                final Properties properties = new Properties();
                properties.setProperty(Index.PropertyKeys.DOCUMENTS, documentCount);
                properties.setProperty(Index.PropertyKeys.TERMS, numTerms);
                properties.setProperty(Index.PropertyKeys.POSTINGS, postings);
                properties.setProperty(Index.PropertyKeys.MAXCOUNT, maxCount);
                properties.setProperty(Index.PropertyKeys.INDEXCLASS, FileIndex.class.getName());
                properties.addProperty(Index.PropertyKeys.CODING, "FREQUENCIES:GAMMA");
                properties.addProperty(Index.PropertyKeys.CODING, "POINTERS:DELTA");
                if (completeness.compareTo(Completeness.COUNTS) >= 0) {
                    properties.addProperty(Index.PropertyKeys.CODING, "COUNTS:GAMMA");
                }
                if (completeness.compareTo(Completeness.POSITIONS) >= 0) {
                    properties.addProperty(Index.PropertyKeys.CODING, "POSITIONS:DELTA");
                }
                properties.setProperty(Index.PropertyKeys.TERMPROCESSOR, ObjectParser.toSpec(termProcessor));
                properties.setProperty(Index.PropertyKeys.OCCURRENCES, numOccurrences);
                properties.setProperty(Index.PropertyKeys.MAXDOCSIZE, maxDocSize);
                properties.setProperty(Index.PropertyKeys.SIZE, index.writtenBits());
                if (field != null) {
                    properties.setProperty(Index.PropertyKeys.FIELD, field);
                }
                saveProperties(ioFactory, properties, batchBasename + DiskBasedIndex.PROPERTIES_EXTENSION);
                index.close();
                offsets.close();
                posNumBits.close();
                sumsMaxPos.close();
                occurrencies.close();
                frequencies.close();
            } else {
                final IndexWriter indexWriter = new BitStreamIndexWriter(ioFactory, batchBasename, maxDocInBatch + 1, true, flags);

                ByteArrayPostingList bapl;
                OutputBitStream obs;
                int maxCount = -1;
                int frequency;
                int maxFrequency = 0;
                // Compute max frequency and allocate position array.
                for (ByteArrayPostingList b : termMap.values()) {
                    b.close();
                    b.align();
                    if (b.frequency > Integer.MAX_VALUE) {
                        throw new IllegalArgumentException("Batches of non-standard indices are required to have frequency not larger than Integer.MAX_VALUE");
                    }
                    if (maxFrequency < b.frequency) {
                        maxFrequency = (int) b.frequency;
                    }
                    if (maxCount < b.maxCount) {
                        maxCount = b.maxCount;
                    }
                }

                final long[] bitPos = new long[maxFrequency];
                final int[] pointer = new int[maxFrequency];
                int[] pos = new int[maxCount];
                final boolean hasCounts = completeness.compareTo(Completeness.COUNTS) >= 0;
                final boolean hasPositions = completeness.compareTo(Completeness.POSITIONS) >= 0;
                int count = -1, moreCount = -1;

                for (int i = 0; i < numTerms; i++) {
                    bapl = termMap.get(termArray[i]);
                    @SuppressWarnings("resource")
                    final InputBitStream ibs = new InputBitStream(bapl.buffer);
                    frequency = (int) bapl.frequency; // This could be much more than the actual frequency in virtual indices

                    // Calculate posting bit positions and corresponding pointers
                    for (int j = 0; j < frequency; j++) {
                        bitPos[j] = ibs.readBits(); // Cache bit poisition
                        pointer[j] = ibs.readDelta(); // Cache pointer
                        if (hasCounts) {
                            count = ibs.readGamma() + 1;
                        }
                        if (hasPositions) {
                            ibs.skipDeltas(count); // Skip document positions
                        }
                    }

                    // Sort stably pointers and positions by increasing pointer
                    quickSort(0, frequency, new AbstractIntComparator() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public int compare(final int i0, final int i1) {
                            final int t = pointer[i0] - pointer[i1];
                            if (t != 0) {
                                return t;
                            }
                            final long u = bitPos[i0] - bitPos[i1]; // We need a stable sort
                            return u < 0 ? -1 : u > 0 ? 1 : 0;
                        }
                    }, (final int i0, final int i1) -> {
                        final long t = bitPos[i0];
                        bitPos[i0] = bitPos[i1];
                        bitPos[i1] = t;
                        final int p = pointer[i0];
                        pointer[i0] = pointer[i1];
                        pointer[i1] = p;
                    });

                    int actualFrequency = frequency;
                    // Compute actual frequency for virtual indices
                    if (indexingIsVirtual) {
                        actualFrequency = 1;
                        for (int j = 1; j < frequency; j++) {
                            if (pointer[j] != pointer[j - 1]) {
                                actualFrequency++;
                            }
                        }
                        if (ASSERTS) {
                            for (int j = 1; j < frequency; j++) {
                                assert pointer[j] >= pointer[j - 1];
                                assert pointer[j] != pointer[j - 1] || bitPos[j] > bitPos[j - 1];
                            }
                        }
                    }

                    indexWriter.newInvertedList();
                    indexWriter.writeFrequency(actualFrequency);

                    int currPointer;
                    for (int j = 0; j < frequency; j++) {
                        ibs.position(bitPos[j]);
                        obs = indexWriter.newDocumentRecord();
                        indexWriter.writeDocumentPointer(obs, currPointer = ibs.readDelta());
                        if (ASSERTS) {
                            assert currPointer == pointer[j];
                        }
                        if (hasCounts) {
                            count = ibs.readGamma() + 1;
                        }
                        if (hasPositions) {
                            ibs.readDeltas(pos, count);
                            for (int p = 1; p < count; p++) {
                                pos[p] += pos[p - 1] + 1;
                            }
                        }

                        if (indexingIsVirtual) {
                            while (j < frequency - 1) {
                                ibs.position(bitPos[j + 1]);
                                if (currPointer != ibs.readDelta()) {
                                    break;
                                }
                                j++;
                                if (hasCounts) {
                                    moreCount = ibs.readGamma() + 1;
                                }
                                if (hasPositions) {
                                    pos = IntArrays.grow(pos, count + moreCount, count);
                                    pos[count] = ibs.readDelta();
                                    if (ASSERTS) {
                                        assert pos[count] > pos[count - 1];
                                    }
                                    for (int p = 1; p < moreCount; p++) {
                                        pos[count + p] = pos[count + p - 1] + 1 + ibs.readDelta();
                                    }
                                }
                                count += moreCount;
                            }
                            if (maxCount < count) {
                                maxCount = count;
                            }
                        }

                        if (hasCounts) {
                            indexWriter.writePositionCount(obs, count);
                        }
                        if (hasPositions) {
                            indexWriter.writeDocumentPositions(obs, pos, 0, count, -1);
                        }
                    }
                }

                if (this.maxCount < maxCount) {
                    this.maxCount = maxCount;
                }

                indexWriter.close();
                final Properties properties = indexWriter.properties();
                totPostings += properties.getLong("postings");
                properties.setProperty(Index.PropertyKeys.TERMPROCESSOR, ObjectParser.toSpec(termProcessor));
                properties.setProperty(Index.PropertyKeys.OCCURRENCES, numOccurrences);
                properties.setProperty(Index.PropertyKeys.MAXDOCSIZE, maxDocSize);
                properties.setProperty(Index.PropertyKeys.SIZE, indexWriter.writtenBits());
                if (field != null) {
                    properties.setProperty(Index.PropertyKeys.FIELD, field);
                }
                saveProperties(ioFactory, properties, batchBasename + DiskBasedIndex.PROPERTIES_EXTENSION);

                if (indexingIsRemapped) {
                    // We must permute sizes
                    final int[] document = new int[documentCount], size = new int[documentCount];
                    try (InputBitStream sizes = new InputBitStream(ioFactory.getInputStream(batchBasename + DiskBasedIndex.SIZES_EXTENSION), false)) {
                        for (int i = 0; i < documentCount; i++) {
                            document[i] = sizes.readGamma();
                            size[i] = sizes.readGamma();
                        }
                    }

                    quickSort(0, documentCount, new AbstractIntComparator() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public int compare(int x, int y) {
                            return document[x] - document[y];
                        }
                    }, (int x, int y) -> {
                        int t = document[x];
                        document[x] = document[y];
                        document[y] = t;
                        t = size[x];
                        size[x] = size[y];
                        size[y] = t;
                    });

                    try (OutputBitStream permutedSizes = new OutputBitStream(ioFactory.getOutputStream(batchBasename(batch, basename, batchDir) + DiskBasedIndex.SIZES_EXTENSION), false)) {
                        for (int i = 0, d = 0; i < documentCount; i++) {
                            while (d++ < document[i]) {
                                permutedSizes.writeGamma(0);
                            }
                            permutedSizes.writeGamma(size[i]);
                        }
                    }
                }
            }

            if (indexingIsVirtual) {
                try (OutputBitStream sizes = new OutputBitStream(ioFactory.getOutputStream(batchBasename(batch, basename, batchDir) + DiskBasedIndex.SIZES_EXTENSION))) {
                    final long length = IntBigArrays.length(currSize);
                    for (int i = 0; i < length; i++) {
                        sizes.writeGamma(IntBigArrays.get(currSize, i));
                    }
                }
            }

            termMap.clear();

            numTerms = 0;
            totOccurrences += numOccurrences;
            totDocuments += documentCount;
            final long result = numOccurrences;
            globMaxDocSize = Math.max(maxDocSize, globMaxDocSize);
            numOccurrences = documentCount = maxDocSize = 0;
            maxDocInBatch = (currSize != null ? IntBigArrays.length(currSize) : 0) - 1;
            if (indexingIsStandard) {
                cutPoints.add(cutPoints.getLong(cutPoints.size() - 1) + documentCount);
            }
            batch++;

            System.gc(); // This is exactly the right time to do collection and compaction.
            return result;
        } catch (IOException e) {
            LOGGER.error("I/O Error on batch " + batch);
            throw e;
        }
    }

    protected void openSizeBitStream() throws IOException {
        if (!indexingIsVirtual) {
            sizes = new OutputBitStream(ioFactory.getOutputStream(batchBasename(batch, basename, batchDir) + DiskBasedIndex.SIZES_EXTENSION), false);
        }
    }

    /**
     * Runs in parallel a number of instances.
     *
     * <p>
     * This commodity method takes care of instantiating one instance per
     * indexed field, and to pass the right information to each instance. All
     * options are common to all fields, except for the number of occurrences in
     * a batch, which can be tuned for each field separately.
     *
     * @param ioFactory the factory that will be used to perform I/O.
     * @param basename the index basename.
     * @param documentSequence a document sequence.
     * @param completeness the completeness level of this run.
     * @param termProcessor the term processor for this index.
     * @param builder if not <code>null</code>, a builder that will be used to
     * create new collection built using <code>documentSequence</code>.
     * @param bufferSize the buffer size used in all I/O.
     * @param documentsPerBatch the number of documents that we should try to
     * put in each segment.
     * @param maxTerms the maximum number of overall (i.e., cross-field) terms
     * in a batch.
     * @param indexedField the fields that should be indexed, in increasing
     * order.
     * @param virtualDocumentResolver the array of virtual document resolvers to
     * be used, parallel to <code>indexedField</code>: it can safely contain
     * anything (even <code>null</code>) in correspondence to non-virtual
     * fields, and can safely be <code>null</code> if no fields are virtual.
     * @param virtualGap the array of virtual field gaps to be used, parallel to
     * <code>indexedField</code>: it can safely contain anything in
     * correspondence to non-virtual fields, and can safely be <code>null</code>
     * if no fields are virtual.
     * @param mapFile the name of a file containing a map to be applied to
     * document indices.
     * @param logInterval the minimum time interval between activity logs in
     * milliseconds.
     * @param tempDirName a directory for temporary files.
     * @throws IOException
     * @throws ConfigurationException
     */
    @SuppressWarnings("unchecked")
    public static void run(final IOFactory ioFactory, final String basename, final DocumentSequence documentSequence, final Completeness completeness, final TermProcessor termProcessor, final DocumentCollectionBuilder builder, final int bufferSize,
            final int documentsPerBatch, final int maxTerms, final int[] indexedField, final VirtualDocumentResolver[] virtualDocumentResolver, final int[] virtualGap, final String mapFile, final long logInterval,
            final String tempDirName) throws ConfigurationException, IOException {

        final int numberOfIndexedFields = indexedField.length;
        if (numberOfIndexedFields == 0) {
            throw new IllegalArgumentException("You must specify at least one field");
        }
        final DocumentFactory factory = documentSequence.factory();
        final File tempDir = tempDirName == null ? null : new File(tempDirName);
        for (int i = 0; i < indexedField.length; i++) {
            if (factory.fieldType(indexedField[i]) == DocumentFactory.FieldType.VIRTUAL && (virtualDocumentResolver == null || virtualDocumentResolver[i] == null)) {
                throw new IllegalArgumentException(
                        "No resolver was associated with virtual field " + factory.fieldName(indexedField[i]));
            }
        }

        if (mapFile != null && ioFactory != IOFactory.FILESYSTEM_FACTORY) {
            throw new IllegalStateException("Remapped indices currently do not support I/O factories");
        }
        final int[] map = mapFile != null ? BinIO.loadInts(mapFile) : null;

        final CorpprocScan[] scan = new CorpprocScan[numberOfIndexedFields]; // To scan textual content
        // document data

        final ProgressLogger pl = new ProgressLogger(LOGGER, logInterval, TimeUnit.MILLISECONDS, "documents");
        if (documentSequence instanceof DocumentCollection) {
            pl.expectedUpdates = ((DocumentCollection) documentSequence).size();
        }

        for (int i = 0; i < numberOfIndexedFields; i++) {
            final String fieldName = factory.fieldName(indexedField[i]);
            scan[i] = new CorpprocScan(ioFactory, basename + '-' + fieldName, fieldName, completeness, termProcessor, map != null ? IndexingType.REMAPPED
                    : IndexingType.STANDARD, 0, 0, bufferSize, builder, tempDir);
        }

        if (builder != null) {
            builder.open("@0"); // First batch
        }
        pl.displayFreeMemory = true;
        pl.start("Indexing documents...");

        DocumentIterator iterator = documentSequence.iterator();
        TokenIterator tokenIterator;
        Document document;

        int documentPointer = 0, documentsInBatch = 0;
        long batchStartTime = System.currentTimeMillis();
        boolean outOfMemoryError = false;

        while ((document = iterator.nextDocument()) != null) {
            long overallTerms = 0;
            if (builder != null) {
                builder.startDocument(document.title(), document.uri());
            }
            for (int i = 0; i < numberOfIndexedFields; i++) {
                tokenIterator = (TokenIterator) document.content(indexedField[i]);
                scan[i].processDocument(map != null ? map[documentPointer] : documentPointer, tokenIterator);
                overallTerms += scan[i].numTerms;

                if (scan[i] != null && scan[i].outOfMemoryError) {
                    outOfMemoryError = true;
                }
            }
            if (builder != null) {
                builder.endDocument();
            }
            documentPointer++;
            documentsInBatch++;
            document.close();
            pl.update();

            long percAvailableMemory = 100;
            boolean compacted = false;
            if ((documentPointer & 0xFF) == 0) {
                // We try compaction if we detect less than PERC_AVAILABLE_MEMORY_CHECK memory available
                percAvailableMemory = Util.percAvailableMemory();
                if (!outOfMemoryError && percAvailableMemory < PERC_AVAILABLE_MEMORY_CHECK) {
                    LOGGER.info("Starting compaction... (" + percAvailableMemory + "% available)");
                    compacted = true;
                    Util.compactMemory();
                    percAvailableMemory = Util.percAvailableMemory();
                    LOGGER.info("Compaction completed (" + percAvailableMemory + "% available)");
                }
            }

            if (outOfMemoryError || overallTerms >= maxTerms || documentsInBatch == documentsPerBatch || (compacted && percAvailableMemory < PERC_AVAILABLE_MEMORY_DUMP)) {
                if (outOfMemoryError) {
                    LOGGER.warn("OutOfMemoryError during buffer reallocation: writing a batch of " + documentsInBatch + " documents");
                } else if (overallTerms >= maxTerms) {
                    LOGGER.warn("Too many terms (" + overallTerms + "): writing a batch of " + documentsInBatch + " documents");
                } else if (compacted && percAvailableMemory < PERC_AVAILABLE_MEMORY_DUMP) {
                    LOGGER.warn("Available memory below " + PERC_AVAILABLE_MEMORY_DUMP + "%: writing a batch of " + documentsInBatch + " documents");
                }

                long occurrences = 0;
                for (int i = 0; i < numberOfIndexedFields; i++) {
                    occurrences += scan[i].dumpBatch();
                    scan[i].openSizeBitStream();
                }

                if (builder != null) {
                    builder.close();
                    builder.open("@" + scan[0].batch);
                }

                LOGGER.info("Last set of batches indexed at " + Util.format((1000. * occurrences) / (System.currentTimeMillis() - batchStartTime)) + " occurrences/s");
                batchStartTime = System.currentTimeMillis();
                documentsInBatch = 0;
                outOfMemoryError = false;
            }
        }

        iterator.close();
        if (builder != null) {
            builder.close();
        }

        for (int i = 0;
                i < numberOfIndexedFields;
                i++) {
            scan[i].close();
        }

        documentSequence.close();

        pl.done();

        if (builder != null) {
            final String name = new File(builder.basename()).getName();
            final String[] collectionName = new String[scan[0].batch];
            for (int i = scan[0].batch; i-- != 0;) {
                collectionName[i] = name + "@" + i + DocumentCollection.DEFAULT_EXTENSION;
            }
            IOFactories.storeObject(ioFactory, new ConcatenatedDocumentCollection(collectionName), builder.basename() + DocumentCollection.DEFAULT_EXTENSION);
        }

        if (map != null && documentPointer != map.length) {
            LOGGER.warn("The document sequence contains " + documentPointer + " documents, but the map contains "
                    + map.length + " integers");
        }
    }

    final MutableString word = new MutableString();

    final MutableString nonWord = new MutableString();

    /**
     * The default delimiter separating two documents read from standard input
     * (a newline).
     */
    public static final int DEFAULT_DELIMITER = 10;

    /**
     * The default batch size.
     */
    public static final int DEFAULT_BATCH_SIZE = 100000;

    /**
     * The default maximum number of terms.
     */
    public static final int DEFAULT_MAX_TERMS = 10000000;

    /**
     * The default buffer size.
     */
    public static final int DEFAULT_BUFFER_SIZE = 64 * 1024;

    /**
     * The default virtual field gap.
     */
    public static final int DEFAULT_VIRTUAL_DOCUMENT_GAP = 64;

    /**
     * Processes a document.
     *
     * @param documentPointer the integer pointer associated with the document.
     * @param wordReader the word reader associated with the document.
     * @throws java.io.IOException
     */
    public void processDocument(final long documentPointer, final WordReader wordReader) throws IOException {
        int pos = indexingIsVirtual ? IntBigArrays.get(currSize, documentPointer) : 0;
        final long actualPointer = indexingIsStandard ? documentCount : documentPointer;
        ByteArrayPostingList termBapl;

        word.length(0);
        nonWord.length(0);

        while (wordReader.next(word, nonWord)) {
            if (builder != null) {
                builder.add(word, nonWord);
            }
            if (word.length() == 0) {
                continue;
            }
            if (!termProcessor.processTerm(word)) {
                pos++; // We do consider the positions of terms canceled out by the term processor.
                continue;
            }

            // We check whether we have already seen this term. If not, we add it to the term map.
            if ((termBapl = termMap.get(word)) == null) {
                try {
                    termBapl = new ByteArrayPostingList(new byte[BYTE_ARRAY_POSTING_LIST_INITIAL_SIZE], indexingIsStandard, completeness);
                    termMap.put(word.copy(), termBapl);
                } catch (OutOfMemoryError e) {
                    /* There is not enough memory for enlarging the table. We set a very low growth factor, so at
					 * the next put() the enlargement will likely succeed. If not, we will generate several
					 * out-of-memory error, but we should get to the end anyway, and we will 
					 * dump the current batch as soon as the current document is finished. */
                    outOfMemoryError = true;
                    //termMap.growthFactor( 1 );
                }
                numTerms++;
                if (numTerms % TERM_REPORT_STEP == 0) {
                    LOGGER.info("[" + Util.format(numTerms) + " term(s)]");
                }
            }

            // We now record the occurrence. If a renumbering map has
            // been specified, we have to renumber the document index through it.
            termBapl.setDocumentPointer(actualPointer);
            termBapl.addPosition(pos);
            // Record whether this posting list has an out-of-memory-error problem.
            if (termBapl.outOfMemoryError) {
                outOfMemoryError = true;
            }
            occsInCurrDoc++;
            numOccurrences++;
            pos++;
        }

        if (pos > maxDocSize) {
            maxDocSize = pos;
        }

        if (indexingIsStandard) {
            sizes.writeGamma(pos);
        } else if (indexingIsRemapped) {
            sizes.writeLongGamma(actualPointer);
            sizes.writeGamma(pos);
        }

        if (indexingIsVirtual) {
            IntBigArrays.set(currSize, documentPointer, IntBigArrays.get(currSize, documentPointer) + occsInCurrDoc + virtualDocumentGap);
        }

        occsInCurrDoc = 0;
        documentCount++;
        if (actualPointer > maxDocInBatch) {
            maxDocInBatch = actualPointer;
        }
    }

    /**
     * Processes a document defined by a {@link TokenIterator}.
     *
     * @param documentPointer the integer pointer associated with the document.
     * @param tokenIterator the token iterator associated with the document.
     */
    public void processDocument(final int documentPointer, final TokenIterator tokenIterator) throws IOException {
        final int actualPointer = indexingIsStandard ? documentCount : documentPointer;
        ByteArrayPostingList termBapl;

        int pos = -1;
        while (tokenIterator.hasNext()) {
            final int t = tokenIterator.nextInt();
            if (t < pos) {
                throw new IllegalArgumentException("The token iterator for field " + field + " is not monotone");
            }
            pos = t;
            final MutableString token = tokenIterator.token();
            if (token.length() == 0) {
                continue;
            }
            if (!termProcessor.processTerm(token)) {
                continue;
            }

            // We check whether we have already seen this term. If not, we add it to the term map.
            if ((termBapl = termMap.get(token)) == null) {
                try {
                    termBapl = new ByteArrayPostingList(new byte[BYTE_ARRAY_POSTING_LIST_INITIAL_SIZE], indexingIsStandard, completeness);
                    termMap.put(token.copy(), termBapl);
                } catch (OutOfMemoryError e) {
                    /* There is not enough memory for enlarging the table. We set a very low growth factor, so at
					 * the next put() the enlargement will likely succeed. If not, we will generate several
					 * out-of-memory error, but we should get to the end anyway, and we will 
					 * dump the current batch as soon as the current document is finished. */
                    outOfMemoryError = true;
                }
                numTerms++;
                if (numTerms % TERM_REPORT_STEP == 0) {
                    LOGGER.info("[" + Util.format(numTerms) + " term(s)]");
                }
            }
            // We now record the occurrence. If a renumbering map has
            // been specified, we have to renumber the document index through it.
            termBapl.setDocumentPointer(actualPointer);
            termBapl.addPosition(pos);
            // Record whether this posting list has an out-of-memory-error problem.
            if (termBapl.outOfMemoryError) {
                outOfMemoryError = true;
            }
            occsInCurrDoc++;
            numOccurrences++;

        }

        pos++;
        if (pos > maxDocSize) {
            maxDocSize = pos;
        }

        if (indexingIsStandard) {
            sizes.writeGamma(pos);
        } else if (indexingIsRemapped) {
            sizes.writeGamma(actualPointer);
            sizes.writeGamma(pos);
        }

        occsInCurrDoc = 0;
        documentCount++;
        if (actualPointer > maxDocInBatch) {
            maxDocInBatch = actualPointer;
        }
    }

    private static void makeEmpty(final IOFactory ioFactory, final String filename) throws IOException {
        if (ioFactory.exists(filename) && !ioFactory.delete(filename)) {
            throw new IOException("Cannot delete file " + filename);
        }
        ioFactory.createNewFile(filename);
    }

    public static void saveProperties(IOFactory ioFactory, Properties properties, String filename) throws ConfigurationException, IOException {
        try (OutputStream propertiesOutputStream = ioFactory.getOutputStream(filename)) {
            properties.save(propertiesOutputStream);
        }
    }

    /**
     * Closes this pass, releasing all resources.
     *
     * @throws org.apache.commons.configuration.ConfigurationException
     * @throws java.io.IOException
     */
    public void close() throws ConfigurationException, IOException {
        if (numOccurrences > 0) {
            dumpBatch();
        }

        if (numOccurrences == 0) {
            if (batch == 0) {
                // Special case: no term has been indexed. We generate an empty batch.
                final String batchBasename = batchBasename(0, basename, batchDir);
                LOGGER.debug("Generating empty index " + batchBasename);
                makeEmpty(ioFactory, batchBasename + DiskBasedIndex.TERMS_EXTENSION);
                makeEmpty(ioFactory, batchBasename + DiskBasedIndex.FREQUENCIES_EXTENSION);
                makeEmpty(ioFactory, batchBasename + DiskBasedIndex.OCCURRENCIES_EXTENSION);
                if (!indexingIsVirtual) {
                    sizes.close();
                }

                final IndexWriter indexWriter = new BitStreamIndexWriter(ioFactory, batchBasename, totDocuments, true, flags);
                indexWriter.close();
                final Properties properties = indexWriter.properties();
                properties.setProperty(Index.PropertyKeys.TERMPROCESSOR, ObjectParser.toSpec(termProcessor));
                properties.setProperty(Index.PropertyKeys.OCCURRENCES, 0);
                properties.setProperty(Index.PropertyKeys.MAXCOUNT, 0);
                properties.setProperty(Index.PropertyKeys.MAXDOCSIZE, maxDocSize);
                properties.setProperty(Index.PropertyKeys.SIZE, 0);
                if (field != null) {
                    properties.setProperty(Index.PropertyKeys.FIELD, field);
                }
                saveProperties(ioFactory, properties, batchBasename + DiskBasedIndex.PROPERTIES_EXTENSION);
                batch = 1;
            } else {
                ioFactory.delete(batchBasename(batch, basename, batchDir) + DiskBasedIndex.SIZES_EXTENSION); // When there is a batch but no documents.
            }
        }

        termMap = null;

        final Properties properties = new Properties();
        if (field != null) {
            properties.setProperty(Index.PropertyKeys.FIELD, field);
        }
        properties.setProperty(Index.PropertyKeys.BATCHES, batch);
        properties.setProperty(Index.PropertyKeys.DOCUMENTS, totDocuments);
        properties.setProperty(Index.PropertyKeys.MAXDOCSIZE, globMaxDocSize);
        properties.setProperty(Index.PropertyKeys.MAXCOUNT, maxCount);
        properties.setProperty(Index.PropertyKeys.OCCURRENCES, totOccurrences);
        properties.setProperty(Index.PropertyKeys.POSTINGS, totPostings);
        properties.setProperty(Index.PropertyKeys.TERMPROCESSOR, termProcessor.getClass().getName());

        if (!indexingIsVirtual) {
            // This set of batches can be seen as a documental cluster index.
            final Properties clusterProperties = new Properties();
            clusterProperties.addAll(properties);
            clusterProperties.setProperty(Index.PropertyKeys.TERMS, -1);
            clusterProperties.setProperty(DocumentalCluster.PropertyKeys.BLOOM, false);
            clusterProperties.setProperty(IndexCluster.PropertyKeys.FLAT, false);

            if (indexingIsStandard) {
                clusterProperties.setProperty(Index.PropertyKeys.INDEXCLASS, DocumentalConcatenatedCluster.class
                        .getName());
                IOFactories.storeObject(ioFactory, new ContiguousDocumentalStrategy(cutPoints.toLongArray()), basename + CLUSTER_STRATEGY_EXTENSION);

            } else { // Remapped
                clusterProperties.setProperty(Index.PropertyKeys.INDEXCLASS, DocumentalMergedCluster.class
                        .getName());
                IOFactories.storeObject(ioFactory, new IdentityDocumentalStrategy(batch, totDocuments), basename + CLUSTER_STRATEGY_EXTENSION);
            }
            clusterProperties.setProperty(IndexCluster.PropertyKeys.STRATEGY, basename + CLUSTER_STRATEGY_EXTENSION);
            for (int i = 0; i < batch; i++) {
                clusterProperties.addProperty(IndexCluster.PropertyKeys.LOCALINDEX, batchBasename(i, basename, batchDir));
            }
            saveProperties(ioFactory, clusterProperties, basename + CLUSTER_PROPERTIES_EXTENSION);

        }

        saveProperties(ioFactory, properties, basename + DiskBasedIndex.PROPERTIES_EXTENSION);
    }

    @Override
    public String toString() {
        return this.getClass().getSimpleName() + "(" + basename + ":" + field + ")";

    }

    /**
     * An accumulator for payloads.
     *
     * <P>
     * This class is essentially a stripped-down version of {@link CorpprocScan}
     * that just accumulate payloads in a bitstream and releases them in
     * batches. The main difference is that neither sizes nor occurrencies are
     * saved (as they would not make much sense).
     */
    protected static class PayloadAccumulator {

        /**
         * The I/O factory that will be used to create files.
         */
        private final IOFactory ioFactory;

        /**
         * The current basename of the overall index (usually some basename
         * postfixed with the field name).
         */
        private final String basename;

        /**
         * The field name, if available.
         */
        private final String field;

        /**
         * The total number of postings (pairs term/document).
         */
        private long totPostings;

        /**
         * The directory where batches files will be created.
         */
        private final File batchDir;

        /**
         * The flag map for batches.
         */
        final Map<Component, Coding> flags;

        /**
         * The total number of documents.
         */
        private int totDocuments;

        /**
         * The number of documents indexed so far in the current batch.
         */
        private int documentCount;

        /**
         * The current batch.
         */
        private int batch;

        /**
         * The type of indexing for this scan.
         */
        private final IndexingType indexingType;

        /**
         * The pointers into the stream, if {@link #indexingType} is
         * {@link IndexingType#REMAPPED}.
         */
        private long position[];

        /**
         * The output stream underlying this accumulator.
         */
        private FastByteArrayOutputStream accumulatorStream;

        /**
         * The accumulating output bit stream, wrapping
         * {@link #accumulatorStream}.
         */
        private OutputBitStream accumulator;

        /**
         * The cutpoints of the batches (for building later a
         * {@link it.unimi.di.big.mg4j.index.cluster.ContiguousDocumentalStrategy}).
         */
        protected final LongArrayList cutPoints;

        /**
         * The payload accumulated by this accumulator.
         */
        private final Payload payload;

        /**
         * The maximum document ever seen in the current batch.
         */
        private int maxDocInBatch;

        /**
         * Creates a new accumulator.
         *
         * @param ioFactory the factory that will be used to perform I/O.
         * @param basename the basename (usually a global filename followed by
         * the field name, separated by a dash).
         * @param payload the payload stored by this accumulator.
         * @param field the name of the accumulated field.
         * @param indexingType the type of indexing procedure.
         * @param documentsPerBatch the number of documents in each batch.
         * @param batchDir a directory for batch files; batch names will be
         * relativised to this directory if it is not <code>null</code>.
         */
        public PayloadAccumulator(final IOFactory ioFactory, final String basename, final Payload payload, final String field, final IndexingType indexingType, final int documentsPerBatch, final File batchDir) {
            this.basename = basename;
            this.ioFactory = ioFactory;
            this.payload = payload;
            this.field = field;
            this.indexingType = indexingType;
            if (indexingType != IndexingType.STANDARD && indexingType != IndexingType.REMAPPED) {
                throw new UnsupportedOperationException("Non-standard payload-based indices support only standard and remapped indexing");
            }
            if (indexingType == IndexingType.REMAPPED) {
                position = new long[documentsPerBatch];
            }
            this.batchDir = batchDir;
            this.cutPoints = new LongArrayList();
            this.cutPoints.add(0);

            flags = new EnumMap<>(CompressionFlags.DEFAULT_PAYLOAD_INDEX);
            accumulatorStream = new FastByteArrayOutputStream();
            accumulator = new OutputBitStream(accumulatorStream);
        }

        /**
         * Writes in compressed form the data currently accumulated.
         *
         * @throws java.io.IOException
         * @throws org.apache.commons.configuration.ConfigurationException
         */
        protected void writeData() throws IOException, ConfigurationException {
            final String batchBasename = batchBasename(batch, basename, batchDir);

            LOGGER.debug("Generating index " + batchBasename + "; documents: " + documentCount);

            try {
                accumulator.flush();
                final InputBitStream ibs = new InputBitStream(accumulatorStream.array);
                final IndexWriter indexWriter = new BitStreamIndexWriter(ioFactory, batchBasename, indexingType == IndexingType.STANDARD ? documentCount : maxDocInBatch + 1, true, flags);
                indexWriter.newInvertedList();
                indexWriter.writeFrequency(documentCount);
                OutputBitStream obs;

                if (indexingType == IndexingType.STANDARD) {
                    for (int i = 0; i < documentCount; i++) {
                        obs = indexWriter.newDocumentRecord();
                        indexWriter.writeDocumentPointer(obs, i);
                        payload.read(ibs);
                        indexWriter.writePayload(obs, payload);
                    }
                } else {
                    // We sort position by pointed document pointer.
                    LongArrays.quickSort(position, 0, documentCount, new AbstractLongComparator() {
                        private static final long serialVersionUID = 1L;

                        @Override
                        public int compare(final long position0, final long position1) {
                            try {
                                ibs.position(position0);
                                final int d0 = ibs.readDelta();
                                ibs.position(position1);
                                return d0 - ibs.readDelta();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    });
                    for (int i = 0; i < documentCount; i++) {
                        obs = indexWriter.newDocumentRecord();
                        ibs.position(position[i]);
                        indexWriter.writeDocumentPointer(obs, ibs.readDelta());
                        payload.read(ibs);
                        indexWriter.writePayload(obs, payload);
                    }
                }

                indexWriter.close();

                final Properties properties = indexWriter.properties();
                totPostings += properties.getLong("postings");
                properties.setProperty(Index.PropertyKeys.OCCURRENCES, -1);
                properties.setProperty(Index.PropertyKeys.MAXDOCSIZE, -1);
                properties.setProperty(Index.PropertyKeys.SIZE, indexWriter.writtenBits());
                properties.setProperty(Index.PropertyKeys.TERMPROCESSOR, NullTermProcessor.class.getName());
                properties.setProperty(Index.PropertyKeys.PAYLOADCLASS, payload.getClass().getName());
                if (field != null) {
                    properties.setProperty(Index.PropertyKeys.FIELD, field);
                }
                saveProperties(ioFactory, properties, batchBasename + DiskBasedIndex.PROPERTIES_EXTENSION);

                // We *must* generate a fake term file, or index combination won't work.
                final PrintWriter termWriter = new PrintWriter(ioFactory.getOutputStream(batchBasename + DiskBasedIndex.TERMS_EXTENSION));
                termWriter.println("#");
                termWriter.close();

                cutPoints.add(cutPoints.getLong(cutPoints.size() - 1) + documentCount);
                accumulatorStream.reset();
                accumulator.writtenBits(0);
                documentCount = 0;
                maxDocInBatch = -1;
                batch++;
            } catch (IOException e) {
                LOGGER.error("I/O Error on batch " + batch);
                throw e;
            }
        }

        /**
         * Processes the payload of a given document.
         *
         * @param documentPointer the document pointer.
         * @param content the payload.
         * @throws java.io.IOException
         */
        public void processData(final int documentPointer, final Object content) throws IOException {
            // We write document pointers only for non-standard indices.
            if (indexingType != IndexingType.STANDARD) {
                position[documentCount] = accumulator.writtenBits();
                accumulator.writeDelta(documentPointer);
            }
            // TODO: devise an out-of-memory-error check mechanism similar to that of ByteArrayPostingList.
            payload.set(content);
            payload.write(accumulator);

            if (documentPointer > maxDocInBatch) {
                maxDocInBatch = documentPointer;
            }
            documentCount++;
            totDocuments++;
        }

        /**
         * Closes this accumulator, releasing all resources.
         *
         * @throws org.apache.commons.configuration.ConfigurationException
         * @throws java.io.IOException
         */
        public void close() throws ConfigurationException, IOException {
            if (documentCount > 0) {
                writeData();
            }

            if (totDocuments == 0) {
                // Special case: no document has been indexed. We generate an empty batch.
                final String batchBasename = batchBasename(0, basename, batchDir);
                LOGGER.debug("Generating empty index " + batchBasename);

                final IndexWriter indexWriter = new BitStreamIndexWriter(ioFactory, batchBasename, 0, true, flags);
                indexWriter.close();
                final Properties properties = indexWriter.properties();
                properties.setProperty(Index.PropertyKeys.SIZE, 0);
                properties.setProperty(Index.PropertyKeys.OCCURRENCES, -1);
                properties.setProperty(Index.PropertyKeys.MAXCOUNT, -1);
                properties.setProperty(Index.PropertyKeys.MAXDOCSIZE, -1);
                properties.setProperty(Index.PropertyKeys.TERMPROCESSOR, NullTermProcessor.class.getName());
                properties.setProperty(Index.PropertyKeys.PAYLOADCLASS, payload.getClass().getName());
                if (field != null) {
                    properties.setProperty(Index.PropertyKeys.FIELD, field);
                }
                saveProperties(ioFactory, properties, batchBasename + DiskBasedIndex.PROPERTIES_EXTENSION);
                makeEmpty(ioFactory, batchBasename + DiskBasedIndex.TERMS_EXTENSION);
                batch = 1;
            }

            accumulator = null;
            accumulatorStream = null;
            position = null;

            final Properties properties = new Properties();
            if (field != null) {
                properties.setProperty(Index.PropertyKeys.FIELD, field);
            }
            properties.setProperty(Index.PropertyKeys.BATCHES, batch);
            properties.setProperty(Index.PropertyKeys.DOCUMENTS, totDocuments);
            properties.setProperty(Index.PropertyKeys.POSTINGS, totPostings);
            properties.setProperty(Index.PropertyKeys.OCCURRENCES, -1);
            properties.setProperty(Index.PropertyKeys.MAXCOUNT, -1);
            properties.setProperty(Index.PropertyKeys.MAXDOCSIZE, -1);
            properties.setProperty(Index.PropertyKeys.TERMPROCESSOR, NullTermProcessor.class.getName());
            properties.setProperty(Index.PropertyKeys.PAYLOADCLASS, payload.getClass().getName());

            // This set of batches can be seen as a documental cluster index.
            final Properties clusterProperties = new Properties();
            clusterProperties.addAll(properties);
            clusterProperties.setProperty(Index.PropertyKeys.TERMS, 1);
            clusterProperties.setProperty(IndexCluster.PropertyKeys.BLOOM, false);
            clusterProperties.setProperty(IndexCluster.PropertyKeys.FLAT, true);

            if (indexingType == IndexingType.STANDARD) {
                clusterProperties.setProperty(Index.PropertyKeys.INDEXCLASS, DocumentalConcatenatedCluster.class.getName());
                IOFactories.storeObject(ioFactory, new ContiguousDocumentalStrategy(cutPoints.toLongArray()), basename + CLUSTER_STRATEGY_EXTENSION);
            } else {
                clusterProperties.setProperty(Index.PropertyKeys.INDEXCLASS, DocumentalMergedCluster.class.getName());
                IOFactories.storeObject(ioFactory, new IdentityDocumentalStrategy(batch, totDocuments), basename + CLUSTER_STRATEGY_EXTENSION);
            }
            clusterProperties.setProperty(IndexCluster.PropertyKeys.STRATEGY, basename + CLUSTER_STRATEGY_EXTENSION);
            for (int i = 0; i < batch; i++) {
                clusterProperties.addProperty(IndexCluster.PropertyKeys.LOCALINDEX, batchBasename(i, basename, batchDir));
            }
            saveProperties(ioFactory, clusterProperties, basename + CLUSTER_PROPERTIES_EXTENSION);

            saveProperties(ioFactory, properties, basename + DiskBasedIndex.PROPERTIES_EXTENSION);
        }
    }
}
