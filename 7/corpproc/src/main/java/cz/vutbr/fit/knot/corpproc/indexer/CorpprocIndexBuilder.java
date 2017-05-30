package cz.vutbr.fit.knot.corpproc.indexer;

import it.unimi.di.big.mg4j.document.DocumentFactory;
import it.unimi.di.big.mg4j.document.DocumentSequence;
import it.unimi.di.big.mg4j.index.BitStreamIndex;
import it.unimi.di.big.mg4j.index.CompressionFlags;
import it.unimi.di.big.mg4j.index.DiskBasedIndex;
import it.unimi.di.big.mg4j.index.NullTermProcessor;
import it.unimi.di.big.mg4j.index.SkipBitStreamIndexWriter;
import it.unimi.di.big.mg4j.index.cluster.IndexCluster;
import it.unimi.di.big.mg4j.io.IOFactories;
import it.unimi.di.big.mg4j.io.IOFactory;
import it.unimi.di.big.mg4j.tool.Combine;
import it.unimi.di.big.mg4j.tool.Concatenate;
import it.unimi.di.big.mg4j.tool.IndexBuilder;
import it.unimi.di.big.mg4j.tool.Scan;
import it.unimi.di.big.mg4j.tool.Scan.Completeness;
import it.unimi.di.big.mg4j.tool.VirtualDocumentResolver;
import it.unimi.dsi.big.util.ImmutableExternalPrefixMap;
import it.unimi.dsi.big.util.StringMaps;
import it.unimi.dsi.logging.ProgressLogger;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.util.Arrays;
import java.util.Map;
import org.apache.commons.configuration.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * An almost redundant copy of the IndexBuilder class and its method run - the
 * only reason this is here is because of the TOKEN FieldType which is supported
 * in mg4j but not in mg4j-big (which we're using).
 */
public class CorpprocIndexBuilder extends IndexBuilder {

    final static Logger LOGGER = LoggerFactory.getLogger(CorpprocIndexBuilder.class);

    private final String basename;
    private final DocumentSequence documentSequence;

    private IOFactory ioFactory = IOFactory.FILESYSTEM_FACTORY;

    private final long logInterval = ProgressLogger.DEFAULT_LOG_INTERVAL;

    public CorpprocIndexBuilder(String basename, DocumentSequence documentSequence) {
        super(basename, documentSequence);
        this.basename = basename;
        this.documentSequence = documentSequence;
    }

    @Override
    public IndexBuilder ioFactory(final IOFactory ioFactory) {
        this.ioFactory = ioFactory;
        return this;
    }

    @Override
    public void run() throws ConfigurationException, SecurityException, IOException, URISyntaxException, ClassNotFoundException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException {
        final DocumentFactory factory = documentSequence.factory();
        if (indexedFields.isEmpty()) {
            // We index everything
            for (int i = 0; i < factory.numberOfFields(); i++) {
                if (factory.fieldType(i) != DocumentFactory.FieldType.VIRTUAL || virtualDocumentResolvers.containsKey(i)) {
                    indexedFields.add(i);
                }
            }
        }

        final int[] indexedField = indexedFields.toIntArray();

        final String[] basenameField = new String[indexedField.length];
        for (int i = 0; i < indexedField.length; i++) {
            basenameField[i] = basename + "-" + factory.fieldName(indexedField[i]);
        }
        LOGGER.info("Creating indices " + Arrays.toString(basenameField) + "...");

        // Create gap array
        final int[] virtualDocumentGap = new int[indexedField.length];
        for (int i = 0; i < indexedField.length; i++) {
            virtualDocumentGap[i] = virtualDocumentGaps.get(i);
        }

        // Create virtual document resolver array
        final VirtualDocumentResolver[] virtualDocumentResolver = new VirtualDocumentResolver[indexedField.length];
        virtualDocumentResolvers.entrySet().forEach(entry -> {
            virtualDocumentResolver[entry.getKey()] = entry.getValue();
        });

        Map<CompressionFlags.Component, CompressionFlags.Coding> flags = CompressionFlags.DEFAULT_QUASI_SUCCINCT_INDEX;

        CorpprocScan.run(ioFactory,
                basename,
                documentSequence,
                flags.containsKey(CompressionFlags.Component.POSITIONS) ? Completeness.POSITIONS
                : flags.containsKey(CompressionFlags.Component.COUNTS) ? Completeness.COUNTS
                : Completeness.POINTERS,
                NullTermProcessor.getInstance(),
                null,
                Scan.DEFAULT_BUFFER_SIZE,
                Scan.DEFAULT_BATCH_SIZE,
                Scan.DEFAULT_MAX_TERMS,
                indexedField,
                virtualDocumentResolver,
                virtualDocumentGap,
                null,
                logInterval,
                null);

        Arrays.fill(virtualDocumentResolver, null);

        for (int i = 0; i < indexedField.length; i++) {
            String[] inputBasename = IOFactories
                    .loadProperties(ioFactory, basenameField[i] + Scan.CLUSTER_PROPERTIES_EXTENSION)
                    .getStringArray(IndexCluster.PropertyKeys.LOCALINDEX);
            new Concatenate(
                    ioFactory,
                    basenameField[i],
                    inputBasename,
                    false,
                    Combine.DEFAULT_BUFFER_SIZE,
                    flags,
                    Combine.IndexType.QUASI_SUCCINCT,
                    true,
                    BitStreamIndex.DEFAULT_QUANTUM,
                    BitStreamIndex.DEFAULT_HEIGHT,
                    SkipBitStreamIndexWriter.DEFAULT_TEMP_BUFFER_SIZE,
                    logInterval
            ).run();

            CorpprocScan.cleanup(ioFactory, basenameField[i], inputBasename.length, null);
        }

        LOGGER.info("Creating term maps (class: " + ImmutableExternalPrefixMap.class.getSimpleName() + ")...");
        for (int i = 0; i < indexedField.length; i++) {
            IOFactories.storeObject(ioFactory, StringMaps.synchronize(new ImmutableExternalPrefixMap(IOFactories.fileLinesCollection(ioFactory, basenameField[i] + DiskBasedIndex.TERMS_EXTENSION, "UTF-8"))), basenameField[i] + DiskBasedIndex.TERMMAP_EXTENSION);
        }

        LOGGER.info("Indexing completed.");
    }
}
