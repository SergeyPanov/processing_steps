/**
 * Class is used in case of 'index' job.
 */

package cz.vutbr.fit.knot.corpproc.indexer;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import cz.vutbr.fit.knot.corpproc.cli.CommandUpdateCollection;
import cz.vutbr.fit.knot.corpproc.config.ConfigAndPreformate;
import it.unimi.di.big.mg4j.document.DocumentCollection;
import it.unimi.di.big.mg4j.io.IOFactory;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import org.apache.commons.configuration.ConfigurationException;

@Parameters(commandDescription = "Index a collection of documents.")
public class CommandIndex {

    @Parameter(names = {"-s", "--sentence"}, description = "Index sentences instead of documents.")
    private boolean sentences;

    @Parameter(names = {"-g", "--gzipped"}, description = "Process gzipped input files.")
    private boolean gzipped;

    @Parameter(description = "DATA-DIR [OUTPUT-DIR]", required = true)
    private List<String> collections;

    public void run(ConfigAndPreformate configAndPreformate) throws ParameterException, IOException {
        Path dataDir = Paths.get(collections.get(0));
        Path outputDir = Paths.get(collections.size() > 1 ? collections.get(1) : ".");
        outputDir.toFile().mkdirs();

        System.out.println("Processing " + dataDir + "...");

        //Load the files that should be indexed
        Path basePath = dataDir.toRealPath();
        String basename = basePath.getFileName().toString();
        String[] files = Files.list(basePath)
                .filter(Files::isRegularFile).map(Path::toString)
                .toArray(String[]::new);

        //Create the collection
        DocumentCollection collection = new CorpprocDocumentCollection(
                files, configAndPreformate.getFactory(), sentences, gzipped, configAndPreformate.getFields().size());

        //Store collection metadata
        Path outputFile = outputDir.resolve(basename + ".collection");
        Files.write(outputFile, CommandUpdateCollection.getMetadata(files, sentences, gzipped));
        System.out.println("Saved collection metadata to " + outputFile);

        //Start the indexing process
        System.out.println("Indexing " + dataDir + "...");
        try {
            new CorpprocIndexBuilder(basename, collection)
                    .ioFactory(new DirectoryIOFactory(outputDir))
                    .run();
        } catch (SecurityException | URISyntaxException | ClassNotFoundException | InstantiationException | IllegalAccessException | InvocationTargetException | NoSuchMethodException | ConfigurationException ex) {
            throw new IOException("Error occurred while indexing: " + ex.getMessage());
        }
    }

    private class DirectoryIOFactory implements IOFactory {

        private final Path path;

        public DirectoryIOFactory(Path path) {
            this.path = path;
        }

        @Override
        public OutputStream getOutputStream(final String name) throws IOException {
            return new FileOutputStream(path.resolve(name).toFile());
        }

        @Override
        public InputStream getInputStream(final String name) throws IOException {
            return new FileInputStream(path.resolve(name).toFile());
        }

        @Override
        public boolean delete(final String name) {
            return path.resolve(name).toFile().delete();
        }

        @Override
        public boolean exists(final String name) {
            return path.resolve(name).toFile().exists();
        }

        @Override
        public void createNewFile(final String name) throws IOException {
            path.resolve(name).toFile().createNewFile();
        }

        @Override
        public WritableByteChannel getWritableByteChannel(String name) throws IOException {
            return new FileOutputStream(path.resolve(name).toFile()).getChannel();
        }

        @Override
        public ReadableByteChannel getReadableByteChannel(String name) throws IOException {
            return new FileInputStream(path.resolve(name).toFile()).getChannel();
        }

        @Override
        public long length(String name) throws IOException {
            return path.resolve(name).toFile().length();
        }
    }
}
