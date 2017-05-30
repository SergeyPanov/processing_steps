/**
 * Class is used in case of 'update-collection' job.
 */
package cz.vutbr.fit.knot.corpproc.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import cz.vutbr.fit.knot.corpproc.cli.stubs.CustomDocumentCollection;
import cz.vutbr.fit.knot.corpproc.cli.stubs.CustomDocumentFactory;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectStreamClass;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;

import cz.vutbr.fit.knot.corpproc.config.ConfigAndPreformate;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

@Parameters(commandDescription = "Update a .collection file.")
public class CommandUpdateCollection {

    @Parameter(description = "COLLECTION-FILE", required = true)
    private List<String> collections;

    public CommandUpdateCollection() {
    }

    public CommandUpdateCollection(String file) {
        collections = Arrays.asList(file);
    }

    public void run(ConfigAndPreformate configAndPreformate) throws ParameterException, IOException {
        System.out.println("Reading collection file " + collections.get(0) + "... ");

        CustomDocumentCollection cs;
        try (ObjectInputStream ois = new StubInputStream(new FileInputStream(collections.get(0)))) {
            cs = (CustomDocumentCollection) ois.readObject();
        } catch (ClassNotFoundException e) {
            System.out.println("error.");
            throw new IOException("Error reading the object file: " + e.getMessage(), e);
        }

        Files.write(Paths.get(collections.get(0)), getMetadata(cs.file, cs.phrase, cs.gzipped));
        System.out.println("done.");
    }

    private class StubInputStream extends ObjectInputStream {

        public StubInputStream(InputStream is) throws IOException {
            super(is);
        }

        @Override
        protected Class<?> resolveClass(ObjectStreamClass desc) throws IOException, ClassNotFoundException {
            switch (desc.getName()) {
                case "it.unimi.di.big.mg4j.document.CustomDocumentCollection":
                    return CustomDocumentCollection.class;
                case "it.unimi.di.big.mg4j.document.CustomDocumentFactory":
                    return CustomDocumentFactory.class;
                default:
                    return super.resolveClass(desc);
            }
        }
    }

    public static byte[] getMetadata(String[] files, boolean sentences, boolean gzipped) {
        JSONObject meta = new JSONObject();
        JSONArray fileArray = new JSONArray();
        fileArray.addAll(Arrays.asList(files));
        meta.put("files", fileArray);
        meta.put("sentences", sentences);
        meta.put("gzipped", gzipped);
        return meta.toJSONString().getBytes(StandardCharsets.UTF_8);
    }
}
