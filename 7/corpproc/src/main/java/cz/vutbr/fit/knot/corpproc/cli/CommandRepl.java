/**
 * Class is used in case of 'repl' job.
 */
package cz.vutbr.fit.knot.corpproc.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import cz.vutbr.fit.knot.corpproc.config.ConfigAndPreformate;
import cz.vutbr.fit.knot.corpproc.queryserver.ConstraintHolder;
import cz.vutbr.fit.knot.corpproc.queryserver.QueryExecutor;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

@Parameters(commandDescription = "Run a REPL from which a user can query the index.")
public class CommandRepl {

    @Parameter(names = {"-I", "--input"}, description = "A file containing the input.")
    private String input;

    @Parameter(description = "INDEX-DIR [INDEX-DIR...]")
    private List<String> collections;

    private List<QueryExecutor> proxies = new ArrayList<>();
    private ConfigAndPreformate configAndPreformate;

    public void run(ConfigAndPreformate configAndPreformate) throws ParameterException, IOException {
        proxies = QueryExecutor.loadCollections(configAndPreformate, collections);
        this.configAndPreformate = configAndPreformate;

        InputStream in;
        try {
            in = input != null ? new FileInputStream(input) : System.in;
        } catch (FileNotFoundException e) {
            throw new IOException("Couldn't read file " + input + ": " + e.getMessage(), e);
        }
        BufferedReader br = new BufferedReader(new InputStreamReader(in, StandardCharsets.UTF_8));

        while (true) {
            System.out.print(">");
            final String query;
            try {
                query = br.readLine();
            } catch (IOException e) {
                throw new IOException("Failed to read from input: " + e.getMessage(), e);
            }
            if (query == null) {
                System.err.println();
                return;
            }
            if (query.length() == 0) {
                continue;
            }
            runQuery(query);
        }
    }

    private void runQuery(String query) {
        final AtomicInteger processedDocuments = new AtomicInteger(0);
        final AtomicInteger foundResults = new AtomicInteger(0);
        long startTime = System.nanoTime();

        String mg4jQuery = query.replaceAll("(?<!param)[1-9][0-9]*:", "");

        proxies.parallelStream().map(proxy -> {
            try {
                proxy.run(mg4jQuery,  null, 0, new HashMap<>(), 0);
            } catch (Exception e) {
                System.err.println(e.getMessage());
                e.printStackTrace(System.err);
            }
            processedDocuments.getAndAdd(proxy.getTotal());
            return proxy.getResults();
        }).forEachOrdered(xs -> xs.stream().forEach((item) -> {
            foundResults.getAndIncrement();
            System.err.println("Title: " + item.getTitle() + "\nData:" + item.getData() + "\n");
        }));

        long time = System.nanoTime() - startTime;

        NumberFormat nf = NumberFormat.getInstance();
        System.err.println(foundResults.get() + " results; "
                + processedDocuments.get() + " documents examined; "
                + time / 1000000. + " ms; "
                + nf.format(processedDocuments.get() * 1000000000.0 / time) + " documents/s, "
                + nf.format(time / (double) processedDocuments.get()) + " ns/document");
    }
}
