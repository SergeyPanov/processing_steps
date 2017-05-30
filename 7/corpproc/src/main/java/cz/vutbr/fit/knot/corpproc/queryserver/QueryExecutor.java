/**
 * Class querying on the MG4J engine.
 */
package cz.vutbr.fit.knot.corpproc.queryserver;

import cz.vutbr.fit.knot.corpproc.cli.CommandUpdateCollection;
import cz.vutbr.fit.knot.corpproc.config.ConfigAndPreformate;
import cz.vutbr.fit.knot.corpproc.indexer.CorpprocDocumentCollection;
import it.unimi.di.big.mg4j.document.Document;
import it.unimi.di.big.mg4j.document.DocumentCollection;
import it.unimi.di.big.mg4j.index.Index;
import it.unimi.di.big.mg4j.index.TermProcessor;
import it.unimi.di.big.mg4j.query.IntervalSelector;
import it.unimi.di.big.mg4j.query.Query;
import it.unimi.di.big.mg4j.query.QueryEngine;
import it.unimi.di.big.mg4j.query.SelectedInterval;
import it.unimi.di.big.mg4j.query.nodes.QueryBuilderVisitorException;
import it.unimi.di.big.mg4j.query.parser.QueryParserException;
import it.unimi.di.big.mg4j.query.parser.SimpleParser;
import it.unimi.di.big.mg4j.search.DocumentIteratorBuilderVisitor;
import it.unimi.di.big.mg4j.search.score.DocumentScoreInfo;
import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.objects.Object2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.objects.Object2ReferenceLinkedOpenHashMap;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.Reference2DoubleOpenHashMap;
import it.unimi.dsi.fastutil.objects.Reference2ObjectMap;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.IntStream;
import org.apache.commons.configuration.ConfigurationException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

public class QueryExecutor {

    private final DocumentCollection documentCollection;
    private final QueryEngine queryEngine;

    private int total;
    private List<Result> results;

    private final ConfigAndPreformate configAndPreformate;


    public QueryExecutor(ConfigAndPreformate configAndPreformate, DocumentCollection documentCollection, Path directory, String basename) throws ConfigurationException, ClassNotFoundException, SecurityException, InstantiationException, IllegalAccessException, InvocationTargetException, NoSuchMethodException, IOException, URISyntaxException {
        this.configAndPreformate = configAndPreformate;
        this.documentCollection = documentCollection;

        final Object2ReferenceLinkedOpenHashMap<String, Index> indexMap = new Object2ReferenceLinkedOpenHashMap<>(Hash.DEFAULT_INITIAL_SIZE, .5f);

        for (String indexName : configAndPreformate.getFields()) {
            final Index index = Index.getInstance(directory.resolve(basename + '-' + indexName).toString());

            if (documentCollection != null && index.numberOfDocuments != documentCollection.size()) {
                System.err.println("Index " + index + " has " + index.numberOfDocuments + " documents, but the document collection has size " + documentCollection.size());
            }
            indexMap.put(index.field != null ? index.field : basename + '-' + indexName, index);
        }

        final Object2ObjectOpenHashMap<String, TermProcessor> termProcessors = new Object2ObjectOpenHashMap<>(indexMap.size());
        indexMap.forEach((alias, index) -> termProcessors.put(alias, index.termProcessor));

        queryEngine = new QueryEngine(
                new SimpleParser(indexMap.keySet(), indexMap.firstKey(), termProcessors),
                new DocumentIteratorBuilderVisitor(indexMap, indexMap.get(indexMap.firstKey()), Query.MAX_STEMMING),
                indexMap
        );

        final Reference2DoubleOpenHashMap<Index> index2Weight = new Reference2DoubleOpenHashMap<>();
        indexMap.forEach((name, index) -> index2Weight.put(index, 1));
        queryEngine.setWeights(index2Weight);

        queryEngine.intervalSelector = new IntervalSelector(Integer.MAX_VALUE, Integer.MAX_VALUE);
        queryEngine.multiplex = false;
    }

    /**
     * Return number of documents which are contain 'amountOfSnippets' snippets
     * @param amountOfSnippets
     * @return
     */
    //TODO: Make better algorithm for assumption
    private int getNumbersOfDocuments(int amountOfSnippets){
        if(amountOfSnippets == 0){
            return 15;
        }
        int length = String.valueOf(amountOfSnippets).length();
        return (int) Math.pow(10, (length - 2 < 0 ? 0 : length - 2));
    }

    /**
     * Gets snippets from MG4J engine and process them.
     * @param mg4jQuery
     * @param node
     * @param offset
     * @param format
     */

    public void run(String mg4jQuery, Node node, int offset, Map<String, Object> format, int snippets) {
        final ObjectArrayList<DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>>> resultList = new ObjectArrayList<>();
        results = new ArrayList<>();
        try {
            int totalSnippets = 0;

            do {
                total = queryEngine.process(mg4jQuery, offset, getNumbersOfDocuments(snippets), resultList);

                for (DocumentScoreInfo<Reference2ObjectMap<Index, SelectedInterval[]>> dsi : resultList) {
                    try (Document d = documentCollection.document(dsi.document)) {
                        List<String> data = new ArrayList<>();

                        if (dsi.info != null && queryEngine.intervalSelector != null) {
                            SelectedInterval[] interval = dsi.info.values().stream().findFirst().get();
                            SnippetHolder holder = new SnippetHolder(configAndPreformate, interval, documentCollection, d, format);
                            data = holder.makeResult(node);
                        }
                        results.add(new Result(
                                d.title().toString().trim(),
                                d.uri().toString().trim(),
                                data,
                                dsi.document
                        ));
                        totalSnippets += data.size();
                    }
                }
                offset += getNumbersOfDocuments(snippets);
            }while (totalSnippets < snippets && resultList.size() > 0);

        } catch (QueryParserException | QueryBuilderVisitorException | IOException e) {
            System.err.println(e.getCause());
            results = null;
        }

    }

    /**
     * Return content of whole document with some meta information,
     * @param node
     * @param id
     * @param format
     * @return
     * @throws IOException
     */

    public JSONObject getDocument(Node node, int id, Map<String, Object> format) throws IOException {
        try (Document document = documentCollection.document(id)) {
            JSONObject doc = new JSONObject();
            doc.put("data", new SnippetHolder(configAndPreformate, documentCollection, document, format).makeResult(node));
            doc.put("title", document.title());
            doc.put("uri", document.uri());
            doc.put("doc", id);
            return doc;
        }
    }

    public int getDocumentsInCollection() {
        return (int) documentCollection.size();
    }

    public int getTotal() {
        return total;
    }

    public List<Result> getResults() {
        return results;
    }

    public static List<QueryExecutor> loadCollections(ConfigAndPreformate configAndPreformate, List<String> collections) throws IOException {
        List<QueryExecutor> list = new ArrayList<>(collections.size());
        for (String collection : collections) {
            for (Path colPath : Files.find(Paths.get(collection), 1, (path, attrs) -> path.toString().endsWith(".collection")).toArray(Path[]::new)) {
                try {
                    byte[] metaBytes = Files.readAllBytes(colPath);
                    if (metaBytes[0] == (byte) 0xac && metaBytes[1] == (byte) 0xed) {
                        new CommandUpdateCollection(colPath.toString()).run(configAndPreformate);
                        metaBytes = Files.readAllBytes(colPath);
                    }

                    JSONObject meta = (JSONObject) new JSONParser().parse(new String(metaBytes, StandardCharsets.UTF_8));
                    JSONArray fileArray = (JSONArray) meta.get("files");
                    String[] files = new String[fileArray.size()];
                    IntStream.range(0, files.length).forEach(i -> {
                        files[i] = (String) fileArray.get(i);
                    });

                    DocumentCollection col = new CorpprocDocumentCollection(
                            files,
                            configAndPreformate.getFactory(),
                            (boolean) meta.get("sentences"),
                            (boolean) meta.get("gzipped"),
                            configAndPreformate.getFields().size()
                    );
                    list.add(new QueryExecutor(
                            configAndPreformate,
                            col,
                            Paths.get(collection).toRealPath(),
                            colPath.getFileName().toString().replace(".collection", "")
                    ));
                } catch (IOException | ClassNotFoundException | IllegalArgumentException | ParseException | SecurityException | URISyntaxException | NoSuchMethodException | InvocationTargetException | IllegalAccessException | InstantiationException | ConfigurationException e) {
                    throw new IOException("Couldn't load collection '" + collection + "': " + e.getMessage(), e);
                }
            }
        }
        return list;
    }
}
