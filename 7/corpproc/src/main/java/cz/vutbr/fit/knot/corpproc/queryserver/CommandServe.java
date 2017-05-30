/**
 * Class is used in case of 'serve' job.
 */
package cz.vutbr.fit.knot.corpproc.queryserver;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.beust.jcommander.Parameters;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import cz.vutbr.fit.knot.corpproc.cli.OutOfBounds;
import cz.vutbr.fit.knot.corpproc.config.ConfigAndPreformate;
import cz.vutbr.fit.knot.corpproc.config.ConfigHolder;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.UnknownHostException;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;


import org.codehaus.jparsec.error.ParserException;
import org.json.simple.JSONArray;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;


import java.util.stream.IntStream;



@Parameters(commandDescription = "Start a server responding to queries in JSON.")
public class CommandServe {


    @Parameter(names = {"-p", "--port"}, description = "Port on which will server listen")
    private int port = 12000;

    @Parameter(description = "INEDX-DIR [INDEX-DIR...]")
    private List<String> collections;

    private List<QueryExecutor> proxies;
    private String hostname;
    private ConfigAndPreformate configAndPreformate;
    private List<String> validFields;



    public void run(ConfigAndPreformate configAndPreformate) throws ParameterException, IOException {
        try {
            hostname = InetAddress.getLocalHost().getCanonicalHostName();
        } catch (UnknownHostException e) {
            hostname = "-unknown-";
        }

        proxies = QueryExecutor.loadCollections(configAndPreformate, collections);
        this.configAndPreformate = configAndPreformate;

        validFields = new ArrayList<>();
        validFields.addAll(configAndPreformate.getFields());
        validFields.addAll(configAndPreformate.getEntityProps());

        try {
            HttpServer server = HttpServer.create(new InetSocketAddress(port), 0);
            server.createContext("/", new MyHandler());
            server.setExecutor(null);
            server.start();
            System.out.println("The server is listening on port " + port);
        } catch (IOException e) {
            throw new IOException("Could not start the server: " + e.getMessage(), e);
        }
    }

    private JSONObject processInput(JSONObject input) throws OutOfBounds {

        input.put("query", QueryTranslater.getInstance().modifyQuery((String) input.get("query"), false));
        input.put("constraint", QueryTranslater.getInstance().modifyQuery((String) input.get("constraint"), true));

        if (input.get("style") != null &&
                ( ( (String)input.get("style") ).toLowerCase().equals("html") ||
                        ( (String)input.get("style") ).toLowerCase().equals("raw")) ||
                ( (String)input.get("style") ).toLowerCase().equals("ascii")) {
            ConfigHolder.getClonedConfiguration().setStyle((String) input.get("style"));
        }else {
            ConfigHolder.getClonedConfiguration().setStyle(ConfigHolder.getConfiguration().getStyle());
        }
        //Check if field from input message is valid. If not use field from configAndPreformate.
        if (input.get("field") != null && ConfigHolder.getConfiguration().getFields().contains(input.get("field"))){
            ConfigHolder.getClonedConfiguration().setField((String) input.get("field"));
        }else {
            ConfigHolder.getClonedConfiguration().setField(ConfigHolder.getConfiguration().getField());
        }

        return input;
    }

    private class MyHandler implements HttpHandler {

        @Override
        public void handle(HttpExchange t) throws IOException {
            String requestString = new Scanner(t.getRequestBody()).useDelimiter("\\Z").next();

            JSONObject input, result;
            result = new JSONObject();
            try {
                input = (JSONObject) new JSONParser().parse(requestString);

                System.out.println("Query: " + input.toJSONString());
                if(input.get("allowedFields") == null)  // In case of allowedFields is not set, should process query and constraint
                    processInput(input);

                result = process(input);
                System.out.println("Response: " + result.toJSONString());

            } catch (Exception e) {
                System.err.println(e);
                e.printStackTrace(System.err);

                result = new JSONObject();
                result.put("error", "true");
                result.put("errorMessage", e.toString());
            }

            t.sendResponseHeaders(200, 0);
            try (OutputStream os = t.getResponseBody()) {
                os.write(result.toJSONString().getBytes());
            }
        }
    }

    public JSONObject process(JSONObject input) {
        long startTime = System.nanoTime();
        JSONObject result;

        if (input.get("allowedFields") != null){
            result = processAllowedFields(input);
        }else{
            result = input.get("get") == null
                    ? processMultiple(input)
                    : processSingle(input);
        }



        result.put("time", String.valueOf(System.nanoTime() - startTime));
        result.put("hostname", hostname + ":" + String.valueOf(port));

        return result;
    }

    public JSONObject processAllowedFields(JSONObject input){
        JSONObject result = new JSONObject();
        if (((String) input.get("allowedFields")).equals("attributes")){ // Client asked for list of attributes
            JSONObject jo = new JSONObject();
            jo.put("fields", ConfigHolder.getConfiguration().getFields());
            result.put("allowedFields", jo);
        }

        if (((String) input.get("allowedFields")).equals("nertags")){
            JSONObject jo = new JSONObject();
            ((LinkedHashMap) ConfigHolder.getConfiguration().getMapping()).forEach((k, v) -> {
                System.out.print(k);
                jo.put(k, new ArrayList (((Map) ((Map) ConfigHolder.getConfiguration().getMapping()).get(k)).keySet()));
            });
            result.put("allowedFields", jo);
        }
        result.put("total", 1);
        result.put("documents", -1);
        result.put("offset", -1);
        result.put("expectedSnippets", 0);
        result.put("error", "false");
        return result;
    }

    private JSONObject processSingle(JSONObject input) {
        final String documentId = (String) input.get("get");
        final String query = (String) input.get("query");
        final String constraint = (String) input.get("constraint");
        int thread = Math.toIntExact((Long) input.get("thread"));

        Map<String, Object> format = new HashMap<>();  //Holds nertags and information which client is interested in

        if (input.get("format") != null) {
            format = processFormat((JSONObject) input.get("format"));
        }


        JSONObject result = new JSONObject();
        try {

            Node node = constraint.isEmpty() ? null : ConstraintParser.parse(validFields, constraint);
            JSONObject aux = proxies.get(thread).getDocument(node, Integer.parseInt(documentId), format);


            aux.put("thread", Integer.toString(thread));
            JSONArray arr = new JSONArray();
            arr.add(aux);
            result.put("results", arr);
            result.put("total", 1);
            result.put("documents", 1);
            result.put("offset", Integer.parseInt(documentId));
            result.put("expectedSnippets", 0);
            result.put("hostname", InetAddress.getLocalHost().getHostName() + ":" + port);
            result.put("error", "false");
        } catch (ParserException | IOException e) {
            //TODO: GUI should process the error message?
            System.err.println(e);
            e.printStackTrace(System.err);

            result.put("error", "true");
            result.put("errorMessage", e.toString());
        }

        return result;
    }

    private Map<String, Object> processFormat(JSONObject format){
        Map<String, Object> formatMap = new HashMap<>();

        List<String> fields = new ArrayList<String>();
        Map<String, List<String>> nertags = new HashMap<String, List<String>>();

        format.forEach((key, value) -> {
            if (key.equals("attributes")){
                ((JSONArray) value).forEach((v) -> {
                    fields.add(v.toString());
                });
                formatMap.put("attributes", fields);
            }
            if (key.equals("nertags")){
                ((JSONObject) value).forEach((nerKey, nerVal) -> {
                    List<String> attrs = new ArrayList<String>();
                    ((JSONArray) nerVal).forEach((attrVal) -> {
                        attrs.add(attrVal.toString());
                    });
                    nertags.put(nerKey.toString(), attrs);
                });
                formatMap.put("nertags", nertags);
            }

        });

        return formatMap;
    }

    private JSONObject processMultiple(JSONObject input) {
        final String query = ((String) input.get("query")).replace("_SENT_", "¶").replace("_PAR_", "§");
        final String constraint = (String) input.get("constraint");
        final int offset = Integer.parseInt(String.valueOf(input.get("offset")));

        String mg4jQuery = query.replaceAll("(?<!param)[1-9][0-9]*:", "");
//        List<ConstraintHolder> constraints = ConstraintHolder.parseConstraints(validFields, query);
        Node node;
        try {
            node = constraint.isEmpty() ? null : ConstraintParser.parse(validFields, constraint);
        } catch (ParserException e) {
            //TODO: GUI should process the error message?
            System.err.println(e);
            e.printStackTrace(System.err);

            JSONObject result = new JSONObject();
            result.put("error", "true");
            result.put("errorMessage", e.toString());
            return result;
        }

        final AtomicInteger documentsInCollection = new AtomicInteger(0);
        final AtomicInteger total = new AtomicInteger(0);
        final AtomicBoolean error = new AtomicBoolean(false);

        JSONArray documentResults = new JSONArray();

        IntStream.range(0, proxies.size()).parallel().forEach(i -> {
            QueryExecutor proxy = proxies.get(i);       //Cannot get here because of proxies.size == 0
            if (error.get()) {
                return;
            }

            Map<String, Object> format = new HashMap<>();  //Holds nertags and information which client is interested in

            if (input.get("format") != null) {
                format = processFormat((JSONObject) input.get("format"));
            }

            proxy.run(mg4jQuery,  node, offset, format, (((Long)input.get("snippets")).intValue() <= 0 ? 0 : ((Long)input.get("snippets")).intValue()));

            documentsInCollection.getAndAdd(proxy.getDocumentsInCollection());

            List<Result> results = proxy.getResults();
            if (results == null || error.get()) {
                error.getAndSet(true);
                return;
            }
            results.forEach(x -> {
                documentResults.add(x.asJSON(i));
                total.addAndGet(1);
            });
        });

        JSONObject result = new JSONObject();
        result.put("total", String.valueOf(total.get()));
        result.put("documents", String.valueOf(documentsInCollection.get()));
        result.put("offset", documentResults.size());
        result.put("expectedSnippets", ((Long)input.get("snippets")).intValue());

        if (!error.get()) {
            result.put("results", documentResults);
            result.put("error", "false");
        } else {
            result.put("error", "true");
            result.put("errorMessage", error.toString());
        }
        return result;
    }
}
