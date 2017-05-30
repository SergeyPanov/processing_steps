package cz.vutbr.fit.knot.corproc.main;

import org.json.JSONException;
import cz.vutbr.fit.knot.corproc.query.QueryParameters;
import cz.vutbr.fit.knot.corproc.printer.ResponsePrinter;
import cz.vutbr.fit.knot.corproc.processor.ResponseProcessor;
import cz.vutbr.fit.knot.corproc.query.QueryModule;
import org.json.JSONObject;
import cz.vutbr.fit.knot.corproc.parameters.Parameters;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Main {
    private static List<JSONObject> response = new ArrayList<>();

    /**
     * Serialize offsets for each server.
     * @param offsets
     */
    private static void serializeOffsets(Map<String, Integer> offsets){
        try{
            FileOutputStream fileOutputStream = new FileOutputStream("offsets.serialized");
            ObjectOutputStream out = new ObjectOutputStream(fileOutputStream);
            out.writeObject(offsets);
            out.close();
            fileOutputStream.close();
        }catch (IOException e){
            System.err.println("Unfortunately something happened while storing information about offsets.\n" +
                    "Getting next portion on snippets wont be possible =(.");
        }

    }

    /**
     * From list off responses create a map with offsets in each server and return it.
     * @param responses
     * @return
     */
    private static Map<String, Integer> extractOffsets(List<JSONObject> responses) throws JSONException {
        Map<String, Integer> offsets = new HashMap<>();
        for (JSONObject response:
                responses) {

            if (response.getInt("expectedSnippets") <= response.getInt("recivedSnippets"))
                offsets.put(response.getString("hostname"), response.getInt("offset"));
        }
        return offsets;
    }

    /**
     * Deserialize OffsetsHolder.
     * @return
     * @throws IOException
     * @throws ClassNotFoundException
     */
    private static Map<String, Integer> deserializeOffsets(){
        Map<String, Integer> holder = null;
        try {
            FileInputStream fileInputStream = new FileInputStream("offsets.serialized");
            ObjectInputStream objectInputStream = new ObjectInputStream(fileInputStream);
            holder = (Map<String, Integer>) objectInputStream.readObject();
            objectInputStream.close();
            fileInputStream.close();
        }catch (IOException | ClassNotFoundException e){
        }
        return holder;
    }

    public static void main(String[] args) throws Exception {
        QueryModule queryModule = new QueryModule();
        Parameters params = new Parameters(args);
        params.process();

        if (!params.getBoolean("error")){
            System.setErr(new PrintStream(new OutputStream() {
                public void write(int b) {
                }
            }));
        }

        Map<String, Integer> offsets = null;
        if (params.getBoolean("next")){
            offsets = deserializeOffsets();
        }

        ResponseProcessor respProc = new ResponseProcessor();
        respProc.setDye(params.getDye());
        respProc.setStyle(params.getString("style"));
        respProc.setHead(((List<String>) params.getObject("head")).size() > 0);

        ResponsePrinter printer = new ResponsePrinter();
        printer.setProcessor(respProc);
        printer.setHead((ArrayList)params.getObject("head"));
        printer.setFullDocument(params.getBoolean("getFullDocument"));

        QueryParameters queryParameters = new QueryParameters();
        queryParameters.setParams(params.getParams());
        queryModule.startQuery((List<String>) params.getObject("listOfHosts"), queryParameters, response, printer, offsets);
        if (response.size() > 0){
            serializeOffsets(extractOffsets(response));
        }
    }
}
