/**
 * Class represents cz.vutbr.fit.knot.corproc.query.
 * Just send message to the server and return an answer.
 */
package cz.vutbr.fit.knot.corproc.query;


import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import org.restlet.Client;
import org.restlet.Context;
import org.restlet.Request;
import org.restlet.Response;
import org.restlet.data.MediaType;
import org.restlet.data.Method;
import org.restlet.data.Protocol;
import org.restlet.ext.json.JsonRepresentation;


import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


public class Query {
    private JSONObject message = new JSONObject();
    private String responsibleHost;
    private String readTimeout = "0";


    /**
     * Communicate with server 'responsibleHost'
     * @return
     * @throws Exception
     */
    public JSONObject startCommunication() throws Exception {
        JSONObject hostsResponse = new JSONObject();
        int totalSnippets = 0;
        try{
            List<JSONObject> snippets = new ArrayList<>();

            Request request = new Request();
            request.setResourceRef("http://" + this.responsibleHost);
            request.setMethod(Method.POST);
            request.setEntity(message.toString(), MediaType.APPLICATION_JSON);
            Context ctx = new Context();
            ctx.getParameters().add("readTimeout", readTimeout);
            Client client = new Client(ctx, Protocol.HTTP);
            Response response = client.handle(request);
            response.getAttributes();
            JsonRepresentation jsonRep = new JsonRepresentation(response.getEntity());
            JSONObject json = jsonRep.getJsonObject();
            if (json.getBoolean("error")){
                throw new Exception(json.getString("errorMessage"));
            }
            JSONArray results;
            if (json.has("results")){
                results = (JSONArray) json.get("results");
            }else {
                results = new JSONArray();
            }




            // Process all array of result list.
            for (int i = 0; i < results.length(); i++) {
                JSONObject obj = (JSONObject) results.get(i);
                JSONArray data = ((JSONObject) obj).getJSONArray("data");

                JSONObject aux = new JSONObject();
                aux.put("doc", ((JSONObject) results.get(i)).get("doc"));
                aux.put("thread", ((JSONObject) results.get(i)).get("thread"));
                aux.put("title", ((JSONObject) results.get(i)).get("title"));
                aux.put("uri", ((JSONObject) results.get(i)).get("uri"));
                aux.put("data", data);
                totalSnippets += data.length();
                snippets.add(aux);
            }
            hostsResponse = getRespSharedHead(json);
            if (json.has("allowedFields")){
                hostsResponse.put("allowedFields", json.getJSONObject("allowedFields"));
            }
            hostsResponse.put("offset", message.getInt("offset") + hostsResponse.getInt("offset"));
            hostsResponse.put("snippetsFromHost", snippets);
            hostsResponse.put("recivedSnippets", totalSnippets);

        } catch (JSONException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return hostsResponse;
    }


    /**
     * Extract information which are relevant for host.
     * @param json
     * @return
     * @throws JSONException
     */
    private JSONObject getRespSharedHead(JSONObject json) throws JSONException {
        JSONObject sharedInformation = new JSONObject();
        sharedInformation.put("total", json.getInt("total"));
        sharedInformation.put("hostname", json.getString("hostname"));
        sharedInformation.put("documents", json.getInt("documents"));
        sharedInformation.put("time", json.getInt("time"));
        sharedInformation.put("error", json.getString("error"));
        sharedInformation.put("expectedSnippets", json.getInt("expectedSnippets"));
        sharedInformation.put("offset", json.getInt("offset"));
        return sharedInformation;
    }


    public JSONObject getMessage() {
        return message;
    }

    public void setMessage(JSONObject message) {
        this.message = message;
    }

    public String getResponsibleHost() {
        return responsibleHost;
    }

    public void setResponsibleHost(String responsibleHost) {
        this.responsibleHost = responsibleHost;
    }

    public void setReadTimeout(String readTimeout) {
        this.readTimeout = readTimeout;
    }
}
