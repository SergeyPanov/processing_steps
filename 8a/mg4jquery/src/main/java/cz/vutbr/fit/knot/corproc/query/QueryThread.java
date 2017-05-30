/**
 * cz.vutbr.fit.knot.corproc.main.Main class. Runs multiple threads which are requesting servers.
 * Each thread maintain one server.
 */

package cz.vutbr.fit.knot.corproc.query;

import org.json.JSONArray;
import org.json.JSONObject;


import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class QueryThread extends Thread{
    private String responsibleHost;
    private QueryParameters params;

    private List<JSONObject> response;
    private int offset = 0;
    private int expectedSnippets = 0;

    private String getHostWithPort(){
        Pattern withoutPortPtrn = Pattern.compile("[^:]+");
        String keeper = params.getString("keeper");
        Matcher withoutPortMtch = withoutPortPtrn.matcher(keeper);
        if (withoutPortMtch.matches()){
            keeper = keeper + ":" + responsibleHost.split(":")[responsibleHost.split(":").length - 1];
        }
        return keeper;
    }


    @Override
    public void run(){

        try{
            JSONObject message = new JSONObject();
            message.put("query", params.getString("query"));
            message.put("constraint", params.getString("constraint"));
            message.put("offset", offset);
            message.put("style", params.getString("style"));
            message.put("field", params.getString("field"));
            message.put("format", params.getObject("format"));
            message.put("snippets", expectedSnippets);
            if (!params.getString("allowedFields").equals("null")){
                message.put("allowedFields", params.getString("allowedFields"));

                if (!getHostWithPort().equals(responsibleHost)){
                    return;
                }
            }
            if(params.getBoolean("getFullDocument")){
                message.put("get", params.getString("document"));
                message.put("thread", params.getInt("thread"));

                if (!getHostWithPort().equals(responsibleHost)){
                    return;
                }
            }

            Query query = new Query();
            query.setReadTimeout(params.getString("readTimeout"));
            query.setMessage(message);
            query.setResponsibleHost(responsibleHost);
            JSONObject resp = query.startCommunication();
            synchronized (this){
                JSONObject respondedJson = QueryModule.getResponseFromHost(responsibleHost, response);
                if (respondedJson == null)
                    response.add(resp);
                else {
                    respondedJson.put("offset", resp.getInt("offset"));
                    respondedJson.put("recivedSnippets", respondedJson.getInt("recivedSnippets") + resp.getInt("recivedSnippets"));
                    for(int index = 0; index < ((JSONArray)resp.get("snippetsFromHost")).length(); ++index)
                        ((JSONArray)respondedJson.get("snippetsFromHost")).put(((JSONArray)resp.get("snippetsFromHost")).get(index));
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public void setResponsibleHost(String responsibleHost) {
        this.responsibleHost = responsibleHost;
    }



    public int getOffset() {
        return offset;
    }

    public void setOffset(int offset) {
        this.offset = offset;
    }

    public int getExpectedSnippets() {
        return expectedSnippets;
    }

    public void setExpectedSnippets(int expectedSnippets) {
        this.expectedSnippets = expectedSnippets;
    }

    public void setResponse(List<JSONObject> response) {
        this.response = response;
    }

    public void setParams(QueryParameters params) {
        this.params = params;
    }
}
