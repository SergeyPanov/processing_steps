/**
 * Class represents view of the server's response.
 */
package cz.vutbr.fit.knot.corproc.printer;


import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import cz.vutbr.fit.knot.corproc.processor.ResponseProcessor;

import java.io.UnsupportedEncodingException;
import java.util.List;

public class ResponsePrinter {
    private ResponseProcessor processor;
    private List<String> head;
    private boolean isFullDocument;

    private void printAllowedAttributes(JSONObject allowedAttrivutes) throws JSONException {
        for (int i = 0; i < allowedAttrivutes.names().length(); ++i){
            JSONArray fields = allowedAttrivutes.getJSONArray((String) allowedAttrivutes.names().get(i));
            System.out.print((String) allowedAttrivutes.names().get(i) + ": ");
            for (int j = 0; j < fields.length(); ++j){
                System.out.print(fields.getString(j) + ", ");
            }
            System.out.println("");
        }
    }

    public void print(List<JSONObject> target) throws JSONException, UnsupportedEncodingException {
        for (JSONObject ansFromServer:
                target) {
            if (ansFromServer != null){
                if (ansFromServer.has("allowedFields")){
                    printAllowedAttributes(ansFromServer.getJSONObject("allowedFields"));
                }else {
                    for (int i = 0; i < ((JSONArray)ansFromServer.get("snippetsFromHost")).length(); ++i){
                        JSONObject snptsFromDoc = new JSONObject();

                        snptsFromDoc = (JSONObject) ((JSONArray)ansFromServer.get("snippetsFromHost")).get(i);

                        if (head.contains("document")){
                            processor.setDocument(snptsFromDoc.getLong("doc"));
                        }
                        if (head.contains("thread")){
                            processor.setThread(snptsFromDoc.getInt("thread"));
                        }
                        if (head.contains("title")){
                            processor.setTitle(snptsFromDoc.getString("title"));
                        }
                        if (head.contains("uri")){
                            processor.setUri(snptsFromDoc.getString("uri"));
                        }
                        if (head.contains("host")){
                            processor.setHost(ansFromServer.getString("hostname"));
                        }

                        if(! isFullDocument){
                            for (int snippetFromDocIndex = 0; snippetFromDocIndex < snptsFromDoc.getJSONArray("data").length(); ++snippetFromDocIndex){
                                System.out.println(processor.processDataChunk((String) snptsFromDoc.getJSONArray("data").get(snippetFromDocIndex)));
                            }
                        }else {
                            StringBuilder fullText = new StringBuilder();
                            for(int index = 0; index < snptsFromDoc.getJSONArray("data").length(); ++index){
                                fullText.append(snptsFromDoc.getJSONArray("data").get(index));
                            }
                            System.out.println(processor.processDataChunk(fullText.toString()));
                        }

                        snptsFromDoc.remove("data");
                        snptsFromDoc.put("data", new JSONArray());

                    }
                }

                ansFromServer.remove("snippetsFromHost");
                ansFromServer.put("snippetsFromHost", new JSONArray());
            }

        }


    }


    public ResponseProcessor getProcessor() {
        return processor;
    }

    public void setProcessor(ResponseProcessor processor) {
        this.processor = processor;
    }

    public List<String> getHead() {
        return head;
    }

    public void setHead(List<String> head) {
        this.head = head;
    }

    public boolean isFullDocument() {
        return isFullDocument;
    }

    public void setFullDocument(boolean fullDocument) {
        isFullDocument = fullDocument;
    }
}
