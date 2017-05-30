package cz.vutbr.fit.knot.corpproc.queryserver;

import org.json.simple.JSONObject;

import java.util.List;

public class Result {

    private final String title;
    private final String uri;
    private final List<String> data;
    private final long document;

    public Result(String title, String uri, List<String> data, long documentId) {
        this.title = title;
        this.uri = uri;
        this.data = data;
        this.document = documentId;
    }

    public String getTitle() {
        return title;
    }

    public List<String> getData() {
        return data;
    }

    public JSONObject asJSON(int thread) {
        JSONObject obj = new JSONObject();
        obj.put("data", getData());
        obj.put("title", getTitle());
        obj.put("uri", uri);
        obj.put("doc", String.valueOf(document));
        obj.put("thread", String.valueOf(thread));
        return obj;
    }
}
