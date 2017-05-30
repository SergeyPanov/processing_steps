package cz.vutbr.fit.knot.corproc.query;

import java.util.Map;
import java.util.Objects;


public class QueryParameters {
    private Map<String, Object> params;

    public Map<String, Object> getParams() {
        return params;
    }

    public int getInt(String key) {
        String mapValue = getString(key);
        if (mapValue != null) {
            int value = Integer.parseInt(getString(key));
            return value;
        }
        return (Integer) null;
    }

    public void setParams(Map<String, Object> params) {
        this.params = params;
    }
    public Object getObject(String key){
        return params.get(key);
    }
    public void setObject(String key, Object value){
        params.put(key, value);
    }

    public String getString(String key) {
        String value = Objects.toString(params.get(key));
        return (value == null) ? null : value.trim();
    }
    public void setString(String key, String val){
        params.put(key, val);
    }
    public boolean getBoolean(String key){
        return (boolean) params.get(key);
    }
    public void setBoolean(String key, boolean val){
        params.put(key, val);
    }
}
