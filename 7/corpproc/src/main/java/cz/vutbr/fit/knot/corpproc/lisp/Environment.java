package cz.vutbr.fit.knot.corpproc.lisp;

import java.util.HashMap;
import java.util.Map;

public class Environment {

    Map<Symbol, LispObject> funs;
    Map<Symbol, LispObject> vals = new HashMap<>();
    Environment parent = null;
    
    public Environment() {
        funs = new HashMap<>();
    }

    public Environment(Environment parent) {
        this.parent = parent;
    }
    
    public void bindValue(Symbol s, LispObject v) {
        vals.put(s, v);
    }
    
    public LispObject lookupValue(Symbol s) {
        LispObject obj = vals.get(s);
        if (obj == null && parent != null) {
            return parent.lookupValue(s);
        } else {
            return obj;
        }
    }
    
    public void bindFunction(Symbol s, Function v) {
        if (parent == null) {
            funs.put(s, v);
        } else {
            parent.bindFunction(s, v);
        }
    }
    
    public Function lookupFunction(Symbol s) {
        if (parent == null) {
            LispObject obj = funs.get(s);
            return (obj instanceof Function) ? (Function) obj : null;
        } else {
            return parent.lookupFunction(s);
        }
    }
    
    @Override
    public String toString() {
        return "Environment(funs: " + funs + ", vals: " + vals + ")";
    }
}
