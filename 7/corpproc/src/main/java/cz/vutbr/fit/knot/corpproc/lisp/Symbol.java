package cz.vutbr.fit.knot.corpproc.lisp;

import java.util.HashMap;
import java.util.Map;

public final class Symbol extends LispObject {

    private String value;
    private static Map<String, Symbol> map = new HashMap<>();

    public static Symbol makeSymbol(String s) {
        Symbol symbol = map.get(s);
        if (symbol == null) {
            symbol = new Symbol(s);
            map.put(s, symbol);
        }
        return symbol;
    }

    private Symbol(String value) {
        this.value = value;
    }

    @Override
    public LispObject eval(Environment env) {
        LispObject x = env.lookupValue(this);
        if (x == null) {
            return new Error("Undefined variable " + this);
        }
        return x;
    }

    @Override
    public String asString(Environment env) {
        return eval(env).asString(env);
    }

    @Override
    public String toString() {
        return value;
    }

    @Override
    public LispObject clone() {
        return this;
    }
}
