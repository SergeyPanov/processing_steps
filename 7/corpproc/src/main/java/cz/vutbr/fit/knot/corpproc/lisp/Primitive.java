package cz.vutbr.fit.knot.corpproc.lisp;

import static cz.vutbr.fit.knot.corpproc.lisp.Primitives.NIL;
import java.util.function.BiFunction;

public final class Primitive extends Function {

    private BiFunction<Environment, Cons, LispObject> value;
    private int minArgs;
    private int maxArgs;

    public Primitive(BiFunction<Environment, Cons, LispObject> value, int minArgs) {
        this.value = value;
        this.minArgs = minArgs;
        this.maxArgs = minArgs;
    }

    public Primitive(BiFunction<Environment, Cons, LispObject> value, int minArgs, int maxArgs) {
        this.value = value;
        this.minArgs = minArgs;
        this.maxArgs = maxArgs;
    }

    @Override
    public LispObject run(Environment env, LispObject x) {
        if (x == NIL) {
            if (minArgs > 0) {
                return new Error("Wrong argument count 0, expected " + minArgs + " to " + maxArgs);
            } else {
                return value.apply(env, new Cons(NIL));
            }
        }
        if (!(x instanceof Cons)) {
            return new Error("Wrong type of argument: " + x);
        }
        Cons args = (Cons) x;
        
        int size = 0;
        LispObject tmp = args;
        while (tmp instanceof Cons) {
            size++;
            tmp = ((Cons) tmp).rest();
        }

        if (size < minArgs || (size > maxArgs && maxArgs != 0)) {
            return new Error("Wrong argument count " + size + ", expected " + minArgs + " to " + maxArgs);
        }

        return value.apply(env, (Cons) args);
    }
    
    @Override
    public String asString(Environment env) {
        return "<primitive>";
    }

    @Override
    public LispObject clone() {
        return new Primitive(value, minArgs, maxArgs);
    }
}
