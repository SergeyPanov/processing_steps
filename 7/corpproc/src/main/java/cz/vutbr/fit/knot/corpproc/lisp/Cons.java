package cz.vutbr.fit.knot.corpproc.lisp;

import static cz.vutbr.fit.knot.corpproc.lisp.Primitives.NIL;
import java.util.ArrayList;
import java.util.List;

public final class Cons extends LispObject {

    private LispObject first;
    private LispObject rest;

    public static LispObject fromList(List<LispObject> list) {
        if (list.isEmpty()) {
            return NIL;
        } else if (list.size() == 1) {
            return new Cons(list.get(0), NIL);
        } else {
            return new Cons(list.get(0), fromList(list.subList(1, list.size())));
        }
    }

    public Cons(LispObject first) {
        this.first = first;
        this.rest = NIL;
    }

    public Cons(LispObject first, LispObject rest) {
        this.first = first;
        this.rest = rest;
    }

    @Override
    public LispObject eval(Environment env) {
        if (first() instanceof Error) {
            return first();
        }
        if (first() instanceof Cons) {
            setFirst(first().eval(env));
            return eval(env);
        }
        Function f;
        if (first() instanceof Symbol) {
            f = env.lookupFunction((Symbol) first());
            if (f == null) {
                return new Error("Undefined function " + first());
            }
        } else if (first() instanceof Function) {
            f = (Function) first();
        } else {
            return new Error("Invalid function " + first());
        }

        LispObject args = rest();
        if (!(f instanceof Primitive)) {
            LispObject tmp = args;
            while (tmp instanceof Cons) {
                LispObject arg = tmp.first().eval(env);
                if (arg instanceof Error) {
                    return arg;
                }
                ((Cons) tmp).setFirst(arg);
                tmp = tmp.rest();
            }
        }
        return f.run(env, args);
    }
    
    @Override
    public String asString(Environment env) {
        return toString();
    }

    @Override
    public String toString() {
        if (rest == NIL) {
            return '(' + first.toString() + ')';
        }
        if (!(rest instanceof Cons)) {
            return '(' + first.toString() + " . " + rest.toString() + ')';
        }

        LispObject next = rest;
        String s = '(' + first.toString();
        while (next instanceof Cons) {
            Cons other = (Cons) next;
            s += ' ' + other.first().toString();
            next = other.rest();
        }
        if (next == NIL) {
            s += ')';
        } else {
            s += " . " + next.toString() + ')';
        }
        return s;
    }

    @Override
    public LispObject first() {
        return first;
    }

    @Override
    public LispObject rest() {
        return rest;
    }

    public void setFirst(LispObject first) {
        this.first = first;
    }

    public void setRest(LispObject rest) {
        this.rest = rest;
    }

    public List<LispObject> toList() {
        List<LispObject> list = new ArrayList<>();
        LispObject tmp = this;
        while (tmp instanceof Cons) {
            list.add(tmp.first());
            tmp = tmp.rest();
        }
        return list;
    }

    @Override
    public LispObject clone() {
        return new Cons(first.clone(), rest.clone());
    }
}
