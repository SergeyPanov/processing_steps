package cz.vutbr.fit.knot.corpproc.lisp;

import static cz.vutbr.fit.knot.corpproc.lisp.Lambda.makeLambda;
import static cz.vutbr.fit.knot.corpproc.lisp.Symbol.makeSymbol;
import java.util.regex.Pattern;

public abstract class Primitives {

    public static void register(Environment e) {
        e.bindFunction(makeSymbol("ATOM"), ATOM);
        e.bindFunction(makeSymbol("EQ"), EQ);
        e.bindFunction(makeSymbol("CAR"), CAR);
        e.bindFunction(makeSymbol("CDR"), CDR);
        e.bindFunction(makeSymbol("CONS"), CONS);
        e.bindFunction(makeSymbol("QUOTE"), QUOTE);
        e.bindFunction(makeSymbol("EVAL"), EVAL);
        e.bindFunction(makeSymbol("IF"), IF);
        e.bindFunction(makeSymbol("LAMBDA"), LAMBDA);
        e.bindFunction(makeSymbol("DEFUN"), DEFUN);
        e.bindFunction(makeSymbol("SET"), SET);
        e.bindFunction(makeSymbol("SETQ"), SETQ);
        e.bindFunction(makeSymbol("PROGN"), PROGN);
        e.bindFunction(makeSymbol("CONCATENATE"), CONCATENATE);
        e.bindFunction(makeSymbol("SUBST"), SUBST);
        e.bindFunction(makeSymbol("REPLACE"), REPLACE);
        e.bindFunction(makeSymbol("AND"), AND);
        e.bindFunction(makeSymbol("OR"), OR);
        e.bindValue(makeSymbol("T"), T);
        e.bindValue(makeSymbol("NIL"), NIL);

        Lisp.parse(
                "  (defun first (x) (car x))"
                + "(defun last (x) (if (cdr x) (last (cdr x)) (car x)))"
        ).forEach(x -> x.eval(e));
    }

    public static final LispObject T = new TObj();

    public static final class TObj extends LispObject {

        private TObj() {
        }

        @Override
        public LispObject eval(Environment env) {
            return this;
        }

        @Override
        public String toString() {
            return "T";
        }

        @Override
        public LispObject clone() {
            return T;
        }

        @Override
        public String asString(Environment env) {
            return "";
        }
    };

    public static final LispObject NIL = new Nil();

    public static final class Nil extends LispObject {

        private Nil() {
        }

        @Override
        public LispObject eval(Environment env) {
            return this;
        }

        @Override
        public LispObject first() {
            return NIL;
        }

        @Override
        public LispObject rest() {
            return NIL;
        }

        @Override
        public String toString() {
            return "NIL";
        }

        @Override
        public String asString(Environment env) {
            return "";
        }

        @Override
        public LispObject clone() {
            return NIL;
        }
    };

    public static final Primitive ATOM = new Primitive((env, args) -> {
        return !(args.first().eval(env) instanceof Cons) ? T : NIL;
    }, 1);

    public static final Primitive EQ = new Primitive((env, args) -> {
        return args.first().eval(env).equals(
                args.rest().first().eval(env)) ? T : NIL;
    }, 2);

    public static final Primitive CAR
            = new Primitive((env, args) -> args.first().eval(env).first(), 1);

    public static final Primitive CDR
            = new Primitive((env, args) -> args.first().eval(env).rest(), 1);

    public static final Primitive CONS = new Primitive((env, args) -> {
        LispObject car = args.first().eval(env);
        if (car instanceof Error) {
            return car;
        }
        LispObject cdr = args.rest().first().eval(env);
        if (cdr instanceof Error) {
            return cdr;
        }
        return new Cons(car, cdr);
    }, 2);

    public static final Primitive QUOTE
            = new Primitive((env, args) -> args.first(), 1);

    public static final Primitive EVAL = new Primitive((env, x) -> {
        return x.first().eval(env);
    }, 1);

    public static final Primitive IF = new Primitive((env, args) -> {
        LispObject cond = args.first().eval(env);
        if (cond instanceof Error) {
            return cond;
        } else if (cond != NIL) {
            return args.rest().first().eval(env);
        } else {
            return args.rest().rest().first().eval(env);
        }
    }, 2, 3);

    public static final Primitive LAMBDA = new Primitive((env, arg) -> {
        return makeLambda(env, arg.first(), new Cons(makeSymbol("PROGN"), arg.rest()));
    }, 2, 0);

    public static final Primitive DEFUN = new Primitive((env, arg) -> {
        if (!(arg.first() instanceof Symbol)) {
            return new Error("Define requires a symbol as its argument.");
        }
        LispObject f = makeLambda(env, arg.rest().first(), new Cons(makeSymbol("PROGN"), arg.rest().rest()));
        if (!(f instanceof Function)) {
            return f;
        }
        env.bindFunction((Symbol) arg.first(), (Function) f);
        return f;
    }, 3, 0);

    public static final Primitive SET = new Primitive((env, arg) -> {
        LispObject name = arg.first().eval(env);
        if (!(name instanceof Symbol)) {
            return new Error("Define requires a symbol as its argument.");
        }
        LispObject value = arg.rest().first().eval(env);
        if (value instanceof Error) {
            return value;
        }
        env.bindValue((Symbol) name, value);
        return value;
    }, 2);

    public static final Primitive SETQ = new Primitive((env, arg) -> {
        if (!(arg.first() instanceof Symbol)) {
            return new Error("Define requires a symbol as its argument.");
        }
        LispObject value = arg.rest().first().eval(env);
        if (value instanceof Error) {
            return value;
        }
        env.bindValue((Symbol) arg.first(), value);
        return value;
    }, 2);

    public static final Primitive PROGN = new Primitive((env, arg) -> {
        LispObject result = arg.first().eval(env);
        LispObject rest = arg.rest();
        while (rest instanceof Cons) {
            result = rest.first().eval(env);
            rest = rest.rest();
        }
        return result;
    }, 1, 0);

    public static final Primitive CONCATENATE = new Primitive((env, arg) -> {
        String value = "";

        LispObject rest = arg;
        while (rest instanceof Cons) {
            LispObject val = rest.first();
            if (!(val instanceof LispString || val instanceof LispChar)) {
                val = val.eval(env);
            }
            if (val instanceof LispString) {
                value += ((LispString) val).getValue();
            } else if (rest.first() instanceof LispChar) {
                value += ((LispChar) val).getValue();
            } else {
                return new Error("Invalid parameter type: " + rest.first().getClass());
            }
            rest = rest.rest();
        }

        return new LispString(value);
    }, 1, 0);

    public static final Primitive SUBST = new Primitive((env, arg) -> {
        LispObject first = arg.first().eval(env);
        LispObject second = arg.rest().first().eval(env);
        LispObject third = arg.rest().rest().first().eval(env);
        if (third == NIL) {
            return NIL;
        }
        if (!(first instanceof LispChar && second instanceof LispChar && third instanceof LispString)) {
            return new Error("Invalid parameter type");
        }
        return new LispString(((LispString) third).getValue().replace(
                ((LispChar) first).getValue(), ((LispChar) second).getValue()));
    }, 3);

    public static final Primitive REPLACE = new Primitive((env, arg) -> {
        LispObject first = arg.first().eval(env);
        LispObject second = arg.rest().first().eval(env);
        LispObject third = arg.rest().rest().first().eval(env);
        if (third == NIL) {
            return NIL;
        }
        if (!(first instanceof LispString && second instanceof LispString && third instanceof LispString)) {
            return new Error("Invalid parameter type");
        }
        return new LispString(((LispString) third).getValue().replaceAll(
                Pattern.quote(((LispString) first).getValue()),
                ((LispString) second).getValue()
        ));
    }, 3);
    
    public static final Primitive AND = new Primitive((env, arg) -> {
        LispObject rest = arg;
        LispObject cond = NIL;
        while (rest instanceof Cons) {
            cond = rest.first().eval(env);
            if (cond instanceof Error || cond == NIL) {
                return NIL;
            }
            rest = rest.rest();
        }
        return cond;
    }, 1, 0);
    
    public static final Primitive OR = new Primitive((env, arg) -> {
        LispObject rest = arg;
        while (rest instanceof Cons) {
            LispObject cond = rest.first().eval(env);
            if (!(cond instanceof Error) && cond != NIL) {
                return cond;
            }
            rest = rest.rest();
        }
        return NIL;
    }, 1, 0);
}
