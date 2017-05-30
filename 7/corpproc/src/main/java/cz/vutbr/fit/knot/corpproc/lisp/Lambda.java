package cz.vutbr.fit.knot.corpproc.lisp;

import static cz.vutbr.fit.knot.corpproc.lisp.Primitives.NIL;
import static cz.vutbr.fit.knot.corpproc.lisp.Symbol.makeSymbol;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Lambda extends Function {

    private static Symbol OPTIONAL = makeSymbol("&OPTIONAL");
    private static Symbol REST = makeSymbol("&REST");

    private List<Symbol> argNames;
    private int minArgs = -1;
    private int maxArgs = -1;
    private Environment parentEnv;
    private Cons body;

    public static LispObject makeLambda(Environment parent, LispObject argList, Cons body) {
        Lambda l = new Lambda();
        l.parentEnv = parent;

        l.argNames = new ArrayList();
        if (!(argList instanceof Cons) && argList != NIL) {
            return new Error("Invalid function definition, arglist must be a Cons.");
        }
        LispObject arg = argList;
        int i = 0;
        while (arg instanceof Cons) {
            if (arg == NIL) {
                break;
            }
            if (!(arg.first() instanceof Symbol)) {
                return new Error("Invalid function definition, arg names must be symbols.");
            }
            l.argNames.add((Symbol) arg.first());
            if (arg.first() == OPTIONAL) {
                l.minArgs = i;
            } else if (arg.first() == REST) {
                l.minArgs = l.minArgs > -1 ? l.minArgs : i;
                l.maxArgs = 0;
            }
            arg = arg.rest();
            i++;
        }
        l.minArgs = l.minArgs > -1 ? l.minArgs : i;
        l.maxArgs = l.maxArgs > -1 ? l.maxArgs : i;

        l.body = body;

        return l;
    }

    private Lambda() {
    }

    @Override
    public LispObject run(Environment env, LispObject x) {
        if (x == NIL) {
            if (minArgs > 0) {
                return new Error("Wrong argument count 0, expected " + minArgs + " to " + maxArgs);
            } else {
                return body.eval(parentEnv);
            }
        }
        if (!(x instanceof Cons)) {
            return new Error("Wrong type of argument: " + x);
        }
        List<LispObject> args = ((Cons) x).toList();

        if (args.size() < minArgs || (args.size() > maxArgs && maxArgs != 0)) {
            return new Error("Wrong argument count " + args.size()
                    + ", expected " + minArgs + " to " + maxArgs);
        }

        Environment nestedEnv = new Environment(parentEnv);
        boolean optional = false;
        boolean rest = false;
        Symbol restSymbol = null;
        List<LispObject> restArgs = new ArrayList();
        int i = 0;
        for (Symbol s : argNames) {
            if (s == OPTIONAL) {
                optional = true;
                continue;
            }
            if (s == REST) {
                rest = true;
                continue;
            }

            if (rest) {
                restSymbol = s;
                while (i < args.size()) {
                    restArgs.add(args.get(i));
                    i++;
                }
                break;
            }
            if (optional && (!optional || args.size() < i)) {
                break;
            }
            nestedEnv.bindValue(s, args.get(i));
            i++;
            //TODO: Zip(argNames, range(0, argNames.size()) ???
        }
        if (restSymbol != null) {
            nestedEnv.bindValue(restSymbol, Cons.fromList(restArgs));
        }

        //Evaluate body (implicit progn)
        return body.clone().eval(nestedEnv);
    }

    @Override
    public String toString() {
        return "(LAMBDA " + Cons.fromList(argNames.stream().map(x -> x).collect(Collectors.toList())) + " " + body + ")";
    }
    
    @Override
    public String asString(Environment env) {
        return "<lambda>";
    }

    @Override
    public LispObject clone() {
        Lambda l = new Lambda();
        l.argNames = argNames;
        l.body = body;
        l.maxArgs = maxArgs;
        l.minArgs = minArgs;
        l.parentEnv = parentEnv;
        return l;
    }
}
