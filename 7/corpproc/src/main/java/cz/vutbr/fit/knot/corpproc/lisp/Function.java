package cz.vutbr.fit.knot.corpproc.lisp;

public abstract class Function extends LispObject {

    @Override
    public LispObject eval(Environment env) {
        return new Error("Invalid function " + getClass());
    }

    abstract public LispObject run(Environment env, LispObject obj);
}
