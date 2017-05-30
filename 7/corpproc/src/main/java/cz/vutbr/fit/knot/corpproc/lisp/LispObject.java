package cz.vutbr.fit.knot.corpproc.lisp;

import java.io.Serializable;

public abstract class LispObject implements Cloneable, Serializable {

    public LispObject first() {
        return new Error("type error: taking car of a " + this.getClass());
    }

    public LispObject rest() {
        return new Error("type error: taking car of a " + this.getClass());
    }

    public abstract LispObject eval(Environment env);
    
    public abstract String asString(Environment env);
    
    @Override
    public abstract LispObject clone();
}
