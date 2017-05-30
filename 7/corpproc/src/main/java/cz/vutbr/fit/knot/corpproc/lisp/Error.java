package cz.vutbr.fit.knot.corpproc.lisp;

public final class Error extends LispObject {

    private String type = "unknown";

    public Error() {
    }

    public Error(String s) {
        type = s;
    }

    @Override
    public LispObject eval(Environment e) {
        return this;
    }

    @Override
    public String toString() {
        return type;
    }

    @Override
    public LispObject clone() {
        return new Error(type);
    }

    @Override
    public String asString(Environment env) {
        return "<error: " + type + ">";
    }
}
