package cz.vutbr.fit.knot.corpproc.lisp;

import java.util.Objects;

public final class LispString extends LispObject {

    private String value;

    public LispString(String value) {
        this.value = value;
    }

    @Override
    public LispObject eval(Environment e) {
        return this;
    }

    @Override
    public String toString() {
        return '"' + value + '"';
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof LispString && value.equals(((LispString) obj).getValue());
    }

    @Override
    public int hashCode() {
        return 67 * 7 + Objects.hashCode(this.value);
    }

    @Override
    public LispObject clone() {
        return new LispString(value);
    }
    
    public String getValue() {
        return value;
    }

    @Override
    public String asString(Environment env) {
        return value;
    }
}
