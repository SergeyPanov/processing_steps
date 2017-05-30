package cz.vutbr.fit.knot.corpproc.lisp;

import java.util.Objects;

public final class LispChar extends LispObject {

    private char value;

    public LispChar(char value) {
        this.value = value;
    }

    @Override
    public LispObject eval(Environment e) {
        return this;
    }

    @Override
    public String toString() {
        return "#\\" + value;
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof LispChar && value == ((LispChar) obj).getValue();
    }

    @Override
    public int hashCode() {
        return 68 * 7 + Objects.hashCode(this.value);
    }

    @Override
    public LispObject clone() {
        return new LispChar(value);
    }
    
    public char getValue() {
        return value;
    }

    @Override
    public String asString(Environment env) {
        return Character.toString(value);
    }
}
