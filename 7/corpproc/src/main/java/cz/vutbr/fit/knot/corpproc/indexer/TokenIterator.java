package cz.vutbr.fit.knot.corpproc.indexer;

import it.unimi.dsi.fastutil.ints.IntIterator;
import it.unimi.dsi.lang.MutableString;
import java.io.Reader;

public interface TokenIterator extends IntIterator {

    /**
     * Returns the token to be indexed after a call to {@link #nextInt()}.
     *
     * <p>
     * The {@link MutableString} returned by this method can be modified by the
     * caller; moreover, the same instance might be returned at each invocation.
     *
     * @return the token to be indexed after a call to {@link #nextInt()}.
     */
    public MutableString token();

    public Reader getContent();
}
