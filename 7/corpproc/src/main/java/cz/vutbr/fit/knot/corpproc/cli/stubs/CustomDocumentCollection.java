package cz.vutbr.fit.knot.corpproc.cli.stubs;

import it.unimi.di.big.mg4j.document.DocumentFactory;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.sux4j.util.EliasFanoMonotoneLongBigList;
import java.io.Serializable;

public class CustomDocumentCollection implements Serializable {

    private static final long serialVersionUID = 1L;
    
    public String[] file;
    public boolean gzipped;
    public DocumentFactory factory;
    public ObjectArrayList<EliasFanoMonotoneLongBigList> pointers;
    public int size;
    public boolean phrase;
    public long[] firstDocument;
}
