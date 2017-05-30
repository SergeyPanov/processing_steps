package cz.vutbr.fit.knot.corpproc.cli;

import java.io.*;

/**
 * Class only makes deep copy af any object.
 */
public abstract class Clonner {
    public static<T> T clone(T origin){
        try {
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            ObjectOutputStream oos = new ObjectOutputStream(baos);
            oos.writeObject(origin);
            ByteArrayInputStream bais = new ByteArrayInputStream(baos.toByteArray());
            ObjectInputStream ois = new ObjectInputStream(bais);
            return (T) ois.readObject();
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }
}
