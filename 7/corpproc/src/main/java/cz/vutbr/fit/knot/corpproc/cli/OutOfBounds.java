package cz.vutbr.fit.knot.corpproc.cli;

/**
 * Created by Sergey on 11/1/2016.
 */
public class OutOfBounds extends Exception {
    public OutOfBounds(){}
    public OutOfBounds(String message){
        super(message);
    }
}
