package ed.inf.adbs.lightdb;

import java.util.ArrayList;
public abstract class Operator {
    abstract Tuple getNextTuple();
    abstract void reset();
    abstract ArrayList<Tuple> dump();
}
