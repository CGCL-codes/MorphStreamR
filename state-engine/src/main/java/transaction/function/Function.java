package transaction.function;

import java.io.Serializable;

/**
 * Push down function
 */
public abstract class Function implements Serializable {
    public int delta_int;
    public long delta_long;
    public double delta_double;
    public double[] new_value;
}
