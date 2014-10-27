package edu.arizona.cs.mrpkm.cmdparams;

/**
 *
 * @author iychoi
 */
public abstract class PKMCmdParamsParser<T> {
    public abstract T parse(String[] args);
}
