package edu.arizona.cs.mrpkm.cmdparams;

/**
 *
 * @author iychoi
 */
public abstract class APKMCmdParamsParser<T> {
    public abstract T parse(String[] args);
}
