package edu.arizona.cs.mrpkm.commandline;

/**
 *
 * @author iychoi
 */
public abstract class AEmptyKeyArgumentParser extends AArgumentParser {

    @Override
    public String getKey() {
        return null;
    }

    @Override
    public boolean hasDefault() {
        return false;
    }
}
