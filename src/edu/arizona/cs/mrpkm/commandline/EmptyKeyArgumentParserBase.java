package edu.arizona.cs.mrpkm.commandline;

/**
 *
 * @author iychoi
 */
public abstract class EmptyKeyArgumentParserBase extends ArgumentParserBase {

    @Override
    public String getKey() {
        return null;
    }

    @Override
    public boolean hasDefault() {
        return false;
    }
}
