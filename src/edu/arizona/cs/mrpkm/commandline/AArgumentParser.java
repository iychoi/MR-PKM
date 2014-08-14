package edu.arizona.cs.mrpkm.commandline;

/**
 *
 * @author iychoi
 */
public abstract class AArgumentParser {
    public abstract String getKey();
    public abstract Class getValueType();

    public abstract void parse(String[] args) throws ArgumentParseException;
    public abstract boolean hasDefault();
    public abstract boolean isMandatory();
    public abstract int getValueLength();
    public abstract Object getValue();
    public abstract String getHelpMessage();
}
