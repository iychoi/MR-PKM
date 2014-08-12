package edu.arizona.cs.mrpkm.commandline;

/**
 *
 * @author iychoi
 */
public class ArgumentParseException extends Exception {
    static final long serialVersionUID = 7818375828146090155L;

    public ArgumentParseException() {
        super();
    }

    public ArgumentParseException(String string) {
        super(string);
    }

    public ArgumentParseException(String string, Throwable thrwbl) {
        super(string, thrwbl);
    }

    public ArgumentParseException(Throwable thrwbl) {
        super(thrwbl);
    }
}
