package edu.arizona.cs.mrpkm.readididx;

/**
 *
 * @author iychoi
 */
public class ReadIDNotFoundException extends Exception {
    static final long serialVersionUID = 7818375828146090155L;

    public ReadIDNotFoundException() {
        super();
    }

    public ReadIDNotFoundException(String string) {
        super(string);
    }

    public ReadIDNotFoundException(String string, Throwable thrwbl) {
        super(string, thrwbl);
    }

    public ReadIDNotFoundException(Throwable thrwbl) {
        super(thrwbl);
    }
}
