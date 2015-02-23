package edu.arizona.cs.mrpkm.report.notification;

/**
 *
 * @author iychoi
 */
public class EmailNotificationException extends Exception {
    static final long serialVersionUID = 7818375828146090157L;

    public EmailNotificationException() {
        super();
    }

    public EmailNotificationException(String string) {
        super(string);
    }

    public EmailNotificationException(String string, Throwable thrwbl) {
        super(string, thrwbl);
    }

    public EmailNotificationException(Throwable thrwbl) {
        super(thrwbl);
    }
}
