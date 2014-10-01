package edu.arizona.cs.mrpkm.notification;

import javax.mail.MessagingException;

/**
 *
 * @author iychoi
 */
public class Gmail {
    
    private String userid;
    
    private static final String GMAIL_SMTP_HOST = "smtp.gmail.com";
    private static final int GMAIL_SMTP_PORT = 465;
    
    private SendMailSSL smtp;
    
    public Gmail(String id, String password) {
        this.userid = id;
        this.smtp = new SendMailSSL(GMAIL_SMTP_HOST, GMAIL_SMTP_PORT, this.userid, password);
    }
    
    public void send(String to, String subject, String text) throws MessagingException {
        //SendMailTLS sendmail = new SendMailTLS(smtpHost, smtpPort, id, password);
        this.smtp.send(this.userid, to, subject, text);
    }
}
