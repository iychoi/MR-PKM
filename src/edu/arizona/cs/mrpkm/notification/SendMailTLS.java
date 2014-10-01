package edu.arizona.cs.mrpkm.notification;

/* Original code is from
 * http://www.mkyong.com/java/javamail-api-sending-email-via-gmail-smtp-example/
 */

import java.util.Properties;
import javax.mail.Message;
import javax.mail.MessagingException;
import javax.mail.PasswordAuthentication;
import javax.mail.Session;
import javax.mail.Transport;
import javax.mail.internet.InternetAddress;
import javax.mail.internet.MimeMessage;

public class SendMailTLS {

    private String smtpHost;
    private int smtpPort;
    private String user;
    private String password;
    
    public SendMailTLS(String smtpHost, int smtpPort, String user, String password) {
        this.smtpHost = smtpHost;
        this.smtpPort = smtpPort;
        this.user = user;
        this.password = password;
    }
    
    public void send(String from, String to, String subject, String text) throws MessagingException {
        Properties props = new Properties();
        props.put("mail.smtp.host", this.smtpHost);
        props.put("mail.smtp.starttls.enable", "true");
        props.put("mail.smtp.auth", "true");
        props.put("mail.smtp.port", this.smtpPort);

        Session session = Session.getDefaultInstance(props,
                new javax.mail.Authenticator() {
                    protected PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(user, password);
                    }
                });

        Message message = new MimeMessage(session);
        message.setFrom(new InternetAddress(from));
        message.setRecipients(Message.RecipientType.TO,
                InternetAddress.parse(to));
        message.setSubject(subject);
        message.setText(text);

        Transport.send(message);
    }
}
