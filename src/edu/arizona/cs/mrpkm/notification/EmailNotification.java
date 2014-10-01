package edu.arizona.cs.mrpkm.notification;

import java.io.IOException;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.mail.MessagingException;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 * @author iychoi
 */
public class EmailNotification {

    private String email;
    private Gmail mail;
    private Job job;
    
    public EmailNotification(String email, String password) {
        this.email = email;
        this.mail = new Gmail(email, password);
    }

    public void setJob(Job job) {
        this.job = job;
    }

    public void send() throws EmailNotificationException {
        String subject = makeSubject(this.job);
        String text = makeText(this.job);
        try {
            this.mail.send(this.email, subject, text);
        } catch (MessagingException ex) {
            throw new EmailNotificationException(ex.getCause());
        }
    }

    private String makeSubject(Job job) {
        String jobName = job.getJobName();
        String jobStatus;
        try {
            jobStatus = job.getJobState().name();
        } catch (IOException ex) {
            jobStatus = "Unknown";
        } catch (InterruptedException ex) {
            jobStatus = "Unknown";
        }
        
        return job.getJobName() + "(MR-PKM) Finished - " + jobStatus;
    }
    
    private String makeText(Job job) {
        String jobName = job.getJobName();
        String jobID = job.getJobID().toString();
        String jobStatus;
        try {
            jobStatus = job.getJobState().name();
        } catch (IOException ex) {
            jobStatus = "Unknown";
        } catch (InterruptedException ex) {
            jobStatus = "Unknown";
        }
        
        long time = System.currentTimeMillis();
        String finishTime = convertTime(time);
        
        return "Job : " + jobName + "\n" +
                "JobID : " + jobID + "\n" + 
                "Status : " + jobStatus + "\n" +
                "FinishTime : " + finishTime + "\n";
    }
    
    private String convertTime(long time) {
        Date date = new Date(time);
        Format format = new SimpleDateFormat("yyyy MM dd HH:mm:ss");
        return format.format(date);
    }
}
