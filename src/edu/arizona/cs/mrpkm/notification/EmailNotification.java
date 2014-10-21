package edu.arizona.cs.mrpkm.notification;

import edu.arizona.cs.mrpkm.utils.RunningTimeHelper;
import java.io.IOException;
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
    private long beginTime;
    private long finishTime;
    
    public EmailNotification(String email, String password) {
        this.email = email;
        this.mail = new Gmail(email, password);
    }

    public void setJob(Job job) {
        this.job = job;
    }
    
    public void setJobBeginTime(long beginTime) {
        this.beginTime = beginTime;
    }
    
    public void setJobFinishTime(long finishTime) {
        this.finishTime = finishTime;
    }

    public void send() throws EmailNotificationException {
        String subject = makeSubject(this.job);
        String text = makeText(this.job, this.beginTime, this.finishTime);
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
    
    private String makeText(Job job, long beginTime, long finishTime) {
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
        
        String finishTimeStr = RunningTimeHelper.getTimeString(finishTime);
        
        return "Job : " + jobName + "\n" +
                "JobID : " + jobID + "\n" + 
                "Status : " + jobStatus + "\n" +
                "FinishTime : " + finishTimeStr + "\n" + 
                "TimeTaken : " + RunningTimeHelper.getDiffTimeString(beginTime, finishTime);
    }
}
