package edu.arizona.cs.mrpkm.notification;

import edu.arizona.cs.mrpkm.helpers.RunningTimeHelper;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import javax.mail.MessagingException;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 * @author iychoi
 */
public class EmailNotification {

    private String email;
    private Gmail mail;
    private List<Job> jobs;
    
    public EmailNotification(String email, String password) {
        this.email = email;
        this.mail = new Gmail(email, password);
        this.jobs = new ArrayList<Job>();
    }

    public void addJob(Job job) {
        this.jobs.add(job);
    }
    
    public void addJob(Job[] jobs) {
        for(Job job : jobs) {
            this.jobs.add(job);
        }
    }
    
    public void addJob(List<Job> jobs) {
        this.jobs.addAll(jobs);
    }
    
    public void send() throws EmailNotificationException {
        Job lastJob = this.jobs.get(this.jobs.size() - 1);
        
        String subject = makeSubject(lastJob);
        StringBuilder text = new StringBuilder();
        for(Job job : this.jobs) {
            if(text.length() != 0) {
                text.append("\n\n");
            }
            text.append(makeText(job));
        }
        
        try {
            this.mail.send(this.email, subject, text.toString());
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
        
        return jobName + " Finished - " + jobStatus;
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
        
        String finishTimeStr;
        try {
            finishTimeStr = RunningTimeHelper.getTimeString(job.getFinishTime());
        } catch (Exception ex) {
            finishTimeStr = "Unknown";
        }
        
        String timeTakenStr;
        try {
            timeTakenStr = RunningTimeHelper.getDiffTimeString(job.getStartTime(), job.getFinishTime());
        } catch (Exception ex) {
            timeTakenStr = "Unknown";
        }
        
        return "Job : " + jobName + "\n" +
                "JobID : " + jobID + "\n" + 
                "Status : " + jobStatus + "\n" +
                "FinishTime : " + finishTimeStr + "\n" + 
                "TimeTaken : " + timeTakenStr + "\n";
    }
}
