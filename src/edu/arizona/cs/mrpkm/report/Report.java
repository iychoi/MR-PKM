/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package edu.arizona.cs.mrpkm.report;

import edu.arizona.cs.mrpkm.helpers.RunningTimeHelper;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.mapreduce.Job;

/**
 *
 * @author iychoi
 */
public class Report {
    private List<Job> jobs;
    
    public Report() {
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
    
    public void writeTo(String filename) throws IOException {
        writeTo(new File("./", filename));
    }
    
    public void writeTo(File f) throws IOException {
        if(f.getParentFile() != null) {
            if(!f.getParentFile().exists()) {
                f.getParentFile().mkdirs();
            }
        }
        
        Writer writer = new FileWriter(f);
        boolean first = true;
        for(Job job : this.jobs) {
            if(first) {
                first = false;
            }
            writer.write(makeText(job));
            writer.write("\n\n");
        }
        
        writer.close();
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
        
        String startTimeStr;
        try {
            startTimeStr = RunningTimeHelper.getTimeString(job.getStartTime());
        } catch (Exception ex) {
            startTimeStr = "Unknown";
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
        
        String countersStr;
        try {
            countersStr = job.getCounters().toString();
        } catch (Exception ex) {
            countersStr = "Unknown";
        }
        
        return "Job : " + jobName + "\n" +
                "JobID : " + jobID + "\n" + 
                "Status : " + jobStatus + "\n" +
                "StartTime : " + startTimeStr + "\n" +
                "FinishTime : " + finishTimeStr + "\n" + 
                "TimeTaken : " + timeTakenStr + "\n\n" +
                countersStr;
    }
}
