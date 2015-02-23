package edu.arizona.cs.mrpkm.cmdparams;

import edu.arizona.cs.mrpkm.cluster.AMRClusterConfiguration;
import edu.arizona.cs.mrpkm.cluster.MRClusterConfiguration_default;
import org.kohsuke.args4j.Option;

/**
 *
 * @author iychoi
 */
public class PKMCmdParamsBase {
    
    protected static final int DEFAULT_KMERSIZE = 20;
    
    @Option(name = "-h", aliases = "--help", usage = "print this message") 
    protected boolean help = false;

    protected AMRClusterConfiguration cluster = new MRClusterConfiguration_default();

    @Option(name = "-c", aliases = "--cluster", usage = "specify cluster configuration")
    public void setCluster(String clusterConf) throws ClassNotFoundException, InstantiationException, IllegalAccessException {
        this.cluster = AMRClusterConfiguration.findConfiguration(clusterConf);
    }
    
    @Option(name = "-k", aliases = "--kmersize", usage = "specify kmer size")
    protected int kmersize = DEFAULT_KMERSIZE;

    @Option(name = "-n", aliases = "--nodenum", usage = "specify the number of hadoop slaves")
    protected int nodes = 1;
    
    @Option(name = "--report", usage = "specify report to file")
    protected String reportfile;
    
    @Option(name = "--notifyemail", usage = "specify email address for job notification")
    protected String notificationEmail;

    @Option(name = "--notifypassword", usage = "specify email password for job notification")
    protected String notificationPassword;
    
    public boolean isHelp() {
        return this.help;
    }

    public AMRClusterConfiguration getClusterConfig() {
        return this.cluster;
    }

    public int getKmerSize() {
        return this.kmersize;
    }

    public int getNodes() {
        return this.nodes;
    }
    
    public boolean needReport() {
        return (reportfile != null);
    }
    
    public String getReportFilename() {
        return reportfile;
    }
    
    public boolean needNotification() {
        return (notificationEmail != null);
    }
    
    public String getNotificationEmail() {
        return notificationEmail;
    }

    public String getNotificationPassword() {
        return notificationPassword;
    }
    
    @Override
    public String toString() {
        return super.toString();
    }
    
    public boolean checkValidity() {
        if(this.cluster == null || 
                this.kmersize <= 0 ||
                this.nodes <= 0) {
            return false;
        }
        return true;
    }
}
