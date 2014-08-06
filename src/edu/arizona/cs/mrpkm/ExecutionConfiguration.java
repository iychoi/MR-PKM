package edu.arizona.cs.mrpkm;

/**
 *
 * @author iychoi
 */
public class ExecutionConfiguration {

    private ClassExecutionConfiguration classExecutionConfiguration;
    private ParamExecutionConfiguration paramExecutionConfiguration;
    private boolean help;
    
    public ExecutionConfiguration(ClassExecutionConfiguration cec) {
        this.classExecutionConfiguration = cec;
        
        this.paramExecutionConfiguration = null;
        this.help = false;
    }
    
    public ExecutionConfiguration(ParamExecutionConfiguration pec) {
        this.paramExecutionConfiguration = pec;
        
        this.classExecutionConfiguration = null;
        this.help = false;
    }
    
    public ExecutionConfiguration() {
        this.help = true;
        
        this.paramExecutionConfiguration = null;
        this.classExecutionConfiguration = null;
    }
    
    public boolean isClassExecution() {
        if(this.classExecutionConfiguration == null) {
            return false;
        }
        return true;
    }

    public ClassExecutionConfiguration getClassExecution() {
        return this.classExecutionConfiguration;
    }
    
    public boolean isParamExecution() {
        if(this.paramExecutionConfiguration == null) {
            return false;
        }
        return true;
    }

    public ParamExecutionConfiguration getParamExecution() {
        return this.paramExecutionConfiguration;
    }

    public boolean isHelp() {
        return this.help;
    }
}
