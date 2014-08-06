package edu.arizona.cs.mrpkm;

/**
 *
 * @author iychoi
 */
public class ClassExecutionConfiguration {
    
    private Class clazz;
    private String[] args;
    
    public ClassExecutionConfiguration(Class clazz, String[] args) {
        this.clazz = clazz;
        this.args = args;
    }
    
    public Class getExecutionClass() {
        return this.clazz;
    }
    
    public String[] getExecutionArgs() {
        return this.args;
    }
}
