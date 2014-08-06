package edu.arizona.cs.mrpkm;

import edu.arizona.cs.mrpkm.utils.ClassHelper;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 *
 * @author iychoi
 */
public class MRPKM {

    private static final Log LOG = LogFactory.getLog(MRPKM.class);
    
    private static final String[] SEARCH_PACKAGES = {
        "edu.arizona.cs.mrpkm.kmeridx",
        "edu.arizona.cs.mrpkm.readididx"
    };

    private static void invokeClass(Class clazz, String[] args) throws Exception {
        if(clazz == null) {
            throw new IllegalArgumentException("clazz is not given");
        }
        // invoke main
        ClassHelper.invokeMain(clazz, args);
    }
    
    public static void main(String[] args) throws Exception {
        ExecutionConfiguration ec = parseCommandLineArguments(args);
        if(ec.isClassExecution()) {
            // call a main function in the class
            ClassExecutionConfiguration classExecution = ec.getClassExecution();
            invokeClass(classExecution.getExecutionClass(), classExecution.getExecutionArgs());
        } else if(ec.isParamExecution()) {
            // run with command line options without class
            ParamExecutionConfiguration paramExecution = ec.getParamExecution();
            runWithParam(paramExecution);
        } else if(ec.isHelp()) {
            printHelp();
        } else {
            // unknown
            printHelp();   
        }
    }

    private static ExecutionConfiguration parseCommandLineArguments(String[] args) throws Exception {
        String potentialClassName = args[0];
        Class clazz = null;
        try {
            clazz = ClassHelper.findClass(potentialClassName, SEARCH_PACKAGES);
        } catch (ClassNotFoundException ex) {
        }
        
        if(clazz != null) {
            String[] newArgs = new String[args.length-1];
            System.arraycopy(args, 1, newArgs, 0, args.length-1);
            ClassExecutionConfiguration cec = new ClassExecutionConfiguration(clazz, newArgs);
            
            return new ExecutionConfiguration(cec);
        } else {
            //TODO : do parse
            // ParamExecutionConfiguration
        }
        
        // help?
        return new ExecutionConfiguration();
    }

    private static void runWithParam(ParamExecutionConfiguration paramExecution) throws Exception {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    private static void printHelp() {
        System.out.println("============================================");
        System.out.println("MR-PKM : Pairwise K-mer Mode pipeline");
        System.out.println("============================================");
        System.out.println("Usage :");
        System.out.println("> MR-PKM <class-name> <arguments ...>");
        System.out.println("> MR-PKM <command line parameters ...>");
    }
}
