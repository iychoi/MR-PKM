package edu.arizona.cs.mrpkm;

import edu.arizona.cs.mrpkm.kmeridx.KmerIndexBuilder;
import edu.arizona.cs.mrpkm.kmermatch.PairwiseKmerModeCounter;
import edu.arizona.cs.mrpkm.readididx.ReadIDIndexBuilder;
import edu.arizona.cs.mrpkm.utils.ClassHelper;
import java.util.ArrayList;
import java.util.List;
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
        "edu.arizona.cs.mrpkm.readididx",
        "edu.arizona.cs.mrpkm.stddiviation",
        "edu.arizona.cs.mrpkm.kmermatch",
        "edu.arizona.cs.mrpkm.tools"
    };
    
    private static void invokeClass(Class clazz, String[] args) throws Exception {
        if(clazz == null) {
            throw new IllegalArgumentException("clazz is not given");
        }
        // invoke main
        ClassHelper.invokeMain(clazz, args);
    }
    
    private static String[] filteroutSubParams(String[] args) {
        List<String> filteredParams = new ArrayList<String>();
        
        for(String arg : args) {
            filteredParams.add(arg);
            break;
        }
        
        return filteredParams.toArray(new String[0]);
    }
    
    private static boolean isHelpParam(String[] args) {
        if(args.length < 1 || 
                args[0].equalsIgnoreCase("-h") ||
                args[0].equalsIgnoreCase("--help")) {
            return true;
        }
        return false;
    }
    
    private static String getTargetClassName(String[] args) {
        if(args.length < 1) {
            return null;
        }
        
        return args[0];
    }
    
    private static String[] getTargetClassArguments(String[] args) {
        List<String> targetClassArguments = new ArrayList<String>();
        if(args.length > 1) {
            for(int i=1; i<args.length; i++) {
                targetClassArguments.add(args[i]);
            }
        }
        
        return targetClassArguments.toArray(new String[0]);
    }
    
    public static void main(String[] args) throws Exception {
        if(isHelpParam(args)) {
            printHelp();
            return;
        } 
        
        String potentialClassName = getTargetClassName(args);
        if(potentialClassName != null) {
            Class clazz = null;
            try {
                clazz = ClassHelper.findClass(potentialClassName, SEARCH_PACKAGES);
            } catch (ClassNotFoundException ex) {
            }

            if(clazz == null) {
                if(potentialClassName.equalsIgnoreCase("ridx")) {
                    clazz = ReadIDIndexBuilder.class;
                } else if(potentialClassName.equalsIgnoreCase("kidx")) {
                    clazz = KmerIndexBuilder.class;
                } else if(potentialClassName.equalsIgnoreCase("pkm")) {
                    clazz = PairwiseKmerModeCounter.class;
                }
            }

            if(clazz != null) {
                String[] classArg = getTargetClassArguments(args);
                // call a main function in the class
                invokeClass(clazz, classArg);
            } else {
                System.err.println("Class name is not given properly : " + potentialClassName);
            }
        } else {
            printHelp();
        }
    }

    private static void printHelp() {
        System.out.println("============================================");
        System.out.println("MR-PKM : Pairwise K-mer Mode pipeline");
        System.out.println("============================================");
        System.out.println("Usage :");
        System.out.println("> MR-PKM <class-name|abbreviation> <arguments ...>");
        System.out.println("Abbreviations :");
        System.out.println("> ridx : ReadIDIndexBuilder");
        System.out.println("> kidx : KmerIndexBuilder");
        System.out.println("> pkm : PairwiseKmerModeCounter");
    }
}
