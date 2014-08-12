package edu.arizona.cs.mrpkm;

import edu.arizona.cs.mrpkm.kmeridx.KmerIndexBuilder;
import edu.arizona.cs.mrpkm.kmermatch.PairwiseKmerModeCounter;
import edu.arizona.cs.mrpkm.readididx.ReadIDIndexBuilder;
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
        "edu.arizona.cs.mrpkm.readididx",
        "edu.arizona.cs.mrpkm.kmermatch"
    };

    private static void invokeClass(Class clazz, String[] args) throws Exception {
        if(clazz == null) {
            throw new IllegalArgumentException("clazz is not given");
        }
        // invoke main
        ClassHelper.invokeMain(clazz, args);
    }
    
    public static void main(String[] args) throws Exception {
        parseCommandLineArguments(args);
    }

    private static void parseCommandLineArguments(String[] args) throws Exception {
        if(args.length == 0 || args[0].equalsIgnoreCase("h") || args[0].equalsIgnoreCase("help") 
                || args[0].equalsIgnoreCase("-h") 
                || args[0].equalsIgnoreCase("--h") || args[0].equalsIgnoreCase("--help")) {
            printHelp();
        } else {
            String potentialClassName = args[0];
            Class clazz = null;
            try {
                clazz = ClassHelper.findClass(potentialClassName, SEARCH_PACKAGES);
            } catch (ClassNotFoundException ex) {
            }

            if(clazz == null) {
                if(args[0].equalsIgnoreCase("ridx")) {
                    clazz = ReadIDIndexBuilder.class;
                } else if(args[0].equalsIgnoreCase("kidx")) {
                    clazz = KmerIndexBuilder.class;
                } else if(args[0].equalsIgnoreCase("pkm")) {
                    clazz = PairwiseKmerModeCounter.class;
                }
            }

            if(clazz != null) {
                String[] newArgs = new String[args.length-1];
                System.arraycopy(args, 1, newArgs, 0, args.length-1);
                // call a main function in the class
                invokeClass(clazz, newArgs);
            } else {
                throw new Exception("Class name is not given properly");
            }
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
