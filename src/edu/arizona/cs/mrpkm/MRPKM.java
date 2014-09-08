package edu.arizona.cs.mrpkm;

import edu.arizona.cs.mrpkm.kmeridx.KmerIndexBuilder;
import edu.arizona.cs.mrpkm.kmermatch.PairwiseKmerModeCounter;
import edu.arizona.cs.mrpkm.readididx.ReadIDIndexBuilder;
import edu.arizona.cs.mrpkm.utils.ClassHelper;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;
import org.kohsuke.args4j.Option;

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
    
    private static class MRPKM_Cmd_Args {
        @Option(name = "-h", aliases = "--help", usage = "print this message") 
        private boolean help = false;
        
        @Argument(metaVar = "[target-class [arguments ...]]", usage = "target-class and arguments")
        private List<String> arguments = new ArrayList<String>();
        
        public boolean isHelp() {
            return this.help;
        }
        
        public String getTargetClass() {
            if(this.arguments.isEmpty()) {
                return null;
            }
            
            return this.arguments.get(0);
        }
        
        public String[] getTargetClassArgs() {
            if(this.arguments.isEmpty()) {
                return new String[0];
            }
            
            String[] newArgs = new String[this.arguments.size()-1];
            for(int i=1;i<this.arguments.size();i++) {
                newArgs[i-1] = this.arguments.get(i);
            }
            
            return newArgs;
        }
        
        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            for(String arg : this.arguments) {
                if(sb.length() != 0) {
                    sb.append(", ");
                }
                
                sb.append(arg);
            }
            
            return "help = " + this.help + "\n" +
                    "arguments = " + sb.toString();
        }
    }

    private static void invokeClass(Class clazz, String[] args) throws Exception {
        if(clazz == null) {
            throw new IllegalArgumentException("clazz is not given");
        }
        // invoke main
        ClassHelper.invokeMain(clazz, args);
    }
    
    public static void main(String[] args) throws Exception {
        MRPKM_Cmd_Args cmdargs = new MRPKM_Cmd_Args();
        CmdLineParser parser = new CmdLineParser(cmdargs);
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            // handling of wrong arguments
            System.err.println(e.getMessage());
            parser.printUsage(System.err);
        }
        
        if(cmdargs.isHelp()) {
            printHelp();
        } else {
            String potentialClassName = cmdargs.getTargetClass();
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
                    String[] classArg = cmdargs.getTargetClassArgs();
                    // call a main function in the class
                    invokeClass(clazz, classArg);
                } else {
                    System.err.println("Class name is not given properly");
                }
            } else {
                System.err.println("Class name is not given");
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
