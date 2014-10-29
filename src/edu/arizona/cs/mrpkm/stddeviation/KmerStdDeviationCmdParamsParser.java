package edu.arizona.cs.mrpkm.stddeviation;

import edu.arizona.cs.mrpkm.cmdparams.PKMCmdParamsParser;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

/**
 *
 * @author iychoi
 */
public class KmerStdDeviationCmdParamsParser extends PKMCmdParamsParser<KmerStdDeviationCmdParams> {
    public KmerStdDeviationCmdParamsParser() {
    }
    
    @Override
    public KmerStdDeviationCmdParams parse(String[] args) {
        KmerStdDeviationCmdParams cmdargs = new KmerStdDeviationCmdParams();
        CmdLineParser parser = new CmdLineParser(cmdargs);
        CmdLineException parseException = null;
        try {
            parser.parseArgument(args);
        } catch (CmdLineException e) {
            // handling of wrong arguments
            parseException = e;
        }
        
        if(cmdargs.isHelp() || !cmdargs.checkValidity()) {
            parser.printUsage(System.err);
            return cmdargs;
        }
        
        if(parseException != null) {
            System.err.println(parseException.getMessage());
            parser.printUsage(System.err);
            return null;
        }
        return cmdargs;
    }
}
