package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.mrpkm.cmdparams.APKMCmdParamsParser;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

/**
 *
 * @author iychoi
 */
public class PairwiseKmerMatcherCmdParamsParser extends APKMCmdParamsParser<PairwiseKmerMatcherCmdParams> {
    public PairwiseKmerMatcherCmdParamsParser() {
    }

    @Override
    public PairwiseKmerMatcherCmdParams parse(String[] args) {
        PairwiseKmerMatcherCmdParams cmdargs = new PairwiseKmerMatcherCmdParams();
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
