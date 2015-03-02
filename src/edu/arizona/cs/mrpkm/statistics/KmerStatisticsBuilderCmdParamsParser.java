package edu.arizona.cs.mrpkm.statistics;

import edu.arizona.cs.mrpkm.cmdparams.APKMCmdParamsParser;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

/**
 *
 * @author iychoi
 */
public class KmerStatisticsBuilderCmdParamsParser extends APKMCmdParamsParser<KmerStatisticsBuilderCmdParams> {
    public KmerStatisticsBuilderCmdParamsParser() {
    }
    
    @Override
    public KmerStatisticsBuilderCmdParams parse(String[] args) {
        KmerStatisticsBuilderCmdParams cmdargs = new KmerStatisticsBuilderCmdParams();
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
