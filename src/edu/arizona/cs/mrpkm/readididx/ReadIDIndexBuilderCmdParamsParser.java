package edu.arizona.cs.mrpkm.readididx;

import edu.arizona.cs.mrpkm.cmdparams.PKMCmdParamsParser;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

/**
 *
 * @author iychoi
 */
public class ReadIDIndexBuilderCmdParamsParser extends PKMCmdParamsParser<ReadIDIndexCmdParams> {
    public ReadIDIndexBuilderCmdParamsParser() {
    }
    
    @Override
    public ReadIDIndexCmdParams parse(String[] args) {
        ReadIDIndexCmdParams cmdargs = new ReadIDIndexCmdParams();
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
