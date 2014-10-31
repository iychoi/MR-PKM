package edu.arizona.cs.mrpkm.readididx;

import edu.arizona.cs.mrpkm.cmdparams.APKMCmdParamsParser;
import org.kohsuke.args4j.CmdLineException;
import org.kohsuke.args4j.CmdLineParser;

/**
 *
 * @author iychoi
 */
public class ReadIDIndexBuilderCmdParamsParser extends APKMCmdParamsParser<ReadIDIndexBuilderCmdParams> {
    public ReadIDIndexBuilderCmdParamsParser() {
    }
    
    @Override
    public ReadIDIndexBuilderCmdParams parse(String[] args) {
        ReadIDIndexBuilderCmdParams cmdargs = new ReadIDIndexBuilderCmdParams();
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
