package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.mrpkm.cmdparams.PKMCmdParamsBase;
import java.util.ArrayList;
import java.util.List;
import org.kohsuke.args4j.Argument;
import org.kohsuke.args4j.Option;

/**
 *
 * @author iychoi
 */
public class ModeCounterCmdParams extends PKMCmdParamsBase {
    
    private ArrayList<Integer> rounds = new ArrayList<Integer>();
    
    @Option(name = "-r", aliases = "--round", usage = "specify Round ID to be excuted")
    public void addRound(int roundId) {
        this.rounds.add(roundId);
    }
    
    public List<Integer> getRounds() {
        return this.rounds;
    }
    
    @Argument(metaVar = "input-path output-path", usage = "input-path and output-path")
    private List<String> paths = new ArrayList<String>();

    public String getOutputPath() {
        if(this.paths.size() > 1) {
            return this.paths.get(this.paths.size()-1);
        }
        return null;
    }

    public String getInputPath() {
        if(this.paths.isEmpty()) {
            return null;
        }

        return this.paths.get(0);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        for(String arg : this.paths) {
            if(sb.length() != 0) {
                sb.append(", ");
            }

            sb.append(arg);
        }

        return "paths = " + sb.toString();
    }

    @Override
    public boolean checkValidity() {
        if(!super.checkValidity()) {
           return false;
        }
        
        if(this.paths == null || this.paths.isEmpty() || this.paths.size() != 2) {
            return false;
        }
        return true;
    }
}
