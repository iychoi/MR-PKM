package edu.arizona.cs.mrpkm.stddiviation;

import edu.arizona.cs.mrpkm.cmdparams.PKMCmdParams;
import java.util.ArrayList;
import java.util.List;
import org.kohsuke.args4j.Argument;

/**
 *
 * @author iychoi
 */
public class KmerStdDiviationCmdParams extends PKMCmdParams {
    @Argument(metaVar = "input-path [input-path ...] output-path", usage = "input-paths and output-path")
    private List<String> paths = new ArrayList<String>();

    public String getOutputPath() {
        if(this.paths.size() > 1) {
            return this.paths.get(this.paths.size()-1);
        }
        return null;
    }

    public String[] getInputPaths() {
        if(this.paths.isEmpty()) {
            return new String[0];
        }

        String[] inpaths = new String[this.paths.size()-1];
        for(int i=0;i<this.paths.size()-1;i++) {
            inpaths[i] = this.paths.get(i);
        }
        return inpaths;
    }

    public String getCommaSeparatedInputPath() {
        String[] inputPaths = getInputPaths();
        StringBuilder CSInputPath = new StringBuilder();
        for(String inputpath : inputPaths) {
            if(CSInputPath.length() != 0) {
                CSInputPath.append(",");
            }
            CSInputPath.append(inputpath);
        }
        return CSInputPath.toString();
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
        
        if(this.paths == null || this.paths.isEmpty() || this.paths.size() < 2) {
            return false;
        }
        return true;
    }
}
