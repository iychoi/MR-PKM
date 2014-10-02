package edu.arizona.cs.mrpkm.cluster;

/**
 *
 * @author iychoi
 */
public class MRClusterConfiguration_default extends AMRClusterConfiguration {
    
    private static final int CPU_CORE_PER_NODE = 1;
    
    @Override
    public int getCoresPerMachine() {
        return CPU_CORE_PER_NODE;
    }

    @Override
    public int getReadIndexBuilderReducerNumber(int nodes) {
        return nodes;
    }
    
    @Override
    public int getKmerIndexBuilderReducerNumber(int nodes) {
        return nodes;
    }

    @Override
    public int getPairwiseKmerModeCounterReducerNumber(int nodes) {
        return nodes;
    }

    @Override
    public int getMapReduceChildMemSize() {
        return 0;
    }

    @Override
    public int getMapReduceFileBufferSize() {
        return 0;
    }
}
