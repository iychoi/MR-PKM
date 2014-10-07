package edu.arizona.cs.mrpkm.cluster;

/**
 *
 * @author iychoi
 */
public class MRClusterConfiguration_uits extends AMRClusterConfiguration {
    
    private static final int CPU_CORE_PER_NODE = 12;
    private static final int CHILD_MEM_SIZE = 1024; // 1GB
    private static final int FILE_BUFFER_SIZE = 4096; // 4KB
    
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
        return nodes * (CPU_CORE_PER_NODE / 4);
    }

    @Override
    public int getPairwiseKmerModeCounterReducerNumber(int nodes) {
        return nodes * (CPU_CORE_PER_NODE / 4);
    }

    @Override
    public int getMapReduceChildMemSize() {
        return CHILD_MEM_SIZE;
    }

    @Override
    public int getMapReduceFileBufferSize() {
        return FILE_BUFFER_SIZE;
    }

    @Override
    public boolean isMapReduce2() {
        return true;
    }
}
