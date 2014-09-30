package edu.arizona.cs.mrpkm.cluster;

/**
 *
 * @author iychoi
 */
public class MRClusterConfiguration_atmosmall1 extends AMRClusterConfiguration {
    
    private static final int CPU_CORE_PER_NODE = 2;
    private static final int CHILD_MEM_SIZE = 2048; // 2GB
    private static final int FILE_BUFFER_SIZE = 4096; // 4KB
    
    @Override
    public int getCoresPerMachine() {
        return CPU_CORE_PER_NODE;
    }

    @Override
    public int getReducerNumber(int nodes) {
        return nodes;
    }

    @Override
    public int getMapReduceChildMemSize() {
        return CHILD_MEM_SIZE;
    }

    @Override
    public int getMapReduceFileBufferSize() {
        return FILE_BUFFER_SIZE;
    }
}
