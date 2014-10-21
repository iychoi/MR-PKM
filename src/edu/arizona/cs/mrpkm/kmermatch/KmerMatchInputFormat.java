package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.mrpkm.kmerrange.KmerRangeSlicer;
import edu.arizona.cs.mrpkm.kmerrange.KmerRangeSlice;
import edu.arizona.cs.mrpkm.types.CompressedSequenceWritable;
import edu.arizona.cs.mrpkm.types.KmerIndexPathFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.PathFilter;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.security.TokenCache;

/**
 *
 * @author iychoi
 */
public class KmerMatchInputFormat extends SequenceFileInputFormat<CompressedSequenceWritable, KmerMatchResult> {

    private static final Log LOG = LogFactory.getLog(KmerMatchInputFormat.class);

    @Override
    public RecordReader<CompressedSequenceWritable, KmerMatchResult> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new KmerMatchRecordReader();
    }
    
    public static void setKmerSize(JobContext job, int kmerSize) {
        job.getConfiguration().setInt(KmerMatchHelper.getConfigurationKeyOfKmerSize(), kmerSize);
    }
    
    public static void setSliceNum(JobContext job, int numSlices) {
        job.getConfiguration().setInt(KmerMatchHelper.getConfigurationKeyOfSliceNum(), numSlices);
    }

    public List<InputSplit> getSplits(JobContext job) throws IOException {
        int kmerSize = job.getConfiguration().getInt(KmerMatchHelper.getConfigurationKeyOfKmerSize(), -1);
        if(kmerSize <= 0) {
            throw new IOException("kmer size must be a positive number");
        }
        
        int numSlices = job.getConfiguration().getInt(KmerMatchHelper.getConfigurationKeyOfSliceNum(), -1);
        if(numSlices <= 0) {
            throw new IOException("number of slices must be a positive number");
        }
        
        KmerRangeSlicer.SlicerMode slicerMode = job.getConfiguration().getEnum(KmerMatchHelper.getConfigurationSlicerMode(), KmerRangeSlicer.SlicerMode.MODE_EQUAL_ENTRIES);
        
        // generate splits
        List<InputSplit> splits = new ArrayList<InputSplit>();
        List<FileStatus> files = listStatus(job);
        List<Path> indexFiles = new ArrayList<Path>();
        for (FileStatus file : files) {
            Path path = file.getPath();
            long length = file.getLen();
            indexFiles.add(path);
        }
        
        LOG.info("# of Split input file : " + indexFiles.size());
        for(int i=0;i<indexFiles.size();i++) {
            LOG.info("> " + indexFiles.get(i).toString());
        }
        
        Path[] indexFilePaths = indexFiles.toArray(new Path[0]);
        
        KmerRangeSlicer slicer = new KmerRangeSlicer(kmerSize, numSlices, slicerMode);
        KmerRangeSlice[] slices = slicer.getSlices();
        
        for(KmerRangeSlice slice : slices) {
            splits.add(new KmerIndexSplit(indexFilePaths, slice));
        }
        
        // Save the number of input files in the job-conf
        job.getConfiguration().setLong(KmerMatchHelper.getConfigurationKeyOfInputFileNum(), files.size());

        LOG.debug("Total # of splits: " + splits.size());
        return splits;
    }
    
    @Override
    protected List<FileStatus> listStatus(JobContext job) throws IOException {
        List<FileStatus> result = new ArrayList<FileStatus>();
        Path[] dirs = getInputPaths(job);
        if (dirs.length == 0) {
            throw new IOException("No input paths specified in job");
        }

        // get tokens for all the required FileSystems..
        TokenCache.obtainTokensForNamenodes(job.getCredentials(), dirs, job.getConfiguration());

        // creates a MultiPathFilter with the hiddenFileFilter and the
        // user provided one (if any).
        List<PathFilter> filters = new ArrayList<PathFilter>();
        PathFilter jobFilter = getInputPathFilter(job);
        if (jobFilter != null) {
            filters.add(jobFilter);
        }
        filters.add(new KmerIndexPathFilter());
        PathFilter inputFilter = new MultiPathFilter(filters);

        for (int i = 0; i < dirs.length; ++i) {
            Path p = dirs[i];
            if(inputFilter.accept(p)) {
                FileSystem fs = p.getFileSystem(job.getConfiguration());
                FileStatus status = fs.getFileStatus(p);
                result.add(status);
            }
        }

        LOG.info("Total input paths to process : " + result.size());
        return result;
    }
    
    private static class MultiPathFilter implements PathFilter {

        private List<PathFilter> filters;

        public MultiPathFilter(List<PathFilter> filters) {
            this.filters = filters;
        }

        public boolean accept(Path path) {
            for (PathFilter filter : filters) {
                if (!filter.accept(path)) {
                    return false;
                }
            }
            return true;
        }
    }
}
