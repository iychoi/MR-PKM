package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.mrpkm.readididx.KmerHistogramHelper;
import edu.arizona.cs.mrpkm.types.histogram.KmerHistogram;
import edu.arizona.cs.mrpkm.types.histogram.KmerHistogramRecord;
import edu.arizona.cs.mrpkm.types.histogram.KmerHistogramRecordComparator;
import edu.arizona.cs.mrpkm.kmeridx.KmerIndexHelper;
import edu.arizona.cs.mrpkm.types.kmerrangepartition.KmerRangePartitioner;
import edu.arizona.cs.mrpkm.types.kmerrangepartition.KmerRangePartition;
import edu.arizona.cs.mrpkm.types.hadoop.CompressedSequenceWritable;
import edu.arizona.cs.mrpkm.types.filters.KmerIndexPathFilter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Hashtable;
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

    private final static String NUM_INPUT_FILES = "mapreduce.input.num.files";
    
    @Override
    public RecordReader<CompressedSequenceWritable, KmerMatchResult> createRecordReader(InputSplit split, TaskAttemptContext context) throws IOException {
        return new KmerMatchRecordReader();
    }
    
    public static void setInputFormatConfig(JobContext job, KmerMatchInputFormatConfig inputFormatConfig) {
        inputFormatConfig.saveTo(job.getConfiguration());
    }
    
    public List<InputSplit> getSplits(JobContext job) throws IOException {
        KmerMatchInputFormatConfig inputFormatConfig = new KmerMatchInputFormatConfig();
        inputFormatConfig.loadFrom(job.getConfiguration());
        
        int kmerSize = inputFormatConfig.getKmerSize();
        if(kmerSize <= 0) {
            throw new IOException("kmer size must be a positive number");
        }
        
        int numPartitions = inputFormatConfig.getPartitionNum();
        if(numPartitions <= 0) {
            throw new IOException("number of slices must be a positive number");
        }
        
        // generate splits
        List<InputSplit> splits = new ArrayList<InputSplit>();
        List<FileStatus> files = listStatus(job);
        List<Path> indexFiles = new ArrayList<Path>();
        for (FileStatus file : files) {
            Path path = file.getPath();
            indexFiles.add(path);
        }
        
        LOG.info("# of Split input file : " + indexFiles.size());
        for(int i=0;i<indexFiles.size();i++) {
            LOG.info("> " + indexFiles.get(i).toString());
        }
        
        Path[] indexFilePaths = indexFiles.toArray(new Path[0]);

        // histogram
        Hashtable<String, Boolean> indexGroupTable = new Hashtable<String, Boolean>();
        List<String> indexGroupFiles = new ArrayList<String>();
        for(int i=0;i<indexFiles.size();i++) {
            String fastaFileName = KmerIndexHelper.getFastaFileName(indexFiles.get(i));
            Boolean has = indexGroupTable.get(fastaFileName);
            if(has == null) {
                indexGroupTable.put(fastaFileName, true);
                indexGroupFiles.add(fastaFileName);
            }
        }
        indexGroupTable.clear();
        
        List<KmerHistogram> histogramReaders = new ArrayList<KmerHistogram>();
        for(String indexGroupEntry : indexGroupFiles) {
            Path histogramHadoopPath = new Path(inputFormatConfig.getHistogramPath(), KmerHistogramHelper.makeHistogramFileName(indexGroupEntry));
            FileSystem fs = histogramHadoopPath.getFileSystem(job.getConfiguration());
            if (fs.exists(histogramHadoopPath)) {
                KmerHistogram histogram = new KmerHistogram();
                histogram.loadFrom(histogramHadoopPath, fs);
                histogramReaders.add(histogram);
            } else {
                throw new IOException("k-mer histogram is not found in given paths");
            }
        }
        
        // merge histogram
        Hashtable<String, KmerHistogramRecord> histogramRecords = new Hashtable<String, KmerHistogramRecord>();
        long histogramRecordCounts = 0;
        for(int i=0;i<histogramReaders.size();i++) {
            KmerHistogramRecord[] records = histogramReaders.get(i).getSortedRecords();
            histogramRecordCounts += histogramReaders.get(i).getKmerCount();
            for(KmerHistogramRecord rec : records) {
                KmerHistogramRecord ext_rec = histogramRecords.get(rec.getKmer());
                if(ext_rec == null) {
                    histogramRecords.put(rec.getKmer(), rec);
                } else {
                    ext_rec.increaseCount(rec.getCount());
                }
            }
        }
        
        List<KmerHistogramRecord> histogramRecordsArr = new ArrayList<KmerHistogramRecord>();
        histogramRecordsArr.addAll(histogramRecords.values());
        Collections.sort(histogramRecordsArr, new KmerHistogramRecordComparator());
        
        KmerRangePartitioner partitioner = new KmerRangePartitioner(kmerSize, numPartitions);
        KmerRangePartition[] partitions = partitioner.getHistogramPartitions(histogramRecordsArr.toArray(new KmerHistogramRecord[0]), histogramRecordCounts);
        
        for(KmerRangePartition partition : partitions) {
            splits.add(new KmerMatchIndexSplit(indexFilePaths, partition));
        }
        
        // Save the number of input files in the job-conf
        job.getConfiguration().setLong(NUM_INPUT_FILES, files.size());

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
