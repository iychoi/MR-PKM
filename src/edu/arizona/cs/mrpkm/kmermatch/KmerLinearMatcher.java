package edu.arizona.cs.mrpkm.kmermatch;

import edu.arizona.cs.mrpkm.types.kmerrangepartition.KmerRangePartition;
import edu.arizona.cs.mrpkm.kmeridx.AKmerIndexReader;
import edu.arizona.cs.mrpkm.kmeridx.FilteredKmerIndexReader;
import edu.arizona.cs.mrpkm.kmeridx.KmerIndexHelper;
import edu.arizona.cs.mrpkm.statistics.KmerStatisticsHelper;
import edu.arizona.cs.mrpkm.types.hadoop.CompressedIntArrayWritable;
import edu.arizona.cs.mrpkm.types.hadoop.CompressedSequenceWritable;
import edu.arizona.cs.mrpkm.helpers.FileSystemHelper;
import edu.arizona.cs.mrpkm.helpers.SequenceHelper;
import edu.arizona.cs.mrpkm.types.statistics.KmerStdDeviation;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 *
 * @author iychoi
 */
public class KmerLinearMatcher {
    
    private static final Log LOG = LogFactory.getLog(KmerLinearMatcher.class);
    
    private Path[] inputIndexPaths;
    private String kmerIndexChunkInfoPath;
    private KmerRangePartition slice;
    private Configuration conf;
    private TaskAttemptContext context;
    
    private AKmerIndexReader[] readers;
    private BigInteger sliceSize;
    private CompressedSequenceWritable progressKey;
    private boolean eof;
    private BigInteger beginSequence;
    
    private CompressedSequenceWritable[] stepKeys;
    private CompressedIntArrayWritable[] stepVals;
    private List<Integer> stepMinKeys;
    private boolean stepStarted;
    
    public KmerLinearMatcher(Path[] inputIndexPaths, KmerRangePartition slice, String filterPath, String kmerIndexChunkInfoPath, TaskAttemptContext context) throws IOException {
        initialize(inputIndexPaths, slice, filterPath, kmerIndexChunkInfoPath, context, context.getConfiguration());
    }
    
    public KmerLinearMatcher(Path[] inputIndexPaths, KmerRangePartition slice, String filterPath, String kmerIndexChunkInfoPath, Configuration conf) throws IOException {
        initialize(inputIndexPaths, slice, filterPath, kmerIndexChunkInfoPath, null, conf);
    }
    
    private void initialize(Path[] inputIndexPaths, KmerRangePartition slice, String filterPath, String kmerIndexChunkInfoPath, TaskAttemptContext context, Configuration conf) throws IOException {
        this.inputIndexPaths = inputIndexPaths;
        this.kmerIndexChunkInfoPath = kmerIndexChunkInfoPath;
        this.slice = slice;
        this.conf = conf;
        this.context = context;
        
        Path[][] indice = KmerIndexHelper.groupKmerIndice(this.inputIndexPaths);
        this.readers = new AKmerIndexReader[indice.length];
        LOG.info("# of KmerIndexReader : " + indice.length);
        for(int i=0;i<indice.length;i++) {
            if(context != null) {
                context.progress();
            }
            
            FileSystem fs = indice[i][0].getFileSystem(this.conf);
            String fastaFilename = KmerIndexHelper.getFastaFileName(indice[i][0]);
            String stddevFilename = KmerStatisticsHelper.makeStdDeviationFileName(fastaFilename);
            Path stdDeviationPath = new Path(filterPath, stddevFilename);
            
            KmerStdDeviation stddevReader = new KmerStdDeviation();
            stddevReader.loadFrom(stdDeviationPath, fs);
            double avg = stddevReader.getAverage();
            double stddev = stddevReader.getStdDeviation();
            double factor = 2;
            this.readers[i] = new FilteredKmerIndexReader(fs, FileSystemHelper.makeStringFromPath(indice[i]), this.kmerIndexChunkInfoPath, this.slice.getPartitionBeginKmer(), this.slice.getPartitionEndKmer(), this.context, this.conf, avg, stddev, factor);
        }
        
        this.sliceSize = slice.getPartitionSize();
        this.progressKey = null;
        this.eof = false;
        this.beginSequence = this.slice.getPartitionBegin();
        this.stepKeys = new CompressedSequenceWritable[this.readers.length];
        this.stepVals = new CompressedIntArrayWritable[this.readers.length];
        this.stepStarted = false;
        
        LOG.info("Matcher is initialized");
        LOG.info("> Range " + this.slice.getPartitionBeginKmer() + " ~ " + this.slice.getPartitionEndKmer());
        LOG.info("> Num of Slice Entries : " + this.slice.getPartitionSize().longValue());
    }
    
    /*
    public void reset() throws IOException {
        for(AKmerIndexReader reader : this.readers) {
            reader.reset();
        }
        
        this.currentProgress = BigInteger.ZERO;
        this.stepKeys = new CompressedSequenceWritable[this.readers.length];
        this.stepVals = new CompressedIntArrayWritable[this.readers.length];
        this.stepStarted = false;
        this.reportCounter = 0;
    }
    */
    
    public KmerMatchResult stepNext() throws IOException {
        List<Integer> minKeyIndice = getNextMinKeys();
        if(minKeyIndice.size() > 0) {
            CompressedSequenceWritable minKey = this.stepKeys[minKeyIndice.get(0)];
            this.progressKey = minKey;
            
            // check matching
            CompressedIntArrayWritable[] minVals = new CompressedIntArrayWritable[minKeyIndice.size()];
            String[][] minIndexPaths = new String[minKeyIndice.size()][];

            int valIdx = 0;
            for (int idx : minKeyIndice) {
                minVals[valIdx] = this.stepVals[idx];
                minIndexPaths[valIdx] = this.readers[idx].getIndexPaths();
                valIdx++;
            }

            return new KmerMatchResult(minKey, minVals, minIndexPaths);
        }
        
        // step failed and no match
        this.eof = true;
        this.progressKey = null;
        return null;
    }
    
    private List<Integer> findMinKeys() throws IOException {
        CompressedSequenceWritable minKey = null;
        List<Integer> minKeyIndice = new ArrayList<Integer>();
        for(int i=0;i<this.readers.length;i++) {
            if(this.stepKeys[i] != null) {
                if(minKey == null) {
                    minKey = this.stepKeys[i];
                    minKeyIndice.clear();
                    minKeyIndice.add(i);
                } else {
                    int comp = minKey.compareTo(this.stepKeys[i]);
                    if (comp == 0) {
                        // found same min key
                        minKeyIndice.add(i);
                    } else if (comp > 0) {
                        // found smaller one
                        minKey = this.stepKeys[i];
                        minKeyIndice.clear();
                        minKeyIndice.add(i);
                    }
                }
            }
        }

        return minKeyIndice;
    }
    
    private List<Integer> getNextMinKeys() throws IOException {
        if(!this.stepStarted) {
            for(int i=0;i<this.readers.length;i++) {
                // fill first
                CompressedSequenceWritable key = new CompressedSequenceWritable();
                CompressedIntArrayWritable val = new CompressedIntArrayWritable();
                if(this.readers[i].next(key, val)) {
                    this.stepKeys[i] = key;
                    this.stepVals[i] = val;
                } else {
                    this.stepKeys[i] = null;
                    this.stepVals[i] = null;
                }
            }
            
            this.stepStarted = true;
            this.stepMinKeys = findMinKeys();
            return this.stepMinKeys;
        } else {
            // find min key
            if(this.stepMinKeys.size() == 0) {
                //EOF
                return this.stepMinKeys;
            }
            
            // move min pointers
            for (int idx : this.stepMinKeys) {
                CompressedSequenceWritable key = new CompressedSequenceWritable();
                CompressedIntArrayWritable val = new CompressedIntArrayWritable();
                if(this.readers[idx].next(key, val)) {
                    this.stepKeys[idx] = key;
                    this.stepVals[idx] = val;
                } else {
                    this.stepKeys[idx] = null;
                    this.stepVals[idx] = null;
                }
            }
            
            this.stepMinKeys = findMinKeys();
            return this.stepMinKeys;
        }
    }
    
    public float getProgress() {
        if(this.progressKey == null) {
            if(this.eof) {
                return 1.0f;
            } else {
                return 0.0f;
            }
        } else {
            BigInteger seq = SequenceHelper.convertToBigInteger(this.progressKey.getSequence());
            BigInteger prog = seq.subtract(this.beginSequence);
            int comp = this.sliceSize.compareTo(prog);
            if (comp <= 0) {
                return 1.0f;
            } else {
                BigDecimal progD = new BigDecimal(prog);
                BigDecimal rate = progD.divide(new BigDecimal(this.sliceSize), 3, BigDecimal.ROUND_HALF_UP);
                
                float f = rate.floatValue();
                return Math.min(1.0f, f);
            }
        }
    }
    
    public void close() throws IOException {
        for(AKmerIndexReader reader : this.readers) {
            reader.close();
        }
    }
}
