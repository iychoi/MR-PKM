for samples in 48 #96
do
  echo "Running k-mer search of $samples samples"
  hadoop dfs -rm -r match$samples
  rm match$samples.report
  time hadoop jar MR-PKM-Dist.jar PairwiseKmerMatcher -c uits -k 20 -n 12 -s histo$samples -f stat$samples -u kidx$samples --report match$samples.report kidx$samples match$samples
done
