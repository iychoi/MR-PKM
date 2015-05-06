for samples in 6 12 24 48 96
do
  echo "Running Statistics of $samples samples"
  hadoop dfs -rm -r stat$samples
  rm stat$samples.report
  time hadoop jar MR-PKM-Dist.jar KmerStatisticsBuilder -c uits -k 20 -n 12 --report stat$samples.report kidx$samples stat$samples
done
