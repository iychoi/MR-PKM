for samples in 48 #96
do
  echo "Running mode counter of $samples samples"
  hadoop dfs -rm -r mode$samples
  rm mode$samples.report
  time hadoop jar MR-PKM-Dist.jar ModeCounter -c uits -k 20 -n 12 --report mode$samples.report match$samples mode$samples
done
