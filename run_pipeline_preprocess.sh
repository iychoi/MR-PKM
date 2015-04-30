
prog="time hadoop jar MR-PKM-Dist.jar"

input=../metagenomics/tara_ocean
ridx=ridx_tara
histogram=histo_tara
kidx=kidx_tara
stat=stat_tara

echo "Running readID Index Builder for $input"
hadoop dfs -rm -r $ridx
hadoop dfs -rm -r $histogram
rm $ridx.report
$prog ReadIDIndexBuilder -c uits -k 20 -n 12 -s $histogram --report $ridx.report $input $ridx

echo "Running k-mer Index Builder for $input"
hadoop dfs -rm -r $kidx
rm $kidx.report
$prog KmerIndexBuilder -c uits -k 20 -n 12 -i $ridx -s $histogram --report $kidx.report $input $kidx
$prog KmerIndexChunkInfoBuilder -c uits -k 20 -n 12 $kidx $kidx

echo "Running Statistics of $samples samples"
hadoop dfs -rm -r $stat
rm $stat.report
$prog KmerStatisticsBuilder -c uits -k 20 -n 12 --report $stat.report $kidx $stat

echo "Preprocessing completed!"

