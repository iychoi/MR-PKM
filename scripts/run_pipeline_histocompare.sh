prog="time hadoop jar MR-PKM-Dist.jar"

input=../metagenomics/tara_ocean
histogram=histo_tara
kidx=kidx_tara
stat=stat_tara
hcompare=hcompare_tara

echo "Running k-mer histogram comparisions for $input"
hadoop dfs -rm -r $hcompare
rm $hcompare.report
$prog PairwiseKmerFrequencyComparator -c uits -k 20 -n 12 -s $histogram -f $stat -u $kidx --report $hcompare.report $kidx $hcompare
