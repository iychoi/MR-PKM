prog="time hadoop jar MR-PKM-Dist.jar"
input=../metagenomics/pov
histogram=histo_pov
kidx=kidx_pov
stat=stat_pov
hcompare=hcompare_pov
match=match_pov

echo "Running k-mer search of $samples samples"
hadoop dfs -rm -r $match
rm $match.report
time hadoop jar MR-PKM-Dist.jar PairwiseKmerMatcher -c uits -k 20 -n 12 -s $histogram -f $stat -u $kidx --report $match.report $kidx $match
