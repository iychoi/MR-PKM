#! /usr/bin/env python

import shutil
import os
import os.path
import subprocess
import sys
import glob

# CONFIG
MR_VER = 2

# global variables
jar_dependency_dir_hadoop_0_20_2 = [
    'hadoop_0_20_2_libs/'
]

jar_dependency_dir_hadoop_2_3_0 = [
    'hadoop_2_3_0_libs/'
]

jar_copy_to = '../dist/lib'

hadoop_2_3_0_exports = """
export JAVA_HOME=/usr/lib/jvm/java-7-openjdk-amd64
export HADOOP_PREFIX=/opt/cdh5.1.3/hadoop-2.3.0-cdh5.1.3
export HADOOP_COMMON_HOME=${HADOOP_PREFIX}
export HADOOP_HDFS_HOME=${HADOOP_PREFIX}
export HADOOP_MAPRED_HOME=${HADOOP_PREFIX}
export HADOOP_YARN_HOME=${HADOOP_PREFIX}
export PATH=${HADOOP_PREFIX}/bin:$PATH
export HADOOP_CONF_DIR=/opt/cdh5.1.3/hadoop-2.3.0-cdh5.1.3/etc/hadoop

"""

def isMR2():
    if MR_VER == 2:
        return True
    return False

# Copy dependencies
def findjars(path):
    realpath = os.path.realpath(path)
    findpattern = realpath + "/*.jar"
    jars = glob.glob(findpattern)
    return jars

def dep():
    if isMR2():
        dep_2_3_0()
    else:
        dep_0_20_2()

def dep_0_20_2():
    for jar_dir in jar_dependency_dir_hadoop_0_20_2:
	jars = findjars(jar_dir)
        for jar in jars:
            copyto = os.path.realpath(os.path.abspath(jar_copy_to))
            print "copying", jar
            shutil.copy2(jar, copyto)
    print "done!"

def dep_2_3_0():
    for jar_dir in jar_dependency_dir_hadoop_2_3_0:
	jars = findjars(jar_dir)
        for jar in jars:
            copyto = os.path.realpath(os.path.abspath(jar_copy_to))
            print "copying", jar
            shutil.copy2(jar, copyto)
    print "done!"

def removeOutDir():
    #remove outdir
    if os.path.exists('sample/output'):
        shutil.rmtree('sample/output')

def run(args):
    programargs = ""
    for x in range(0, len(args)):
        arg = args[x]
        programargs += arg

    subprocess.call("cd ..;time java -cp dist/lib/*:dist/MR-PKM.jar edu.arizona.cs.mrpkm.MRPKM " + programargs, shell=True)

# READ ID Index Builder
def runReadIDIndexBuilder():
    if isMR2():
        runReadIDIndexBuilder_2_3_0()
    else:
        runReadIDIndexBuilder_0_20_2()

def runReadIDIndexBuilder_0_20_2():
    removeOutDir();
    subprocess.call("cd ..;time java -cp dist/lib/*:dist/MR-PKM.jar edu.arizona.cs.mrpkm.MRPKM ReadIDIndexBuilder -n 2 -k 20 -s test/sample/histogram test/sample/input/ test/sample/ridx", shell=True)

def runReadIDIndexBuilder_2_3_0():
    removeOutDir();
    subprocess.call("cd ..;time hadoop jar store/MR-PKM-Dist.jar ReadIDIndexBuilder -n 2 -k 20 -s test/sample/histogram test/sample/input/ test/sample/ridx", shell=True)

# Kmer Index Builder
def runKmerIndexBuilder():
    if isMR2():
        runKmerIndexBuilder_2_3_0()
    else:
        runKmerIndexBuilder_0_20_2()

def runKmerIndexBuilder_0_20_2():
    removeOutDir()
    subprocess.call("cd ..;time java -cp dist/lib/*:dist/MR-PKM.jar edu.arizona.cs.mrpkm.MRPKM KmerIndexBuilder -k 20 -i test/sample/ridx -s test/sample/histogram test/sample/input/ test/sample/kidx", shell=True)

def runKmerIndexBuilder_2_3_0():
    removeOutDir()
    subprocess.call("cd ..;time hadoop jar store/MR-PKM-Dist.jar KmerIndexBuilder -k 20 -i test/sample/ridx -s test/sample/histogram test/sample/input/ test/sample/kidx", shell=True)

# Kmer Index Chunk Info Builder
def runKmerIndexChunkInfoBuilder():
    if isMR2():
        runKmerIndexChunkInfoBuilder_2_3_0()
    else:
        runKmerIndexChunkInfoBuilder_0_20_2()

def runKmerIndexChunkInfoBuilder_0_20_2():
    removeOutDir()
    subprocess.call("cd ..;time java -cp dist/lib/*:dist/MR-PKM.jar edu.arizona.cs.mrpkm.MRPKM KmerIndexChunkInfoBuilder -k 20 test/sample/kidx test/sample/chunkinfo", shell=True)

def runKmerIndexChunkInfoBuilder_2_3_0():
    removeOutDir()
    subprocess.call("cd ..;time hadoop jar store/MR-PKM-Dist.jar KmerIndexChunkInfoBuilder -k 20 test/sample/kidx test/sample/chunkinfo", shell=True)

# Kmer Index Statistics
def runKmerStatistics():
    if isMR2():
        runKmerStatistics_2_3_0()
    else:
        runKmerStatistics_0_20_2()

def runKmerStatistics_0_20_2():
    removeOutDir()
    subprocess.call("cd ..;time java -cp dist/lib/*:dist/MR-PKM.jar edu.arizona.cs.mrpkm.MRPKM KmerStatisticsBuilder -k 20 test/sample/kidx test/sample/statistics", shell=True)

def runKmerStatistics_2_3_0():
    removeOutDir()
    subprocess.call("cd ..;time hadoop jar store/MR-PKM-Dist.jar KmerStatisticsBuilder -k 20 test/sample/kidx test/sample/statistics", shell=True)

# Pairwise Kmer Frequency Comparator
def runKmerFrequencyComparison():
    runKmerFrequencyComparison_2_3_0()

def runKmerFrequencyComparison_2_3_0():
    removeOutDir();
    subprocess.call("cd ..;time hadoop jar store/MR-PKM-Dist.jar PairwiseKmerFrequencyComparator -k 20 -s test/sample/histogram -u test/sample/chunkinfo -f test/sample/statistics test/sample/kidx test/sample/kfc", shell=True)

# Pairwise Kmer Matcher
def runPairwiseKmerMatcher():
    runPairwiseKmerMatcher_2_3_0()

def runPairwiseKmerMatcher_2_3_0():
    removeOutDir();
    subprocess.call("cd ..;time hadoop jar store/MR-PKM-Dist.jar PairwiseKmerMatcher -k 20 -s test/sample/histogram -u test/sample/chunkinfo -f test/sample/stddv test/sample/kidx test/sample/match", shell=True)

# Mode Counter
def runModeCounter():
    runModeCounter_2_3_0()

def runModeCounter_2_3_0():
    removeOutDir();
    subprocess.call("cd ..;time hadoop jar store/MR-PKM-Dist.jar ModeCounter -k 20 test/sample/match test/sample/mode", shell=True)

def main():
    if len(sys.argv) < 2:
        print "command : ./test.py run <classname> <program arguments> ..."
        print "command : ./test.py dep"
        print "command : ./test.py ridx"
        print "command : ./test.py kidx"
        print "command : ./test.py kidxci"
        print "command : ./test.py stat"
        print "command : ./test.py kfc"
        print "command : ./test.py matcher"
        print "command : ./test.py mode"
    else:
        command = sys.argv[1]

        if command == "run":
            run(sys.argv[2:])
        elif command == "dep":
            dep()
        elif command == "ridx":
            runReadIDIndexBuilder()
        elif command == "kidx":
            runKmerIndexBuilder()
        elif command == "kidxci":
            runKmerIndexChunkInfoBuilder()
        elif command == "stat":
            runKmerStatistics()
        elif command == "kfc":
            runKmerFrequencyComparison()
        elif command == "matcher":
            runPairwiseKmerMatcher()
        elif command == "mode":
            runModeCounter()
        else:
            print "invalid command"

if __name__ == "__main__":
    main()
