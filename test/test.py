#! /usr/bin/env python

import shutil
import os
import os.path
import subprocess
import sys
import glob

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

def findjars(path):
    realpath = os.path.realpath(path)
    findpattern = realpath + "/*.jar"
    jars = glob.glob(findpattern)
    return jars

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

def runReadIDIndexBuilder_0_20_2():
    removeOutDir();
    subprocess.call("cd ..;time java -cp dist/lib/*:dist/MR-PKM.jar edu.arizona.cs.mrpkm.MRPKM ReadIDIndexBuilder -n 2 -k 20 test/sample/input/ test/sample/output", shell=True)

def runReadIDIndexBuilder_2_3_0():
    removeOutDir();
    subprocess.call("cd ..;time hadoop jar dist/MR-PKM.jar ReadIDIndexBuilder -libjars dist/lib/* -n 2 -k 20 test/sample/input/ test/sample/output", shell=True)

def runKmerIndexBuilder_0_20_2():
    removeOutDir()
    subprocess.call("cd ..;time java -cp dist/lib/*:dist/MR-PKM.jar edu.arizona.cs.mrpkm.MRPKM KmerIndexBuilder -k 20 -i test/sample/ridx test/sample/input/ test/sample/output", shell=True)

def runKmerIndexBuilder_2_3_0():
    removeOutDir()
    subprocess.call("cd ..;time hadoop jar dist/MR-PKM.jar KmerIndexBuilder -libjars dist/lib/* -k 20 -i test/sample/ridx test/sample/input/ test/sample/output", shell=True)

def runKmerStandardDiviation_0_20_2():
    removeOutDir()
    subprocess.call("cd ..;time java -cp dist/lib/*:dist/MR-PKM.jar edu.arizona.cs.mrpkm.MRPKM KmerStdDiviation -k 20 test/sample/kidx test/sample/stddv", shell=True)

def runKmerStandardDiviation_2_3_0():
    removeOutDir()
    subprocess.call("cd ..;time hadoop jar dist/MR-PKM.jar KmerStdDiviation -libjars dist/lib/* -k 20 test/sample/kidx test/sample/stddv", shell=True)

def runPairwiseKmerModeCounter_0_20_2(mode):
    removeOutDir();
    smode = "range"
    if mode == 0:
        smode = "range"
    elif mode == 1:
        smode = "entries"
    elif mode == 2:
        smode = "weight"
    subprocess.call("cd ..;time java -cp dist/lib/*:dist/MR-PKM.jar edu.arizona.cs.mrpkm.MRPKM PairwiseKmerModeCounter --min 1 --max 999 --partitionermode " + smode + " test/sample/kidx test/sample/output", shell=True)

def runPairwiseKmerModeCounter_2_3_0(mode):
    removeOutDir();
    smode = "range"
    if mode == 0:
        smode = "range"
    elif mode == 1:
        smode = "entries"
    elif mode == 2:
        smode = "weight"
    subprocess.call("cd ..;time hadoop jar dist/MR-PKM.jar PairwiseKmerModeCounter -libjars dist/lib/* --min 1 --max 999 --partitionermode " + smode + " test/sample/kidx test/sample/output", shell=True)

def runTestKmerSequenceSlice(kmer, nslices, mode, samplePath):
    if samplePath:
        subprocess.call("cd ..;time java -cp dist/lib/*:dist/MR-PKM.jar edu.arizona.cs.mrpkm.tools.KmerSequencePartitionerTester " + str(kmer) + " " + str(nslices) + " " + str(mode) + " " + samplePath, shell=True)
    else:
        subprocess.call("cd ..;time java -cp dist/lib/*:dist/MR-PKM.jar edu.arizona.cs.mrpkm.tools.KmerSequencePartitionerTester " + str(kmer) + " " + str(nslices) + " " + str(mode), shell=True)

def main():
    if len(sys.argv) < 2:
        print "command : ./test.py run <classname> <program arguments> ..."
        print "command : ./test.py dep_0.20.2"
        print "command : ./test.py dep_2.3.0"
        print "command : ./test.py ridx_0.20.2"
        print "command : ./test.py ridx_2.3.0"
        print "command : ./test.py kidx_0.20.2"
        print "command : ./test.py kidx_2.3.0"
        print "command : ./test.py stddv_0.20.2"
        print "command : ./test.py stddv_2.3.0"
        print "command : ./test.py pkm_ee_0.20.2"
        print "command : ./test.py pkm_ee_2.3.0"
        print "command : ./test.py pkm_er_0.20.2"
        print "command : ./test.py pkm_er_2.3.0"
        print "command : ./test.py test_kslice_ee <kmer size> <num of slices>"
        print "command : ./test.py test_kslice_er <kmer size> <num of slices>"
        print "command : ./test.py test_kslice_wr <kmer size> <num of slices>"
        print "command : ./test.py test_kslice_s <kmer size> <num of slices>"
    else:
        command = sys.argv[1]

        if command == "run":
            run(sys.argv[2:])
        elif command == "dep_0.20.2":
            dep_0_20_2()
        elif command == "dep_2.3.0":
            dep_2_3_0()
        elif command == "ridx_0.20.2":
            runReadIDIndexBuilder_0_20_2()
        elif command == "ridx_2.3.0":
            runReadIDIndexBuilder_2_3_0()
        elif command == "kidx_0.20.2":
            runKmerIndexBuilder_0_20_2()
        elif command == "kidx_2.3.0":
            runKmerIndexBuilder_2_3_0()
        elif command == "stddv_0.20.2":
            runKmerStandardDiviation_0_20_2()
        elif command == "stddv_2.3.0":
            runKmerStandardDiviation_2_3_0()
        elif command == "pkm_ee_0.20.2":
            runPairwiseKmerModeCounter_0_20_2(1)
        elif command == "pkm_ee_2.3.0":
            runPairwiseKmerModeCounter_2_3_0(1)
        elif command == "pkm_er_0.20.2":
            runPairwiseKmerModeCounter_0_20_2(0)
        elif command == "pkm_er_2.3.0":
            runPairwiseKmerModeCounter_2_3_0(0)
        elif command == "test_kslice_ee":
            runTestKmerSequenceSlice(int(sys.argv[2]), int(sys.argv[3]), 1, None)
        elif command == "test_kslice_wr":
            runTestKmerSequenceSlice(int(sys.argv[2]), int(sys.argv[3]), 2, None)
        elif command == "test_kslice_er":
            runTestKmerSequenceSlice(int(sys.argv[2]), int(sys.argv[3]), 0, None)
        elif command == "test_kslice_s":
            runTestKmerSequenceSlice(int(sys.argv[2]), int(sys.argv[3]), 3, sys.argv[4])
        else:
            print "invalid command"

if __name__ == "__main__":
    main()
