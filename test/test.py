#! /usr/bin/env python

import shutil
import os
import os.path
import subprocess
import sys
import glob

# global variables
jar_dependency_dirs = [
    'hadoop_libs/'
]

jar_copy_to = '../dist/lib'

def findjars(path):
    realpath = os.path.realpath(path)
    findpattern = realpath + "/*.jar"
    jars = glob.glob(findpattern)
    return jars

def dep():
    for jar_dir in jar_dependency_dirs:
	jars = findjars(jar_dir)
        for jar in jars:
            copyto = os.path.realpath(os.path.abspath(jar_copy_to))
            print "copying", jar
            shutil.copy2(jar, copyto)
    print "done!"

def run(args):
    programargs = ""
    for x in range(0, len(args)):
        arg = args[x]
        programargs += arg

    subprocess.call("cd ..;time java -cp dist/lib/*:dist/MR-PKM.jar edu.arizona.cs.mrpkm.MRPKM " + programargs, shell=True)

def runReadIDIndexBuilder():
    #remove outdir
    if os.path.exists('sample/output'):
        shutil.rmtree('sample/output')

    subprocess.call("cd ..;time java -cp dist/lib/*:dist/MR-PKM.jar edu.arizona.cs.mrpkm.MRPKM ReadIDIndexBuilder default test/sample/input/CP* test/sample/output", shell=True)

def runKmerIndexBuilder():
    #remove outdir
    if os.path.exists('sample/output'):
        shutil.rmtree('sample/output')

    subprocess.call("cd ..;time java -cp dist/lib/*:dist/MR-PKM.jar edu.arizona.cs.mrpkm.MRPKM KmerIndexBuilder default false 20 1 test/sample/input/CP* test/sample/ridx test/sample/output", shell=True)

def main():
    if len(sys.argv) < 2:
        print "command : ./test.py run <classname> <program arguments> ..."
        print "command : ./test.py dep"
        print "command : ./test.py ridx"
        print "command : ./test.py kidx"
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
        else:
            print "invalid command"

if __name__ == "__main__":
    main()
