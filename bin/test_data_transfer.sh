#!/bin/bash

pipelines_list="16 32 64"
for $pipelines in $pipelines_list; do

    bw_uname="balasubr"

    # Removing existing files on remote
    gsissh $bw_uname@bw.ncsa.illinois.edu "rm -rf /u/sciteam/$bw_uname/scratch/test_data_transfer/*"

    # Remove any existing files on local
    rm *.dat -rf

    # Creating $pipelines number of 5.6MB files
    for i in `seq 1 1 $pipelines`; do
        dd if=/dev/zero of=output_$i.dat  bs=573K  count=10
    done

    # Transferring the output*.dat files from local to remote
    gsiscp *.dat $bw_uname@bw.ncsa.illinois.edu:/u/sciteam/$bw_uname/scratch/test_data_transfer/
    rm *.dat

    # Sequentially transfer output*.dat files from remote to local
    start=`date +%s`
    for i in `seq 1 1 $pipelines`; do
        gsisftp $bw_uname@bw.ncsa.illinois.edu:/u/sciteam/$bw_uname/scratch/test_data_transfer/output_$i.dat .
    done
    stop=`date +%s`

    # Find duration and write to file
    duration=$((stop - start))i

    # Save time by hour
    hour=`date +%I`
    echo "Pipelines: $pipelines, Duration:$duration secs" >> $HOME/transfer_$hour.prof

