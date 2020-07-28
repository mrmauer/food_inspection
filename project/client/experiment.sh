#!/bin/sh

server=localhost
port=30235
inspfile=../data/chi2k.json
bulkfile=chi2k.csv 
tweetfile=../data/twit1.json 

while getopts :i:b:t:s:p: opt
do	
    case $opt in
        i) inspfile=$OPTARG;;
        b) bulkfile=$OPTARG;;
	    t) tweetfile=$OPTARG;;
	    s) server=$OPTARG;;
        p) port=$OPTARG;;        
	    [?]) echo Usage: $0 [-i inspection json] [-b inspection csv] [-t tweet json] [-s server] [-p port]
		exit 1;;
	esac
done

rm -rf ./exp_results
mkdir ./exp_results
mkdir ./exp_results/idx_pre
mkdir ./exp_results/idx_post
mkdir ./exp_results/idx_never
mkdir ./exp_results/tweet_loading

i=0
tot=17
for idx in pre post never
do
    for load in 1 10 100 1000 bulk
    do
        i=$((i + 1))
        echo Experiment $i/$tot: insp loading w/ index $idx, load $load
        if [ $load = bulk ]
            then
                loadfile=$bulkfile
            else
                loadfile=$inspfile
        fi
        python3 client.py -i $loadfile -s $server -p $port --load $load --index $idx 2> ./exp_results/idx_$idx/load_$load.txt
        if [ $load = bulk ] && [ $idx != pre ]
            then
                i=$((i + 1))
                if [ $idx = post ]
                    then
                        filename=withidx.txt
                        echo Experiment $i/$tot: tweet loading w/ index
                    else
                        filename=noidx.txt
                        echo Experiment $i/$tot: tweet loading w/o index
                fi
                python3 client.py -i $loadfile -t $tweetfile -s $server -p $port --load $load --index $idx 2> ./exp_results/tweet_loading/$filename
        fi
    done
done
echo Experiments complete! Output saved in ./exp_results/