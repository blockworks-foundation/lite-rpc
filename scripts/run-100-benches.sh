echo "Spawning 100 processes"
for i in {1..10} ;
do
    ( ts-node bench_transactions.ts 100 100 http://5.62.126.197:10800 & ); 
done

