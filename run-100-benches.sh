echo "Spawning 100 processes"
for i in {1..100} ;
do
    ( ts-node bench_transactions.ts 100 100 http://192.168.1.3:9000 & ); 
done

