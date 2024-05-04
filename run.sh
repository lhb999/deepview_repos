#spark-submit --class Master.OnlySlave target/scala-2.11/etri-project_2.11-1.0.jar /root/ETRI/data/yeon1000000.txt 8 /root/ETRI/data/query/Query100_100ran.txt 
#spark-submit --class Master.OnlySlave target/scala-2.11/etri-project_2.11-1.0.jar /root/ETRI/data/DataForANN1000000.txt 8 /root/ETRI/data/query/RandomQuery100.txt 
#spark-submit --class Master.OnlySlave target/scala-2.11/etri-project_2.11-1.0.jar /root/ETRI/data/DataForANN1000000.txt 8 /root/ETRI/data/query/Query100_100ran.txt 
#spark-submit --class Master.OnlySlave target/scala-2.11/etri-project_2.11-1.0.jar /root/ETRI/data/Random1000000.txt 8 /root/ETRI/data/query/RandomQuery100.txt >> ANN_Test_Ran.txt
#spark-submit --class Master.OnlySlave target/scala-2.11/etri-project_2.11-1.0.jar /root/ETRI/data/Randomdata15000_16dim.txt 8 /root/ETRI/data/Randomdata15000_16dim.txt > KNN_Test_16dim_ran.txt
#spark-submit --class Master.OnlySlave target/scala-2.11/etri-project_2.11-1.0.jar /root/ETRI/data/Randomdata15000_16dim.txt 8 /root/ETRI/data/Randomdata15000_16dim.txt 
nohup spark-submit --class Master.main target/scala-2.11/etri-project_2.11-1.0.jar /root/ETRI/data/yeon1000000.txt 8 /root/ETRI/data/yeon1000000.txt 10
