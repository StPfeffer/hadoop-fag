```
cd ~/hadoop
cd /netflix
mvn clean compile
cd ..
cp netflix_titles.csv ./data/
cp netflix/target/NetflixMR-uber.jar ./data/
```

```
docker compose up -d
docker exec -it hadoop-resource-manager-1 bash

hdfs dfs -mkdir -p /input /output
hdfs dfs -put /data/netflix_titles.csv /input
hadoop jar /data/NetflixMR-uber.jar /input/netflix_titles.csv /output/result_$(date +%s)
```

```
hdfs dfs -cat /output/result_<TIMESTAMP>/part-r-00000
```
