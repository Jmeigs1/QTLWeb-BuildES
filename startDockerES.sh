docker run -d --ulimit nofile=200000:200000 \
--restart always \
-v esdata01:/usr/share/elasticsearch/data \
-p 9200:9200 \
-p 9300:9300 \
-e "discovery.type=single-node" \
docker.elastic.co/elasticsearch/elasticsearch:7.2.0