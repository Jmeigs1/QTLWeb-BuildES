docker run -d --ulimit nofile=200000:200000 -p 9200:9200 -p 9300:9300 -e "discovery.type=single-node" docker.elastic.co/elasticsearch/elaicsearch:7.2.0