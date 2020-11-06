
# This is the python script to load the data into the Elasticsearch
The script uses threading and Elasticsearch bulk API to push massive amounts of data in a timely manner. The folder contains Dockerfile and it is recommended to be run through docker, where every requirement for script to run will be met.
To build the docker image, run: docker build -t bigdata1:1.0 .

The script needs 5 environment variables to run, we can pass them like so:
docker run -e APP_TOKEN="your API token" -e DATASET_ID="your dataset id" -e ES_HOST="your elasticsearch host" -e ES_USERNAME="your elasticsearch username" -e ES_PASSWORD="your elasticsearch password" bigdata:1.0 

Script also supports two command line arguments: --page_size(requred) and --num_pages(optional). 
--page_size specifies how many records to request per API call.
--num_pages  if provided, specifies how many pages(--page_size) to get from API. If not provided, script will run until all records are pushed into the elasticsearch.

Arguments can be provided like so: docker run bigdata:1.0 --page_size=10 --num_pages=2

Full command should look something like this:
docker run -e APP_TOKEN="your API token" -e DATASET_ID="your dataset id" -e ES_HOST="your elasticsearch host" -e ES_USERNAME="your elasticsearch username" -e ES_PASSWORD="your elasticsearch password" bigdata:1.0 --page_size=10 --num_pages=2

# Kibana NYC parking violations example

![kibana dashboard](https://github.com/Swiimmingly/elasticsearch_data/blob/master/assets/kibanadashboard.png?raw=true)
