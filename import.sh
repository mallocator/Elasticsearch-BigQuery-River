#!/bin/bash

# query can also be javascript, e.g : "query":"var now = Date.now(); 'SELECT * FORM [t_' + now + '] LIMIT 10'"

JSON=$(cat <<EOF
{
    "type":"river-bigquery",
    "bigquery":{
        "index":"myindex",
        "type":"mytype",
        "project":"myproject",
        "keyFile":"keyfileInClasspath",
        "account":"googleAccount",
        "query":"SELECT * FROM [myTable]",
        "interval":"60000"
    }
}
EOF
)

curl -XDELETE 127.0.0.1:9200/_river/river-bigquery
echo
curl -XPUT 127.0.0.1:9200/_river/river-bigquery/_meta -d "$JSON"
echo
