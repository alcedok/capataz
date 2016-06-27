

from elasticsearch import Elasticsearch
import json
es = Elasticsearch()
es.indices.refresh(index="cars")

query = { "query": { "filtered" : {"query" : {"match_all" : {}},
        "filter": 
          { "geo_distance": {
            "distance":      "10 miles",
            "distance_type": "sloppy_arc", 
            "pick_location": {
              "lat":   40.748609999999999,
              "lon": -73.984409999999983}
        }
      }
    }
  }
}

result = es.search(index='cars',body=query, ignore = 400,size=3)

print json.dumps(result, indent=2)







