from SPARQLWrapper import SPARQLWrapper, JSON, JSONLD, XML, RDFXML, TURTLE
from urllib.parse import urlparse
import requests
import pprint

queryString="""PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX peti: <http://auto.tuwien.ac.at/sic/PETIont#>
            PREFIX sosa: <http://www.w3.org/ns/sosa/>

            CONSTRUCT {?sensor a sosa:Sensor.
                ?sensor peti:hasPhysicalQuantity ?b.
                ?sensor sosa:madeObservation ?obs.
                ?obs sosa:hasSimpleResult ?value.} 
            where {
                ?sensor a sosa:Sensor.
                ?sensor peti:hasPhysicalQuantity ?b.
                ?sensor sosa:madeObservation ?obs.
                ?obs sosa:hasSimpleResult ?value.
            } LIMIT 3"""

federatedSPARQLQueryEndpoint="http://localhost:8080/rdf4j-server/repositories/federatedendpoint"
# federatedSPARQLQueryEndpoint=urlparse('localhost:8080/repositories/federatedendpoint/sparql') #change to rdf4j:8080
#-------------works-------------
sparql = SPARQLWrapper(federatedSPARQLQueryEndpoint)
sparql.setQuery(queryString)

sparql.setReturnFormat(JSONLD)
results = sparql.query().convert()
#----------------------
# for result in results["results"]["bindings"]:
#     print(result["sensor"]["value"])

# print('---------------------------')

# for result in results["results"]["bindings"]:
#     print('%s: %s' % (result["sensor"], result["sensor"]["value"]))


#----------alos works-----------
# response = requests.post(federatedSPARQLQueryEndpoint, data={'Accept':'application/sparql-results+json','query': queryString, })#federatedendpoint
# data = response.json()
# pprint.pprint(data)
#-----------------------

#response = requests.post(federatedSPARQLQueryEndpoint, data={'Content-Type':'application/sparq-results+json','query': queryString, })#application/ld+json
#results = response.json()
##results = response.content

#pprint.pprint(results)
print(results.serialize(format='json-ld'))