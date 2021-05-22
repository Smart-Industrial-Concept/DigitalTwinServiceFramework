#%%
from rdflib import Graph, ConjunctiveGraph, Literal, RDF, URIRef, Namespace
from rdflib.namespace import XSD
from rdflib.plugins.stores import sparqlstore
from rdflib.plugins.sparql.processor import SPARQLResult

import json
import pandas as pd

from SPARQLWrapper import SPARQLWrapper, JSON, JSONLD, XML, RDFXML, TURTLE
import requests

from collections import defaultdict
#%% Test read insert statement
f = open("insertStatement.txt", "r")
insert = f.read()
f.close()
print(insert)

PETI=Namespace("http://auto.tuwien.ac.at/sic/PETIont#")
ns=dict(peti=PETI)

g2 = Graph().parse(data=insert, format='n3')




#%%reading json-ld input
f = open("anomalies_json-ld.json", "r")
anomalies = f.read()
f.close()
#print(anomalies)

g = Graph().parse(data=anomalies, format='json-ld')

PETI=Namespace("http://auto.tuwien.ac.at/sic/PETIont#")
OWLTIME=Namespace("http://www.w3.org/2006/time#")
ns=dict(peti=PETI, time=OWLTIME)

result=g.query(""" 
SELECT ?anomaly ?sensor  ?startTime ?endTime
WHERE {?anomaly a peti:Anomaly;
                ^peti:hasAnomaly ?sensor;
                time:hasBeginning/time:inXSDDateTimeStamp ?startTime;
                time:hasEnd/time:inXSDDateTimeStamp ?endTime.

}""", 
initNs=ns)

def sparql_results_to_df(results: SPARQLResult):
    """
    Export results from an rdflib SPARQL query into a `pandas.DataFrame`,
    using Python types. See https://github.com/RDFLib/rdflib/issues/1179.
    """
    return pd.DataFrame(
        data=([None if x is None else x.toPython() for x in row] for row in results),
        columns=[str(x) for x in results.vars],
    )

df=sparql_results_to_df(result)

#%%group anomlies based on their occurance -> Incidence

incidencesIndexes=list()
indexList=list(range(len(df)))
searchList=list(range(len(df)))

while indexList:
    i=indexList.pop(0)
    currentTimes=df[['startTime','endTime']].iloc[i]
    currentList=[i]

    tempList=[]
    for j in indexList:
        iterTimes=df[['startTime','endTime']].iloc[j]

        if (iterTimes <= currentTimes+pd.Timedelta(minutes=4)).all() and (iterTimes >= currentTimes-pd.Timedelta(minutes=4)).all():
            currentList.append(j)
            tempList.append(j)   

    #remove temp from list
    indexList= [x for x in indexList if x not in tempList]
    incidencesIndexes.append(currentList)

#tranform indexes into sensors
incidences=list()
for i in range(len(incidencesIndexes)):
    incidences.append(df['sensor'].iloc[incidencesIndexes[i]].values.tolist())

#%%Inzidenz -> root sensor identifizieren

#genreate sparql query
inputString=""
for i in range(len(incidences[0])):
    inputString=inputString+"<"+incidences[0][i]+">\n"

queryString="""
PREFIX peti: <http://auto.tuwien.ac.at/sic/PETIont#>

SELECT ?sensor ?prevSensor
WHERE {
   VALUES ?sensor{""" + inputString + """ 
    }  
  ?sensor ^peti:hasMeasurement ?virtualProperty.
  ?virtualProperty ^peti:hasInfluenceOn/^peti:hasInfluenceOn/peti:hasEquivalentProperty* ?otherProperty.
  ?otherProperty peti:hasMeasurement ?prevSensor.
  
  VALUES ?prevSensor{"""+ inputString + """
  }
}
"""

causalRelationEndpoint="http://localhost:8086/rdf4j-server/repositories/causalrelations"
sparql = SPARQLWrapper(causalRelationEndpoint)
sparql.setQuery(queryString)
sparql.setReturnFormat(JSON)
results = sparql.query().convert()


resultGraph = defaultdict(list)

#create graph based on pytho dict
for result in results["results"]["bindings"]:
    #print('%s %s' % (result["sensor"]["value"], result["prevSensor"]["value"]))
    resultGraph[result["prevSensor"]["value"]]
    resultGraph[result["sensor"]["value"]].append(result["prevSensor"]["value"])

#get root node
for key in resultGraph:
    if len(resultGraph[key]) == 0:
        rootNode= key
        break
print(rootNode)

#%%von root sensor -> implicit redundancy (nächsten finden)
queryString="""
PREFIX peti: <http://auto.tuwien.ac.at/sic/PETIont#>
SELECT ?influencedSensor
WHERE {
      
    <""" + rootNode + """> ^peti:hasMeasurement ?virtualProperty.
    ?virtualProperty peti:hasInfluenceOn/peti:hasInfluenceOn ?otherVirtualProperty.
	?otherVirtualProperty peti:hasMeasurement ?influencedSensor.
}"""
sparql = SPARQLWrapper(causalRelationEndpoint)
sparql.setQuery(queryString)
sparql.setReturnFormat(JSON)
results = sparql.query().convert()

influencedSensorList=list()
for result in results["results"]["bindings"]:
    influencedSensorList.append([result["influencedSensor"]["value"]])
    
#%%voting über sensor fehler machen
#%ergebnis in eval-results schreiben
if len(influencedSensorList) >= (len(incidences[0])-1)/2:
    print("Sensor Fault")

    anomalyList=df['anomaly'].iloc[incidencesIndexes[0]].values.tolist()

    
    insertString=""
    #create peti:incidence instance
    insertString = insertString + "peti:incident_0 a peti:Incident.\n" 
    insertString = insertString + "peti:incident_0 a peti:SensorFault.\n"
    #link incident with anomalies
    for element in anomalyList:
        insertString=insertString + "peti:incident_0 peti:hasRelatedAnomaly <" + element +">.\n"

    #add rootsensor
    insertString = insertString + "peti:incident_0 peti:hasRootCause <"+ rootNode +">."

    sparqlString="""
    PREFIX peti: <http://auto.tuwien.ac.at/sic/PETIont#>
    PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

    INSERT { """ + insertString + """
    } WHERE {}
    """

    update_endpoint = 'http://localhost:8086/rdf4j-server/repositories/evalresutls/statements'  # localhost:3031// change to fuseki_anomaly:3030 if containerized
    query_endpoint = 'http://localhost:8086/rdf4j-server/repositories/evalresutls/'
    response = requests.post(update_endpoint, data={'content-type':'application/sparql-update','update': sparqlString, })#application/ld+json


else:
    print("abnormal behavior")

     
    anomalyList=df['anomaly'].iloc[incidencesIndexes[0]].values.tolist()

    
    insertString=""
    #create peti:incidence instance
    insertString = insertString + "peti:incident_1 a peti:Incident.\n" 
    insertString = insertString + "peti:incident_1 a peti:AbnormalBehavior.\n"
    #link incident with anomalies
    for element in anomalyList:
        insertString=insertString + "peti:incident_1 peti:hasRelatedAnomaly <" + element +">.\n"

    #add rootsensor
    insertString = insertString + "peti:incident_1 peti:hasRootCause <"+ rootNode +">."





#%%ergebnis als antwor senden mit type:"sensorFault"/"abnormalBehavior"/"unidentifiable"




