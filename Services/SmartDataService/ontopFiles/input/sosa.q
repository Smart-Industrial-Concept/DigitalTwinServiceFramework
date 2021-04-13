[QueryGroup="Basic"] @collection [[
[QueryItem="AllSensors"]
prefix : <http://auto.tuwien.ac.at/sic/PETIont#>
prefix sosa:<http://www.w3.org/ns/sosa/>

select *
WHERE{
?s a sosa:Sensor

}

[QueryItem="m_in_Data"]
prefix : <http://auto.tuwien.ac.at/sic/PETIont#>
prefix sosa:<http://www.w3.org/ns/sosa/>

select *
WHERE{
?s a sosa:Observation.
?s sosa:hasSimpleResult ?res;
   sosa:resultTime ?time.

}
]]
