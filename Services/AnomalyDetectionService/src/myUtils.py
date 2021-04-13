# -*- coding: utf-8 -*-
"""
Created on Thu Dec  5 08:34:00 2019

@author: gsteindl
"""
import numpy as np
import pandas as pd

from sklearn import linear_model
from sklearn import model_selection
from sklearn.metrics import mean_absolute_error
from sklearn.metrics import r2_score

#import matplotlib.pyplot as plt 
import pyprog

from sklearn.neighbors import KNeighborsRegressor

from rdflib import Graph, Literal, RDF, URIRef, Namespace
from rdflib.namespace import XSD

from rdflib.plugins.stores import sparqlstore

# just using the current value as forecast for values[t+1]. Then the mean absolute error as well
# as R² is calculated. This is just used as a reference for the performance measurement of the other forecast models.
def simpleForecast(outputValues):
        
    y_pred=outputValues.shift(1)
    
    y_pred=np.array(y_pred.values)[1:]
    y_true=np.array(outputValues.values)[1:]
    
    #error=y_true-y_pred
    #plt.plot(error)
    
    mae=mean_absolute_error(y_true,y_pred)
    r2=r2_score(y_true,y_pred)
    print('SimpleScore - \tMAE: '+ str(mae) + '\tR2: '+ str(r2))
    

def identifyARX(inputValues,outputValues,delays=1,validationSize=0.20):

    #add time delays in input vector
  
    if delays>0:
        for i in range(1, delays+1) :
          inputValues = pd.concat([inputValues, outputValues.shift(i)], axis=1, ignore_index=True)
    
        #remove nan
        inputValues=inputValues[delays:]
        outputValues=outputValues[delays:]
    
    #split
    validation_size = 0.20
    seed = 7
    X_train, X_validation, Y_train, Y_validation = model_selection.train_test_split(inputValues, outputValues, test_size=validation_size, random_state=seed)
    
    
    #train linear regression
    model=linear_model.LinearRegression()
    if isinstance(X_train, pd.Series):
        #model.fit(X_train.values.reshape(-1,1),Y_train.values.reshape(-1,1))
        model.fit(X_train.values.reshape(-1,1),Y_train.values.reshape(-1,1))
    else:
        #remove Nan rows
        model.fit(X_train,Y_train)
    
    
    #validation
    y_pred=list()
    
    for i in range(0,len(X_validation)):
        if isinstance(X_train, pd.Series):
            y_pred.append(model.predict(X_validation.iloc[[i]].values.reshape(-1,1))[0][0])
        else:
            y_pred.append(model.predict(X_validation.iloc[[i],:])[0])
    
    y_pred=np.array(y_pred)
    y_true=np.array(Y_validation.values)
    
    #TODO:  prediction is starting one time solt too early!!!
    
    #error=y_true-y_pred
    #plt.plot(error)
    
    mae=mean_absolute_error(y_true,y_pred)
    r2=r2_score(y_true,y_pred)
    print('Score - \tMAE: '+ str(mae) + '\tR2: '+ str(r2))
    
    
    return model

def addingNoiseToSensorData(data,tempSensorList,massFlowSensorList,controlVariableList,varTemp,varMassFlow,visualize=False):
    temps=data[tempSensorList]
    noisyTemps=temps.apply(lambda x: x+np.random.normal(0,varTemp,len(temps)))

    massflows=data[massFlowSensorList]
    noisyMassflows=massflows.apply(lambda x: x+np.random.normal(0,varMassFlow,len(massflows)))

    noisyData=pd.concat([noisyTemps,noisyMassflows,data[controlVariableList[0]],data[controlVariableList[1]]],axis=1)
    
    # if visualize==True:
    #     fig, ax=plt.subplots(2)
    #     ax[0].plot(temps,alpha=0.7)
    #     ax[0].plot(noisyTemps,alpha=0.7)
    #     ax[0].set_ylabel('Temperaures [K]')
    #     ax[1].plot(massflows,alpha=0.7)
    #     ax[1].plot(noisyMassflows,alpha=0.7)
    #     ax[1].set_ylabel('Mass Flow [kg/s]')
    
    return noisyData

def simulateUseCaseSerialParallelModel(models,workingDataForPrediction,startIndex=1,stopIndex=1,windowSize=1):
    
    u=workingDataForPrediction[['Actuator_Fan1','Actuator_H1','T_in','m_out','T_ext']].copy()
    x=workingDataForPrediction[['m_in','T_sup']].copy()
    y=workingDataForPrediction[['T_hx','T_p']].copy()
    
    measuredData=workingDataForPrediction[['m_in','T_sup','T_hx','T_p']]
    
    
    ventilationModel=models['http://auto.tuwien.ac.at/sic/PETIont#Fan1']['model']
    heaterModel=models['http://auto.tuwien.ac.at/sic/PETIont#H1']['model']
    heatExModel=models['http://auto.tuwien.ac.at/sic/PETIont#HE1']['model']
    processModel=models['http://auto.tuwien.ac.at/sic/PETIont#SiPro']['model']
     
    predResults=np.empty((stopIndex+1,len(x.columns)+len(y.columns)))
    predResults.fill(np.nan)
    
    print("Start seriell-parallel model simulation...")
        
    #create a fancy progress bar to indicate progress
    prog = pyprog.ProgressBar("Simulation started... ", " OK!")
    prog.update()
        
    #residuals=np.zeros((stopIndex+1-windowSize,len(x.columns)+len(y.columns)))
    residuals=np.zeros((stopIndex+1,len(x.columns)+len(y.columns)))
    
    for currentIndex in range(startIndex,stopIndex+1-windowSize):
        
        predWindowResults=np.zeros((windowSize,len(x.columns)+len(y.columns)))      
        x_prev=x.iloc[currentIndex-1].copy()
        y_prev=y.iloc[currentIndex-1].copy()
        
        windowIndex=0
        for index in range(currentIndex, currentIndex+windowSize):
            
            u_current=u.iloc[index]  
                #without copy() delay because otherwise they are overwritten in the original x (call by reference)
            x_current=x.iloc[index].copy()
            #y_current=y.iloc[index].copy()
                  
            currentModelInput=np.array([u_current['Actuator_Fan1'],x_prev['m_in']]) 
            m_hat=ventilationModel.predict(currentModelInput.reshape(1,-1))

            currentModelInput=np.array([x_current['m_in'],y_prev['T_hx'],u_current['Actuator_H1'],x_prev['T_sup']])
            T_sup_hat=heaterModel.predict(currentModelInput.reshape(1,-1))
        
            currentModelInput=np.array([x_current['m_in'],u_current['m_out'],u_current['T_in'],u_current['T_ext'],y_prev['T_hx']])
            T_hx_hat=heatExModel.predict(currentModelInput.reshape(1,-1))
            
            currentModelInput=np.array([x_current['m_in'],x_current['T_sup'],y_prev['T_p']])
            T_p_hat=processModel.predict(currentModelInput.reshape(1,-1))
                        
            predWindowResults[windowIndex]=np.array([m_hat[0],T_sup_hat[0],T_hx_hat[0],T_p_hat[0]]).flatten()
        
        #todo parallel structure should not use prediced inputs, insted predict all compnents in parallel
            x_prev['m_in']=m_hat[0]
            x_prev['T_sup']=T_sup_hat[0]
            y_prev['T_hx']=T_hx_hat[0]
            y_prev['T_p']=T_p_hat[0]
                        
            windowIndex=windowIndex+1
       
        predResults[currentIndex+windowSize]=predWindowResults[windowSize-1]
        
        #calculate residuals
        
        r=measuredData[currentIndex:currentIndex+windowSize]-predWindowResults
        windowResiduals=r/predWindowResults*100
        
        #using only the max Ressidual
        #residuals[currentIndex]=windowResiduals.abs().max()
        residuals[currentIndex+windowSize]=windowResiduals.iloc[windowSize-1]
        
        
        # Show (Update) the current status
        p=round(currentIndex/((stopIndex+1-windowSize-startIndex))*100)
        prog.set_stat(p)
        prog.update()
    
    print("\n... done!")   
    prog.end()

    return predResults, residuals




def returnFaultIndexs(df, blockSize=10):
    '''Find block of faults in a row '''
    faultIndexes=list()
    #i=df.first_vald_index()
    startIndex=0
    stopIndex=0
    
    status=0 # 0 noFault 
    # blockSize=4
    counterFault=0
    counterNoFault=0
    
    for label,content in df.iteritems():
        if(status == 0 and not pd.isna(content)): # fault beginning
            counterNoFault=0
            
            if counterFault >= blockSize:
                status=1 #found start
                startIndex=label-(counterFault)
                counterFault=0
            
            counterFault=counterFault+1
    
        elif status == 1 and (pd.isna(content) or label==df.index[-1]):    #if an fault occures till the end of the series
            counterFault=0
        
            if counterNoFault >= blockSize or label==df.index[-1]:
                status=0 #found end
                stopIndex=label-counterNoFault
                counterNoFault=0
                faultIndexes.append((startIndex,stopIndex))
                
            counterNoFault=counterNoFault+1
        else:
            counterNoFault=0
            counterFault=0
    return faultIndexes


def getTimeStampFromIndex(indexList, refTimeIndex):
    '''retrive the timestamp for a certain index'''
    timestampList=[]
   
    for el in indexList:
        timestampList.append({'hasBeginning':refTimeIndex[el[0]],'hasEnd':refTimeIndex[el[1]]})
        
    return timestampList


def storeAnomalyAtSparqlEndpoint(anomaliesDict,query_endpoint,update_endpoint):
    '''writes anomalies to sparl endpoint '''

    #sosaNsString='http://www.w3.org/ns/sosa/'
    #sosa = Namespace(sosaNsString)
    owlTimeString="http://www.w3.org/2006/time#"
    owlTime =Namespace(owlTimeString)
    petiString="http://auto.tuwien.ac.at/sic/PETIont#"
    peti =Namespace(petiString)



    store = sparqlstore.SPARQLUpdateStore(auth=('admin','123'))
    store.open((query_endpoint, update_endpoint))

    default_graph = URIRef('anomalyGraph')

    g=Graph(store, identifier=default_graph)
    #the second graph is necessary to avoid bug for add_graph() function in rdflib and still can serialize in json-ld
    g2=Graph()
    #g=Graph(identifier=default_graph)
    g.bind("peti",peti)
    g.bind("time",owlTime)
    i=0
    store.add_graph(g)
    for key in anomaliesDict:
        for anomaly in anomaliesDict[key]:
            id=str(i)
            anomalyURI=petiString + 'anomaly_'+ key + '_' + id
            anomalyStartURI=petiString + 'anomalyStart_'+key+ '_' + id
            anomalyEndURI=petiString + 'anomalyEnd_'+key+ '_' + id
            
            #anomaly
            g.add((URIRef(anomalyURI), RDF.type, peti.Anomaly))
            g.add((URIRef(anomalyURI), RDF.type, owlTime.Interval))
            #instant for begin and and
            g.add((URIRef(anomalyStartURI), RDF.type, owlTime.Instant))
            g.add((URIRef(anomalyEndURI), RDF.type, owlTime.Instant))
            
            g.add((URIRef(anomalyURI), owlTime.hasBeginning, URIRef(anomalyStartURI) ))
            g.add((URIRef(anomalyURI), owlTime.hasEnd, URIRef(anomalyEndURI) ))
            g.add((URIRef(anomalyStartURI), owlTime.inXSDDateTimeStamp , Literal(anomaly['hasBeginning'])) )
            g.add((URIRef(anomalyEndURI), owlTime.inXSDDateTimeStamp , Literal(anomaly['hasEnd'])))
            g.add((URIRef(petiString+key),peti.hasAnomaly,URIRef(anomalyURI)))  

            #second graph to avoid bug (see above)
            #anomaly
            g2.add((URIRef(anomalyURI), RDF.type, peti.Anomaly))
            g2.add((URIRef(anomalyURI), RDF.type, owlTime.Interval))
            #instant for begin and and
            g2.add((URIRef(anomalyStartURI), RDF.type, owlTime.Instant))
            g2.add((URIRef(anomalyEndURI), RDF.type, owlTime.Instant))
            
            g2.add((URIRef(anomalyURI), owlTime.hasBeginning, URIRef(anomalyStartURI) ))
            g2.add((URIRef(anomalyURI), owlTime.hasEnd, URIRef(anomalyEndURI) ))
            g2.add((URIRef(anomalyStartURI), owlTime.inXSDDateTimeStamp , Literal(anomaly['hasBeginning'])) )
            g2.add((URIRef(anomalyEndURI), owlTime.inXSDDateTimeStamp , Literal(anomaly['hasEnd'])))
            g2.add((URIRef(petiString+key),peti.hasAnomaly,URIRef(anomalyURI)))  

            i=i+1

    #store.add_graph(g)
    store.commit()
    store.close()

    return g2