'''
Copyright (C) <2012> <tousif.pasha@gmail.com>

Permission is hereby granted, free of charge, 
to any person obtaining a copy of this software 
and associated documentation files (the "Software"),
 to deal in the Software without restriction, including without limitation the rights to use, 
 copy, modify, merge, publish, distribute, sublicense, and/or sell copies of the Software, 
 and to permit persons to whom the Software is furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all copies or 
substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
 INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND 
 NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES
 OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF
  OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
'''

from urllib import request
import urllib
import base64
import json
import xml.etree.ElementTree as El
import sys


class Hwrapper:
    

    def __init__(self):
        self.port=None
        self.host=None
        self.content_type=None
        self.requestObject=None
        
    def setAcceptType(self,content_type):
        if content_type.lower() == "json":
            self.content_type="application/json"
        if content_type.lower() == "xml":
            self.content_type="text/xml"   
            
    def setTable(self,tableName):
        self.tableName=tableName
        
    def connectionParameters(self,host,port,https=False):
        
        if self.content_type == None:
            self.content_type="text/xml"
            
        self.https=https
        self.host=host
        self.port=port
    
    
    def getBaseUrl(self):
        
        if self.https:
            url="https://"+str(self.host)+":"+str(self.port)
        else:
            url="http://"+str(self.host)+":"+str(self.port)
        
        return url
    
    
    def getVersion(self):
        if self.host != None or self.port!=None:
            try :
                requestObj=request.Request(self.getBaseUrl()+"/version")
                requestObj.add_header("Accept",str(self.content_type))
                if self.content_type=="application/json":
                    response=json.loads(request.urlopen(requestObj).read().decode('utf-8'))
                    return response
                else:
                    response=El.fromstring(request.urlopen(requestObj).read())
                    return response
            except:
                print(" Error:",sys.exc_info()[0])
                raise   
        else:
            print("Please assign host port before query")
            raise IOError
        
#
#        Table operations
#
#

    def list_tables(self):
        if self.host != None or self.port!=None:
            try :
                requestObj=request.Request(self.getBaseUrl())
                requestObj.add_header("Accept",str(self.content_type))
                if self.content_type=="application/json":
                    response=json.loads(request.urlopen(requestObj).read().decode('utf-8'))
                    return response
                else:
                    print(request.urlopen(requestObj).read())
                    response=El.fromstring(request.urlopen(requestObj).read())
                    return response
            except:
                print(" Error:",sys.exc_info()[0])
                raise   
        else:
            print("Please assign host port before querying list tables")
            raise IOError
        return
    
    def table_schema(self,table_Name):
        if self.host != None or self.port!=None:
            try :
                requestObj=request.Request(self.getBaseUrl()+str("/"+table_Name+"/schema"))
                requestObj.add_header("Accept",str(self.content_type))
                if self.content_type=="application/json":
                    response=json.loads(request.urlopen(requestObj).read().decode('utf-8'))
                    return response
                else:
                    response=El.fromstring(request.urlopen(requestObj).read())
                    return response
            except:
                print(" Error:",sys.exc_info()[0])
                raise   
        else:
            print("Please assign host port before querying table schema")
            raise IOError
        return
    
    def create_table(self,table_Name,columnArray):
        
#        request.get_method = lambda: 'PUT'
        schema= "<?xml version='1.0' encoding='UTF-8' standalone='yes'?><TableSchema name='"+table_Name+"'  IS_META='false' IS_ROOT='false'>"
        if isinstance(columnArray,set):
            for columns in columnArray:
                schema=schema+"<ColumnSchema name='"+columns+"' />"
            
        schema=schema+"</TableSchema>"
        requestObj=request.Request(self.getBaseUrl()+str("/")+str(table_Name)+"/schema")
        requestObj.get_method = lambda: 'POST'
        requestObj.add_header("Content-length", len(schema))
        requestObj.add_header("Content-type","text/xml")
        response= request.urlopen(requestObj,data=(schema).encode(encoding='utf_8'))
        if(response.status==201):
            print("success in creating table")
        
        return response.status
    
    
    
        
        
        
    
    def drop_table(self,table_Name):
        requestObj=request.Request(self.getBaseUrl()+str("/")+str(table_Name)+"/schema")
        requestObj.get_method = lambda: 'DELETE'
        response= request.urlopen(requestObj)
        
        if(response.status==200):
            print("success in deleting table ")
        
        return response.status
    
    


        
    
    
    
#hwrapper=Hwrapper()
#hwrapper.connectionParameters("10.0.1.47","9300", False)
#hwrapper.setAcceptType("xml")
#list={"cf","cf1"}
#print(hwrapper.create_table("test1",list))

#print(hwrapper.drop_table("test1"))
