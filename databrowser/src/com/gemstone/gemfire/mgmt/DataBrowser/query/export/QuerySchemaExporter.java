/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query.export;

import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnNotFoundException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionRepository;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryResult;

/**
 * This class is responsible to iterate through the introspection results of the query result
 * and help to prepare an application specific model representation of the same. This
 * is achieved by making call-backs to the application specific handlers.
 * 
 * @author Hrishi
 **/
public class QuerySchemaExporter {
  
  public static void exportSchema(QuerySchemaExportHandler schemaHandler, QueryResult result) throws ColumnNotFoundException {
    if(schemaHandler == null) {
      throw new IllegalStateException("schemaHandler is required to export schema. Please provide valid schemaHandler");
    }
    
    schemaHandler.handleStartSchema();
    
    IntrospectionResult[] info = result.getIntrospectionResult();
    for(int i = 0; i < info.length ; i++) {
     if(info[i].getResultType() == IntrospectionResult.STRUCT_TYPE_RESULT) {
       schemaHandler.handleStructType(info[i]);
       break;    
     }
    }
  
    info = IntrospectionRepository.singleton().getIntrospectionResultInfo();
    
    for(int i = 0 ; i < info.length ; i++) {
      IntrospectionResult metaInfo = info[i];
      int resultType = metaInfo.getResultType();
      
      switch (resultType) {
        case IntrospectionResult.COMPOSITE_TYPE_RESULT: {
          schemaHandler.handleCompositeType(metaInfo);
          break;
        }

        case IntrospectionResult.COLLECTION_TYPE_RESULT: {
          schemaHandler.handleCollectionType(metaInfo);
          break;
        }
        
        case IntrospectionResult.MAP_TYPE_RESULT: {
          schemaHandler.handleMapType(metaInfo);
          break;
        }
        
        default: //Not implemented.
          break;
      }  
    }
    
    schemaHandler.handleEndSchema();
  }
}
