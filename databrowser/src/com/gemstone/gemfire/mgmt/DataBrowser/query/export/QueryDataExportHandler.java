/*========================================================================= 
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query.export;

import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnNotFoundException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnValueNotAvailableException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;

/**
 * This class provides an interface for defining application specific 
 * call-back handler during export of the query result.
 * 
 * @author Hrishi
 **/
public interface QueryDataExportHandler {
  public final String COLLECTION_NAME = "Collection";
  public final String COLLECTION_TYPE_NAME = "CollectionType";
  public final String COLLECTION_ELEMENT_NAME = "element";  
  public final String STRUCT_TYPE_NAME = "StructType";
  public final String STRUCT_ELEMENT_NAME = "Struct";
  public final String QUERY_RESULT_NAME = "QueryResult";
    
  public void handleStartDocument();
  
  public void handleEndDocument();
  
  public void handleStartCollectionElement(String name, IntrospectionResult metaInfo, Object element);
  
  public void handleEndCollectionElement(String name);
  
  public void handlePrimitiveType(String name, Class type, Object value);
  
  public void handleStartCompositeType(String name, IntrospectionResult metaInfo,
      Object value) throws ColumnNotFoundException,
      ColumnValueNotAvailableException;
  
  public void handleEndCompositeType(String name, IntrospectionResult metaInfo,
      Object value) throws ColumnNotFoundException,
      ColumnValueNotAvailableException;
  
  public void handleStartCollectionType(String name, String typeName, Object value) throws ColumnNotFoundException,
      ColumnValueNotAvailableException;

  public void handleEndCollectionType(String name, String typeName, Object value) throws ColumnNotFoundException,
      ColumnValueNotAvailableException;
  
  public void handleStartStructType(String name, IntrospectionResult metaInfo,
      Object val) throws ColumnNotFoundException,
      ColumnValueNotAvailableException;  
  
  public void handleEndStructType(String name, IntrospectionResult metaInfo,
      Object val) throws ColumnNotFoundException,
      ColumnValueNotAvailableException;
  
  public void handleStartPdxType(String name, IntrospectionResult metaInfo,
      Object value);

  public void handleEndPdxType(String name, IntrospectionResult metaInfo,
      Object value);
  
  public Object getResultDocument();

}
