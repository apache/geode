/*========================================================================= 
 * (c)Copyright 2002-2009, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query.export;

import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnNotFoundException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;

/**
 * This class defines an interface for defining application specific call-back
 * handler for preparing a model for query schema.
 * 
 * @author Hrishi
 **/
public interface QuerySchemaExportHandler {
  
  public String getSchema();
  
  public void handleStartSchema();
  
  public void handleEndSchema();

  public void handlePrimitiveType(IntrospectionResult metaInfo)
      throws ColumnNotFoundException;

  public void handleCompositeType(IntrospectionResult metaInfo)
      throws ColumnNotFoundException;

  public void handleCollectionType(IntrospectionResult metaInfo)
      throws ColumnNotFoundException;

  public void handleMapType(IntrospectionResult metaInfo)
      throws ColumnNotFoundException;

  public void handleStructType(IntrospectionResult metaInfo)
      throws ColumnNotFoundException;

}
