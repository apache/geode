/*=========================================================================
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.ui.tree;

import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.export.QueryDataExportHandler;


public class ObjectTreeModelImpl implements QueryDataExportHandler {

  private ObjectImage root;
  private ObjectImage current;

  public Object getResultDocument() {
    return root;
  }
  
  public void handleStartDocument() {
  }
  
  public void handleEndDocument() {
  }
 
  public void handleStartCollectionElement(String name, IntrospectionResult metaInfo, Object element) {
  }

  public void handleEndCollectionElement(String name) {
  }


  public void handlePrimitiveType(String name, Class type, Object value) {
    ObjectImage temp = new ObjectImage(name, IntrospectionResult.PRIMITIVE_TYPE_RESULT, value);
    temp.setTypeName(type.getCanonicalName());
    
    if(root == null) {
      current = root = temp;      
    } else {
      current.addChild(temp);
      temp.setParent(current);
    }    
  }

  public void handleStartCollectionType(String name, String typeName, Object value) {
    ObjectImage temp = new ObjectImage(name, IntrospectionResult.COLLECTION_TYPE_RESULT, value);
    temp.setTypeName(typeName);
    
    if(root == null) {
     root = temp;
    
    } else {
      current.addChild(temp);
      temp.setParent(current);
    }
    
    current = temp;
  }

  public void handleEndCollectionType(String name, String typeName, Object value) {
    if(current != root) {
      current = current.getParent();      
    }  
  }
  
  public void handleStartCompositeType(String name, IntrospectionResult metaInfo, Object value) {
    ObjectImage temp = new ObjectImage(name, metaInfo.getResultType(), value);
    temp.setTypeName(metaInfo.getJavaType().getCanonicalName());

    if(root == null) {
      root = temp;
    } else {
      current.addChild(temp);
      temp.setParent(current);    
    }
    
    current = temp;
  }
  
  public void handleEndCompositeType(String name, IntrospectionResult metaInfo, Object value) {
    if(current != root) {
      current = current.getParent();  
    }    
  }

  public void handleStartStructType(String name, IntrospectionResult metaInfo, Object val)  {
    ObjectImage temp = new ObjectImage(name, metaInfo.getResultType(), val);
    temp.setTypeName(metaInfo.getJavaType().getCanonicalName());
    
    if(root == null) {
      root = temp;
    } else {
      current.addChild(temp);
      temp.setParent(current);    
    }
 
    current = temp;
  }
  
  public void handleEndStructType(String name, IntrospectionResult metaInfo, Object val) {
    if(current != root) {
      current = current.getParent();  
    }  
  }

  public void handleStartPdxType(String name, IntrospectionResult metaInfo,
      Object value) {
    ObjectImage temp = new ObjectImage(name, metaInfo.getResultType(), value);
    temp.setTypeName(metaInfo.getJavaTypeName());

    if(root == null) {
      root = temp;
    } else {
      current.addChild(temp);
      temp.setParent(current);    
    }
    
    current = temp;
  }

  public void handleEndPdxType(String name, IntrospectionResult metaInfo,
      Object value) {
    if(current != root) {
      current = current.getParent();  
    }    
  }
}
