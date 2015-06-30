/*========================================================================= 
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006 
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query.export;

import java.lang.reflect.Array;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnNotFoundException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnValueNotAvailableException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionRepository;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryUtil;
import com.gemstone.gemfire.mgmt.DataBrowser.query.ResultSet;
import com.gemstone.gemfire.mgmt.DataBrowser.query.internal.CollectionTypeResultImpl;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

/**
 * This class is responsible to navigate through the object graph of the Query
 * result and help to prepare an application specific model representation of
 * the graph. This is achieved by making call-backs to the application specific
 * handlers.
 * 
 * @author Hrishi
 **/
public class QueryResultExporter {

  private QueryDataExportHandler dataHandler;
  private int                    depthRequired;
  private int                    depth;

  public QueryResultExporter(QueryDataExportHandler dataHndlr, int depthReq) {
    dataHandler = dataHndlr;
    depthRequired = depthReq;
    depth = -1;
  }

  public int getDepthRequired() {
    return depthRequired;
  }

  public void resetDepth() {
    this.depth = 0;
  }

  public void exportData(QueryResult result) throws ColumnNotFoundException,
      ColumnValueNotAvailableException {

    if (this.dataHandler == null) {
      throw new IllegalStateException(
          "dataHandler is required to export data. Please provide valid dataHandler");
    }

    this.dataHandler.handleStartDocument();

    this.depth = -2;

    // Dummy type created so as to use handleCollectionType API for main
    // resultset as well.
    IntrospectionResult type = new CollectionTypeResultImpl(result
        .getQueryResult().getClass());
    IntrospectionRepository.singleton().addIntrospectionResult(
        result.getQueryResult().getClass(), type);

    Class<?> typeClass = type.getJavaType();
    handleCollectionType("QueryResult", typeClass, result);
    this.dataHandler.handleEndDocument();
  }

  public void exportObject(String name, IntrospectionResult metaInfo, Object obj)
      throws ColumnNotFoundException, ColumnValueNotAvailableException {

    // Increment the depth by 1 and check against the required depth.
    boolean shallow = isRequiredDepthReached(true);

    int resultType = metaInfo.getResultType();

    // If shallow = true and colClass is not primitive, then we will treat this
    // as a primitive type.
    // This will stop the subsequent navigation of the object tree.
    if ((shallow) || (resultType == IntrospectionResult.PRIMITIVE_TYPE_RESULT)) {
      Class<?> type = metaInfo.getColumnClass(0);
      Object value = obj;
      dataHandler.handlePrimitiveType(name, type, value);

    } else if (resultType == IntrospectionResult.COMPOSITE_TYPE_RESULT) {

      handleCompositeType(name, metaInfo, obj);

    } else if (resultType == IntrospectionResult.COLLECTION_TYPE_RESULT) {
      ResultSet temp = (ResultSet) metaInfo.getColumnValue(obj, 0);
      Class<?> type = metaInfo.getJavaType();
      handleCollectionType(name, type, temp);
      temp.close();

    } else if (resultType == IntrospectionResult.STRUCT_TYPE_RESULT) {
      handleStructType(name, metaInfo, obj);

    } else if (resultType == IntrospectionResult.MAP_TYPE_RESULT) {
      ResultSet temp = (ResultSet) metaInfo.getColumnValue(obj, 0);
      Class<?> type = metaInfo.getJavaType();
      handleCollectionType(name, type, temp);
      temp.close();
    } else if (resultType == IntrospectionResult.PDX_TYPE_RESULT) {
      handlePdxType(name, metaInfo, obj);
    }

    // Since we are complete, decrement the depth by one.
    decrDepth();
  }

  private void handleCompositeType(String nm, IntrospectionResult metaInfo,
      Object value) throws ColumnNotFoundException,
      ColumnValueNotAvailableException {

    // Increment the depth by 1 and check against the required depth.
    boolean shallow = isRequiredDepthReached(true);
    String name = (null != nm) ? nm : metaInfo.getJavaType().getSimpleName();

    // Start of the Composite object.
    this.dataHandler.handleStartCompositeType(name, metaInfo, value);

    // This object is not null. For a null object, there is no special
    // processing.
    if (value != null) {

      for (int k = 0; k < metaInfo.getColumnCount(); k++) {
        int      columnType = metaInfo.getColumnType(value, k);
        String   colName    = metaInfo.getColumnName(k);
        Object   colValue   = metaInfo.getColumnValue(value, k);
        Class<?> colClass   = metaInfo.getColumnClass(value, k);

        // If shallow = true and colClass is not primitive, then we will treat
        // this as a primitive type.
        // This will stop the subsequent navigation of the object tree.
        if ((shallow)
            || (columnType == IntrospectionResult.PRIMITIVE_TYPE_COLUMN)) {
          this.dataHandler.handlePrimitiveType(colName, colClass, colValue);

        } else if (columnType == IntrospectionResult.COMPOSITE_TYPE_COLUMN) {
          IntrospectionResult meta = IntrospectionRepository.singleton()
              .getIntrospectionResult(colClass);
          handleCompositeType(colName, meta, colValue);

        } else if (columnType == IntrospectionResult.COLLECTION_TYPE_COLUMN) {
          ResultSet temp = (ResultSet) colValue;
          handleCollectionType(colName, colClass, temp);
          temp.close();

        } else if (columnType == IntrospectionResult.MAP_TYPE_COLUMN) {
          ResultSet temp = (ResultSet) colValue;
          handleCollectionType(colName, colClass, temp);
          temp.close();
        } else if (columnType == IntrospectionResult.PDX_TYPE_COLUMN || 
            columnType == IntrospectionResult.PDX_OBJECT_TYPE_COLUMN) {//eventually goes to introspectAndHandleElement if it's not a PdxInstance
          IntrospectionResult colMetaInfo = null;
          try {
            colMetaInfo = IntrospectionRepository.singleton().introspectTypeByObject(colValue);
            handlePdxType(name, colMetaInfo, colValue);
          } catch (IntrospectionException e) {
            colMetaInfo = null;
            LogUtil.warning("Could not introspect: "+colValue+", skipping it..", e);
          }
        }
      }
    }

    this.dataHandler.handleEndCompositeType(name, metaInfo, value);

    // Since we are complete, decrement the depth by one.
    decrDepth();
  }

  // TODO MGH - added check to avoid a NPE later in the code.
  private void handleCollectionType(String name, Class<?> type, Object value)
      throws ColumnNotFoundException, ColumnValueNotAvailableException,
      NullPointerException {
    if (null == type) {
      throw new NullPointerException(
        "'type' argument for the collection to introspect is null, can't introspect.");
    }

    // For a null object, there is no special processing. Hence return
    // immediately.
    final String typeName = type.getCanonicalName();
    if (value == null) {
      this.dataHandler.handleStartCollectionType(name, typeName, value);
      this.dataHandler.handleEndCollectionType(name, typeName, value);
      return;
    }

    // Increment the depth by 1 and check against the required depth.
    boolean shallow = isRequiredDepthReached(true);

    if (!shallow) {
      boolean isArray = type.isArray();
      ResultSet result = (ResultSet) value;
      this.dataHandler.handleStartCollectionType(name, typeName, value);

      IntrospectionResult[] types = result.getIntrospectionResult();

      for (int i = 0; i < types.length; i++) {
        String elemName = QueryDataExportHandler.COLLECTION_ELEMENT_NAME;

        Collection<?> data = null;

        // In case of Arrays of primitive types, the collection can not have
        // elements of
        // multiple types. Here we get the complete results so as to accommodate
        // for the
        // Java auto-boxing feature. (We need to specify the correct element
        // type).
        if (isArray && types[i].getJavaType().isPrimitive()) {
          data = result.getQueryResult();

        } else {
          data = result.getQueryResult(types[i]);
        }

        Iterator<?> iter = data.iterator();
        while (iter.hasNext()) {
          Object val = iter.next();
          this.dataHandler
              .handleStartCollectionElement(elemName, types[i], val);
          exportObject(QueryDataExportHandler.COLLECTION_ELEMENT_NAME,
              types[i], val);
          this.dataHandler.handleEndCollectionElement(elemName);
        }
      }

      this.dataHandler.handleEndCollectionType(name, typeName, value);

    } else {
      this.dataHandler.handlePrimitiveType(name, type, value);
    }

    // Since we are complete, decrement the depth by one.
    decrDepth();
  }

  private void handleStructType(String nm, IntrospectionResult metaInfo,
      Object val) throws ColumnNotFoundException,
      ColumnValueNotAvailableException {

    // Increment the depth by 1 and check against the required depth.
    boolean shallow = isRequiredDepthReached(true);

    String name = (null != nm) ? nm
        : QueryDataExportHandler.STRUCT_ELEMENT_NAME;

    this.dataHandler.handleStartStructType(name, metaInfo, val);

    for (int k = 0; k < metaInfo.getColumnCount(); k++) {
      Object   colValue   = metaInfo.getColumnValue(val, k);
      int      columnType = metaInfo.getColumnType(val, k);
      String   colName    = metaInfo.getColumnName(k);
      Class<?> colClass   = metaInfo.getColumnClass(val, k);

      // If shallow = true and colClass is not primitive, then we will treat
      // this as a primitive type.
      // This will stop the subsequent navigation of the object tree.
      if ((shallow)
          || (columnType == IntrospectionResult.PRIMITIVE_TYPE_COLUMN)) {
        this.dataHandler.handlePrimitiveType(colName, colClass, colValue);

      } else if (columnType == IntrospectionResult.COMPOSITE_TYPE_COLUMN) {
        IntrospectionResult meta = IntrospectionRepository.singleton()
            .getIntrospectionResult(colClass);
        handleCompositeType(colName, meta, colValue);

      } // This will take care for objects of type (Collection, Map and Array).
      else if (columnType == IntrospectionResult.COLLECTION_TYPE_COLUMN) {
        ResultSet temp = (ResultSet) colValue;
        handleCollectionType(colName, colClass, temp);
        temp.close();

      } else if (columnType == IntrospectionResult.UNKNOWN_TYPE_COLUMN) {

        this.dataHandler.handlePrimitiveType(colName, colClass, colValue);
      } else if (columnType == IntrospectionResult.PDX_TYPE_COLUMN) {
        IntrospectionResult colMetaInfo = null;
        try {
          colMetaInfo = IntrospectionRepository.singleton().introspectTypeByObject(colValue);
          handlePdxType(colName, colMetaInfo, colValue);
        } catch (IntrospectionException e) {
          colMetaInfo = null;
          LogUtil.warning("Could not introspect: "+colValue+", skipping it..", e);
        }
      }
    }

    this.dataHandler.handleEndStructType(name, metaInfo, val);

    // Since we are complete, decrement the depth by one.
    decrDepth();
  }

  /**
   * Handles exporting for objects of GemFire type PdxInstanceImpl in the tree 
   * model.
   * 
   * @param name
   *          name to be used for start node for the given pdxInstanceObj
   * @param metaInfo
   *          IntrospectionResult for the given pdxInstanceObj
   * @param pdxInstanceObj
   *          PdxInstanceImpl object to be introspected
   * @throws ColumnNotFoundException
   *           if column/field at given index is not found
   * @throws ColumnValueNotAvailableException
   *           if value for column/field at given index is not found
   */
  /* PdxInstance could also be part of a Collection/Map, Struct, Composite. This
   * method (handlePdxType) is called from handleXXX methods for Struct &
   * Composite. The handleXXX method for Collection calls exportObject() for
   * individual elements and exportObject() calls handlePdxType if an element 
   * is a PdxInstance */
  private void handlePdxType(String name, IntrospectionResult metaInfo, Object pdxInstanceObj) 
    throws ColumnNotFoundException, ColumnValueNotAvailableException {
    // Increment the depth by 1 and check against the required depth.
    boolean shallow = isRequiredDepthReached(true);
    String nameOpt = (null != name) ? name : metaInfo.getJavaTypeName();

    // Start of the Composite object.
    this.dataHandler.handleStartPdxType(nameOpt, metaInfo, pdxInstanceObj);
    
    // This object is not null. For a null object, there is no special
    // processing.
    if (pdxInstanceObj != null) {

      for (int k = 0; k < metaInfo.getColumnCount(); k++) {
        int      columnType = metaInfo.getColumnType(pdxInstanceObj, k);
        String   colName    = metaInfo.getColumnName(k);
        Object   colValue   = metaInfo.getColumnValue(pdxInstanceObj, k);
        Class<?> colClass   = metaInfo.getColumnClass(pdxInstanceObj, k);
        
        // If shallow = true and colClass is not primitive, then we will treat
        // this as a primitive type.
        // This will stop the subsequent navigation of the object tree.
        if ((shallow) || 
            (columnType == IntrospectionResult.PRIMITIVE_TYPE_COLUMN)) {
          this.dataHandler.handlePrimitiveType(colName, colClass, colValue);
//          System.out.println("QueryResultExporter.handlePdxType()");
        } else if (columnType == IntrospectionResult.COLLECTION_TYPE_COLUMN) {
//          System.out.println("QueryResultExporter.handlePdxType() : isCollectionType1");
          handleSimpleCollectionType(colName, colClass, colValue);
        } else if (columnType == IntrospectionResult.PDX_OBJECT_TYPE_COLUMN) {
//          LogUtil.info("QueryResultExporter.handlePdxType() : value :: "+value);
          //for a PDX_OBJECT_TYPE_COLUMN ignore colClass & use colValue.getClass()
          Class<?> colValueClass = Object.class;
          if (colValue != null) {
            colValueClass = colValue.getClass();//TODO: Just to avoid NPE???? Make it better
          }
          if (QueryUtil.isPdxInstanceType(colValueClass)) {
            IntrospectionResult nestedObjMetaInfo;
            try {
              nestedObjMetaInfo = IntrospectionRepository.singleton().introspectTypeByObject(colValue);
              handlePdxType(colName, nestedObjMetaInfo, colValue);
            } catch (IntrospectionException e) {
              LogUtil.warning("Could not introspect: "+colValue+", skipping it..", e);
            }
          } else if (QueryUtil.isCollectionType(colValueClass)) {
            //it was probably a collection/array serialized as Object using writeObject()
            handleSimpleCollectionType(colName, colValueClass, colValue);
          } else if (QueryUtil.isMapType(colValueClass)) {
            //it was probably a map serialized as Object using writeObject()
            Map<?,?> valuesMap = (Map<?,?>) colValue;
            Set<?>   entrySet  = valuesMap.entrySet();
            handleSimpleCollectionType(colName, entrySet.getClass(), entrySet);
          } else {
            introspectAndHandleElement(colName, colValue);
          }
        }
      }
    }
    this.dataHandler.handleEndPdxType(name, metaInfo, pdxInstanceObj);

    // Since we are complete, decrement the depth by one.
    decrDepth();
  }

  /**
   * Handles exporting for the given collection/array/map and objects in it the
   * tree model. handleCollectionType works on ResultSet but this method uses
   * the actual collection/array/map.
   * 
   * @param name
   *          name to be used for start node for the given collectionObj
   * @param type
   *          type (java.lang.Class) of the given collection
   * @param collectionObj
   *          instance of one of collection/array/map
   * @throws ColumnNotFoundException
   *           if column/field at given index is not found
   * @throws ColumnValueNotAvailableException
   *           if value for column/field at given index is not found
   */
  private void handleSimpleCollectionType(String name, Class<?> type, Object collectionObj) 
      throws ColumnNotFoundException, ColumnValueNotAvailableException {
    if (null == type) {
      throw new NullPointerException(
        "'type' for the collection to introspect is null, can't introspect.");
    }

    // For a null object, there is no special processing. Hence return
    // immediately.
    final String typeName = type.getCanonicalName();
    if (collectionObj == null) {
      this.dataHandler.handleStartCollectionType(name, typeName, collectionObj);
      this.dataHandler.handleEndCollectionType(name, typeName, collectionObj);
      return;
    }

    // Increment the depth by 1 and check against the required depth.
    boolean shallow = isRequiredDepthReached(true);
    
    if (!shallow) {
      boolean isArray = type.isArray();
      this.dataHandler.handleStartCollectionType(name, typeName, collectionObj);

      if (isArray) {
        Class<?> componentType = type.getComponentType();
        int length = Array.getLength(collectionObj);
        if (componentType.isPrimitive()) {
          //Not using exportObject() to handle here, as it's simple for primitive types
          if (boolean.class == componentType) {
            for (int i = 0; i < length; i++) {
              this.dataHandler.handlePrimitiveType(QueryDataExportHandler.COLLECTION_ELEMENT_NAME, componentType, Array.getBoolean(collectionObj, i));
            }
          } else if (byte.class == componentType) {
            for (int i = 0; i < length; i++) {
              this.dataHandler.handlePrimitiveType(QueryDataExportHandler.COLLECTION_ELEMENT_NAME, componentType, Array.getByte(collectionObj, i));
            }
          } else if (char.class == componentType) {
            for (int i = 0; i < length; i++) {
              this.dataHandler.handlePrimitiveType(QueryDataExportHandler.COLLECTION_ELEMENT_NAME, componentType, Array.getChar(collectionObj, i));
            }
          } else if (short.class == componentType) {
            for (int i = 0; i < length; i++) {
              this.dataHandler.handlePrimitiveType(QueryDataExportHandler.COLLECTION_ELEMENT_NAME, componentType, Array.getShort(collectionObj, i));
            }
          } else if (int.class == componentType) {
            for (int i = 0; i < length; i++) {
              this.dataHandler.handlePrimitiveType(QueryDataExportHandler.COLLECTION_ELEMENT_NAME, componentType, Array.getInt(collectionObj, i));
            }
          } else if (long.class == componentType) {
            for (int i = 0; i < length; i++) {
              this.dataHandler.handlePrimitiveType(QueryDataExportHandler.COLLECTION_ELEMENT_NAME, componentType, Array.getLong(collectionObj, i));
            }
          } else if (float.class == componentType) {
            for (int i = 0; i < length; i++) {
              this.dataHandler.handlePrimitiveType(QueryDataExportHandler.COLLECTION_ELEMENT_NAME, componentType, Array.getFloat(collectionObj, i));
            }
          } else if (double.class == componentType) {
            for (int i = 0; i < length; i++) {
              this.dataHandler.handlePrimitiveType(QueryDataExportHandler.COLLECTION_ELEMENT_NAME, componentType, Array.getDouble(collectionObj, i));
            }
          }
        } else {
          Object[] arr = (Object[]) collectionObj;
          for (int i = 0; i < length; i++) {
            introspectAndHandleElement(QueryDataExportHandler.COLLECTION_ELEMENT_NAME, arr[i]);
          }
        }
      } else if (QueryUtil.isCollectionType(type)) {//double check for collection type?
        Collection<?> valuesCollection = (Collection<?>) collectionObj;
        for (Iterator<?> it = valuesCollection.iterator(); it.hasNext();) {
          introspectAndHandleElement(QueryDataExportHandler.COLLECTION_ELEMENT_NAME, it.next());
        }
      }
      this.dataHandler.handleEndCollectionType(name, typeName, collectionObj);
    } else {
      this.dataHandler.handlePrimitiveType(name, type, collectionObj);
    }
    
  }

  /**
   * Introspects and handles exporting for the given data object in it the tree
   * model.
   * 
   * @param data
   *          object to be introspected
   * @throws ColumnNotFoundException
   *           if column/field at given index is not found
   * @throws ColumnValueNotAvailableException
   *           if value for column/field at given index is not found
   */
  private void introspectAndHandleElement(String treeNodeName, Object data) 
      throws ColumnNotFoundException, ColumnValueNotAvailableException {
    IntrospectionRepository singleton = IntrospectionRepository.singleton();
    IntrospectionResult dataElementMetaInfo = null;
    Object dataElement = data != null ? data : new Object();
    try {
      dataElementMetaInfo = singleton.introspectTypeByObject(dataElement);
      exportObject(treeNodeName, dataElementMetaInfo, dataElement);
    } catch (IntrospectionException e) {
      LogUtil.warning("Could not introspect: "+data+", skipping it..", e);
      //return after logging exception
    }
  }

  private boolean isRequiredDepthReached(boolean incrDepth) {
    if (incrDepth) {
      depth = depth + 1;
    }

    if (depthRequired == -1) {
      return false;
    }

    return (depth < depthRequired) ? false : true;
  }

  private void decrDepth() {
    if (depth > 0) {
      depth = depth - 1;
    }
  }

}
