/*=========================================================================
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query.internal;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnNotFoundException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnValueNotAvailableException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionRepository;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryExecutionException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryUtil;

/**
 * This class provides functionality to introspect a given class and perform
 * necessary object-to-relational transformations to flatten the objects.
 *
 * @author Hrishi
 **/
public final class ObjectTypeResultImpl implements IntrospectionResult {

  private Class<?>           type_ = null;
  private List<ObjectColumn> fields_ = null;

  public ObjectTypeResultImpl(Class<?> ty) {
    type_ = ty;
    fields_ = new ArrayList<ObjectColumn>();
  }
  
  public int getResultType() {
    return COMPOSITE_TYPE_RESULT;
  }

  void setObjectColumns(Collection<ObjectColumn> columns) {
    fields_.clear();
    fields_.addAll(columns);
  }
  
  public ObjectColumn getObjectColumn(int index) {
    if (fields_.size() > index) {
      return fields_.get(index);
    }
    
    return null;
  }

  public Class getColumnClass(int index) throws ColumnNotFoundException {
    if (fields_.size() > index) {
      ObjectColumn col = fields_.get(index);
      return col.getFieldType();
    }
    throw new ColumnNotFoundException("Column not available for index :"
        + index);
  }
  
  public Class getColumnClass(Object tuple, int index)
      throws ColumnNotFoundException {
    int type = getColumnType(index);
    ObjectColumn col = fields_.get(index);
    
    if(type == IntrospectionResult.UNKNOWN_TYPE_COLUMN && (tuple != null)) {
      try {
        Object val = col.getValue(tuple);
        if(val != null) {
          return val.getClass();
        }
      }
      catch (ColumnValueNotAvailableException e) {
        throw new ColumnNotFoundException(e);
      }
    }
    
    return col.getFieldType();
  }

  public Class<?> getJavaType() {
    return type_;
  }

  public int getColumnCount() {
    return fields_.size();
  }

  public String getColumnName(int index) throws ColumnNotFoundException {
    if (fields_.size() > index) {
      ObjectColumn col = fields_.get(index);
      return col.getFieldName();
    }
    throw new ColumnNotFoundException("Column not available for index :"
        + index);
  }

  public int getColumnIndex(String name) throws ColumnNotFoundException {
    for (ObjectColumn column : fields_) {
      if (column.getFieldName().equals(name)) {
        return fields_.indexOf(column);
      }
    }

    throw new ColumnNotFoundException("Column not available for name :" + name);
  }

  public int getColumnType(int index) throws ColumnNotFoundException {
    // TODO: Hrishi it is a zero based index right?
    /*
    if (index > fields_.size()) {
      throw new ColumnNotFoundException("Column not available for index :"
          + index);
    }

    ObjectColumn col = fields_.get(index);
     
    if (QueryUtil.isPrimitiveType(col.getFieldType()))
      return IntrospectionResult.PRIMITIVE_TYPE_COLUMN;
    else if (QueryUtil.isCompositeType(col.getFieldType())) {
      if(col.isFinal())
        return IntrospectionResult.COMPOSITE_TYPE_COLUMN;

      return IntrospectionResult.UNKNOWN_TYPE_COLUMN;  
    } else if (QueryUtil.isCollectionType(col.getFieldType()))
      return IntrospectionResult.COLLECTION_TYPE_COLUMN;
    else if (QueryUtil.isMapType(col.getFieldType()))
      return IntrospectionResult.MAP_TYPE_COLUMN;

    throw new ColumnNotFoundException(
        "Unable to detect the column type for index :" + index);
    
    */
    final ObjectColumn col;
    int iRes = IntrospectionResult.UNDEFINED;
    String sXptnMsg = null;
    
    try {
      col = fields_.get(index);
      if (QueryUtil.isPrimitiveType(col.getFieldType()))
        iRes = IntrospectionResult.PRIMITIVE_TYPE_COLUMN;
      else if (QueryUtil.isCompositeType(col.getFieldType())) {
        if(col.isFinal())
          iRes = IntrospectionResult.COMPOSITE_TYPE_COLUMN;
        else
          iRes = IntrospectionResult.UNKNOWN_TYPE_COLUMN;  
      } else if (QueryUtil.isCollectionType(col.getFieldType()))
        iRes = IntrospectionResult.COLLECTION_TYPE_COLUMN;
      else if (QueryUtil.isMapType(col.getFieldType()))
        iRes = IntrospectionResult.MAP_TYPE_COLUMN;
      
      if( IntrospectionResult.UNDEFINED == iRes ) {
        sXptnMsg = "Unable to detect the column type for index :" + index;
      }
    }
    catch( IndexOutOfBoundsException xptn ) {
      sXptnMsg = "Column not available for index : " + index;
    }
    
    if( null != sXptnMsg ) {
      throw new ColumnNotFoundException( sXptnMsg );
    }
    
    return iRes;
  }
  
  public int getColumnType(Object tuple, int index)
      throws ColumnNotFoundException {
   
    int type = getColumnType(index);
    ObjectColumn col = fields_.get(index);
    
    if(type == IntrospectionResult.UNKNOWN_TYPE_COLUMN && tuple != null) {

        try {
          Object value = col.getValue(tuple);
          if(value != null) {
            IntrospectionResult result = IntrospectionRepository.singleton().introspectTypeByObject(value);
            switch(result.getResultType()) {
              case PRIMITIVE_TYPE_RESULT : return PRIMITIVE_TYPE_COLUMN;
              case COMPOSITE_TYPE_RESULT : return COMPOSITE_TYPE_COLUMN;
              case COLLECTION_TYPE_RESULT : return COLLECTION_TYPE_COLUMN;
              case MAP_TYPE_RESULT : return MAP_TYPE_COLUMN;
              case PDX_TYPE_RESULT : return PDX_TYPE_COLUMN;
            }
          }
        }
        catch (ColumnValueNotAvailableException e) {
          throw new ColumnNotFoundException(e);    
        } catch (IntrospectionException e) {
          throw new ColumnNotFoundException(e);              
        } 
    }
    
    return type;
  }

  public Object getColumnValue(Object tuple, int index)
      throws ColumnNotFoundException, ColumnValueNotAvailableException {
    if (fields_.size() > index) {
      ObjectColumn col = fields_.get(index);
      int type = getColumnType(tuple, index);

      switch (type) {
        case COMPOSITE_TYPE_COLUMN: {
          //In this case we don't need to make sure that the type of the field is
          //introspected, because that is already done in ObjectIntrospector class during init. 
          return col.getValue(tuple);
        }
  
        case COLLECTION_TYPE_COLUMN: {
       
          if(col.getFieldType().isArray()) {
            try {
              return QueryUtil.introspectArrayType(col.getFieldType(), col.getValue(tuple));
            }
            catch (IntrospectionException e) {
              throw new ColumnValueNotAvailableException(e);
            }
          }
          
          Collection< Object > result = (Collection<Object>) col.getValue(tuple);
          try {
            return QueryUtil.introspectCollectionObject(result);
          } catch (QueryExecutionException e) {
            throw new ColumnValueNotAvailableException(e);
          }
        }
        
        case MAP_TYPE_COLUMN: {
          Map result = (Map)col.getValue(tuple);
          Collection temp = null;
          if(result != null)          
            temp = result.entrySet();   
          
          try {
            return QueryUtil.introspectCollectionObject(temp);
          }
          catch (QueryExecutionException e) {
            throw new ColumnValueNotAvailableException(e);
          }
        }
        
        default: return col.getValue(tuple);
        }
    }
    throw new ColumnNotFoundException("Column not available for index :"
        + index);
  }
  
  public String getJavaTypeName() {
    Class<?> javaType = getJavaType();
    
    return javaType != null ? javaType.getName() : null;
  }
  
  public boolean isCompatible(Object data) {
    //checks for equality by reference not assignable. Using == assuming single 
    //instance of a class
    return data != null && getJavaType() == data.getClass();
  }
}
