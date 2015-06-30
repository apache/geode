/*=========================================================================
 * (c)Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 * 1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006
 * All Rights Reserved.
 * =======================================================================*/
package com.gemstone.gemfire.mgmt.DataBrowser.query.internal;

import java.util.Collection;
import java.util.Map;

import com.gemstone.gemfire.cache.query.Struct;
import com.gemstone.gemfire.cache.query.types.StructType;
import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnNotFoundException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnValueNotAvailableException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionRepository;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryExecutionException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.QueryUtil;


/**
 * This class provides functionality to introspect the GemFire Struct type of
 * results and convert them into standard Introspection result.
 *
 * @author Hrishi
 **/
public class StructTypeResultImpl implements IntrospectionResult {

  private StructType type_;

  public StructTypeResultImpl(StructType ty) {
    type_ = ty;
  }

  public Class<?> getJavaType() {
    return StructType.class;
  }
  
  public int getResultType() {
    return STRUCT_TYPE_RESULT;
  }
  
  public Class getColumnClass(int index) throws ColumnNotFoundException {
    if (this.type_.getFieldTypes().length > index) {
      //Right now GemFire does not return specific type at all. Hence the hard-coding.
      //ObjectType objType = this.type_.getFieldTypes()[index];
      //return objType.getSimpleClassName();
      return Object.class;
    }
    throw new ColumnNotFoundException("Column not available for index :"
        + index);
  }
  
  public Class getColumnClass(Object tuple, int index)
      throws ColumnNotFoundException {
    
    if (!(tuple instanceof Struct)) {
      throw new ColumnNotFoundException("The parameter should be of type Struct.");
    }
    
    Struct val = (Struct)tuple;
    int count = this.type_.getFieldNames().length;
        
    if(count > index) {
      Object result = val.getFieldValues()[index];
      Class type = (result != null) ? result.getClass() : java.lang.Object.class;
      
      // In case of Struct type of results, the type of the field can change depending upon the
      //actual tuple. This information is not available in the StructType object. Hence make sure
      //we understand the all the types properly.
      try {
        IntrospectionRepository.singleton().introspectType(type);
      }
      catch (IntrospectionException e) {
        throw new ColumnNotFoundException(e);              
      }     
      
      return  type;    
    }
    
    throw new ColumnNotFoundException("The column for this index could not be found"); 
  }

  public int getColumnCount() {
    return this.type_.getFieldNames().length;
  }

  public String getColumnName(int index) throws ColumnNotFoundException {
    if (this.type_.getFieldNames().length > index) {
      return this.type_.getFieldNames()[index];
    }
    throw new ColumnNotFoundException("Column not available for index :"
        + index);
  }

  public int getColumnIndex(String name) throws ColumnNotFoundException {
    return this.type_.getFieldIndex(name);
  }

  public int getColumnType(int index) throws ColumnNotFoundException {
    int count = this.type_.getFieldNames().length;

    if (count > index) {
      return UNKNOWN_TYPE_COLUMN;
    }

    throw new ColumnNotFoundException("Column not available for index :"
        + index);
  }
  
  public int getColumnType(Object tuple, int index)
      throws ColumnNotFoundException {
    
    if (!(tuple instanceof Struct)) {
      throw new ColumnNotFoundException("The parameter should be of type Struct.");
    }
    
    Struct val = (Struct)tuple;
    int count = this.type_.getFieldNames().length;
    
    if (count > index) {
      Object result = val.getFieldValues()[index];
      if(result != null) {
        Class type = result.getClass();        
        if (QueryUtil.isPrimitiveType(type))
          return IntrospectionResult.PRIMITIVE_TYPE_COLUMN;
        else if (QueryUtil.isCompositeType(type))
          return IntrospectionResult.COMPOSITE_TYPE_COLUMN;
        else if (QueryUtil.isCollectionType(type))
          return IntrospectionResult.COLLECTION_TYPE_COLUMN;
        else if (QueryUtil.isMapType(type))
          return IntrospectionResult.MAP_TYPE_COLUMN;
        else if (QueryUtil.isPdxInstanceType(type))
          return IntrospectionResult.PDX_TYPE_COLUMN;  
      }
      
      return IntrospectionResult.UNKNOWN_TYPE_COLUMN;
      
    }
        
    throw new ColumnNotFoundException("The column for this index could not be found");
  }

  public Object getColumnValue(Object tuple, int index)
      throws ColumnNotFoundException, ColumnValueNotAvailableException {
    if (!(tuple instanceof Struct)) {
      throw new ColumnValueNotAvailableException(
          "The parameter should be of type Struct.");
    }

    Struct struct = (Struct) tuple;
    if (struct.getFieldValues().length > index) {
      int type = getColumnType(tuple, index);
      
      switch(type) {
        case COLLECTION_TYPE_COLUMN : {
          Class javaType = getColumnClass(tuple, index);
          
          if(javaType.isArray()) {
            try {
              return QueryUtil.introspectArrayType(javaType, struct.getFieldValues()[index]);
            }
            catch (IntrospectionException e) {
              throw new ColumnValueNotAvailableException(e);
            }
          }
          
          Collection result = (Collection)struct.getFieldValues()[index];

          try {
            return QueryUtil.introspectCollectionObject(result);
          } catch (QueryExecutionException e) {
            throw new ColumnValueNotAvailableException(e);
          }          
        }
        
        case MAP_TYPE_COLUMN : {
          Map result = (Map)struct.getFieldValues()[index];
          Collection temp = null;
          if(result != null)
            temp = result.entrySet();

          try {
            return QueryUtil.introspectCollectionObject(temp);
          } catch (QueryExecutionException e) {
            throw new ColumnValueNotAvailableException(e);
          }  
        }
        
        default : {
          return struct.getFieldValues()[index];
          
        }
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
