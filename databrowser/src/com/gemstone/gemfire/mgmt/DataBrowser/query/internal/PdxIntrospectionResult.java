/*
 * =========================================================================
 * Copyright (c) 2002-2012 VMware, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. VMware products are covered by
 * more patents listed at http://www.vmware.com/go/patents.
 * All Rights Reserved.
 * ========================================================================
 */
package com.gemstone.gemfire.mgmt.DataBrowser.query.internal;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnNotFoundException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnValueNotAvailableException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;
import com.gemstone.gemfire.pdx.internal.PdxField;
import com.gemstone.gemfire.pdx.internal.PdxInstanceImpl;
import com.gemstone.gemfire.pdx.internal.PdxType;

/**
 * This class is used to gather & provide introspection meta-data about Objects 
 * which are of GemFire type PdxInstance - PdxInstanceImpl to be specific. The 
 * introspection is done using other classes PdxType, FieldType & PdxField.
 * 
 * @author abhishek
 * @since 1.3 (GemFire 6.6)
 */
public class PdxIntrospectionResult implements IntrospectionResult {
  /* stores the PdxInfoType objects for the known PdxInstanceImpl objects */
  private static final Map<PdxInstanceImpl, PdxInfoType> pdxInfos = new HashMap<PdxInstanceImpl, PdxInfoType>();
  
  /* PdxType for the given PdxInstanceImpl */
  private PdxType      pdxType;
  /* List of Field Names in the given PdxInstanceImpl */
  private List<String> fieldNames;
  /* Local (Data Browser side) representation of the GemFire PdxType */
  private PdxInfoType  pdxInfoType;

  /* Class that represents FieldType enum in GemFire. */
  private FieldTypeAccessor fieldTypeAccessor;

  //Note: used by PdxTypesIntrospectionResult
  public PdxIntrospectionResult(Object pdxInstanceArg, String fieldTypeClass) {
    PdxInstanceImpl pdxInstance;
    if (pdxInstanceArg instanceof PdxInstanceImpl) {
      pdxInstance = (PdxInstanceImpl) pdxInstanceArg;
    } else {
      throw new IllegalArgumentException("Only PdxInstance class is expected.");
    }

    try {
      fieldTypeAccessor = new FieldTypeAccessor(Class.forName(fieldTypeClass));
    } catch (ClassNotFoundException e) {
      LogUtil.error("Could not load/find required classes.", e);
    }
    
    pdxType     = pdxInstance.getPdxType();
    fieldNames  = pdxType.getFieldNames();
    pdxInfoType = new PdxInfoType(pdxType.getClassName(), pdxType.getTypeId());
    pdxInfos.put(pdxInstance, pdxInfoType);
  }

  public Class<?> getColumnClass(int index) throws ColumnNotFoundException {
    PdxField pdxField = pdxType.getPdxFieldByIndex(index);
    Object fieldType = pdxField.getFieldType();
    Class<?> columnClass = null;
    
    if (fieldType.equals(fieldTypeAccessor.getBoolean())) {
      columnClass = boolean.class;
    } else if (fieldType.equals(fieldTypeAccessor.getByte())) {
      columnClass = byte.class;
    } else if (fieldType.equals(fieldTypeAccessor.getChar())) {
      columnClass = char.class;
    } else if (fieldType.equals(fieldTypeAccessor.getShort())) {
      columnClass = short.class;
    } else if (fieldType.equals(fieldTypeAccessor.getInt())) {
      columnClass = int.class;
    } else if (fieldType.equals(fieldTypeAccessor.getLong())) {
      columnClass = long.class;
    } else if (fieldType.equals(fieldTypeAccessor.getFloat())) {
      columnClass = float.class;
    } else if (fieldType.equals(fieldTypeAccessor.getDouble())) {
      columnClass = double.class;
    } else if (fieldType.equals(fieldTypeAccessor.getDate())) {
//      columnClass = String.class;//TODO: should this be a string
      columnClass = java.util.Date.class;
    } else if (fieldType.equals(fieldTypeAccessor.getString())) {
      columnClass = String.class;
    } else if (fieldType.equals(fieldTypeAccessor.getBooleanArray())) {
      columnClass = boolean[].class;
    } else if (fieldType.equals(fieldTypeAccessor.getCharArray())) {
      columnClass = char[].class;
    } else if (fieldType.equals(fieldTypeAccessor.getByteArray())) {
      columnClass = byte[].class;
    } else if (fieldType.equals(fieldTypeAccessor.getShortArray())) {
      columnClass = short[].class;
    } else if (fieldType.equals(fieldTypeAccessor.getIntArray())) {
      columnClass = int[].class;
    } else if (fieldType.equals(fieldTypeAccessor.getLongArray())) {
      columnClass = long[].class;
    } else if (fieldType.equals(fieldTypeAccessor.getFloatArray())) {
      columnClass = float[].class;
    } else if (fieldType.equals(fieldTypeAccessor.getDoubleArray())) {
      columnClass = double[].class;
    } else if (fieldType.equals(fieldTypeAccessor.getStringArray())) {
      columnClass = String[].class;
    } else if (fieldType.equals(fieldTypeAccessor.getObjectArray())) {
      columnClass = Object[].class;
    } else if (fieldType.equals(fieldTypeAccessor.getArrayOfByteArrays())) {
      columnClass = byte[][].class;
    }else if (fieldType.equals(fieldTypeAccessor.getObject())) {
      columnClass = Object.class;
    } 
    
    if (columnClass == null) {
      throw new ColumnNotFoundException("PDX Type Information for Column not available for index :" + index);
    }

    return columnClass;
  }

  public Class<?> getColumnClass(Object tuple, int index)
      throws ColumnNotFoundException {
    return getColumnClass(index);
  }

  public int getColumnCount() {
    return fieldNames.size();
  }

  public int getColumnIndex(String name) throws ColumnNotFoundException {
    return fieldNames.indexOf(name);
  }

  public String getColumnName(int index) throws ColumnNotFoundException {
    return fieldNames.get(index);
  }

  public int getColumnType(int index) throws ColumnNotFoundException {
    int iRes = IntrospectionResult.UNDEFINED;
    String sXptnMsg = null;
    
    try {
      PdxField pdxField = pdxType.getPdxFieldByIndex(index);
      Object fieldType = pdxField.getFieldType();
//      System.out.println("PdxIntrospectionResult.getColumnType() : pdxField :: "+pdxField);
//      System.out.println("PdxIntrospectionResult.getColumnType() : fieldType :: "+fieldType);
      boolean isPrimitive = fieldType.equals(fieldTypeAccessor.getBoolean()) ||  
                            fieldType.equals(fieldTypeAccessor.getByte()) || 
                            fieldType.equals(fieldTypeAccessor.getChar()) || 
                            fieldType.equals(fieldTypeAccessor.getShort()) ||
                            fieldType.equals(fieldTypeAccessor.getInt()) || 
                            fieldType.equals(fieldTypeAccessor.getLong()) ||
                            fieldType.equals(fieldTypeAccessor.getFloat()) || 
                            fieldType.equals(fieldTypeAccessor.getDouble()) ||
                            fieldType.equals(fieldTypeAccessor.getString()) || 
                            fieldType.equals(fieldTypeAccessor.getDate()); //TODO: should date be here as primitive?
      
      boolean isCollection = fieldType.equals(fieldTypeAccessor.getBooleanArray()) ||  
                            fieldType.equals(fieldTypeAccessor.getCharArray()) || 
                            fieldType.equals(fieldTypeAccessor.getByteArray()) || 
                            fieldType.equals(fieldTypeAccessor.getShortArray()) ||
                            fieldType.equals(fieldTypeAccessor.getIntArray()) || 
                            fieldType.equals(fieldTypeAccessor.getLongArray()) ||
                            fieldType.equals(fieldTypeAccessor.getFloatArray()) || 
                            fieldType.equals(fieldTypeAccessor.getDoubleArray()) ||
                            fieldType.equals(fieldTypeAccessor.getStringArray()) ||
                            fieldType.equals(fieldTypeAccessor.getObjectArray()) ||
                            fieldType.equals(fieldTypeAccessor.getArrayOfByteArrays());
      
      boolean isObject = fieldType.equals(fieldTypeAccessor.getObject());
      
      if (isPrimitive) {
        iRes = IntrospectionResult.PRIMITIVE_TYPE_COLUMN;
      } else if (isCollection) {
        iRes = IntrospectionResult.COLLECTION_TYPE_COLUMN;
      } else if (isObject) {
        iRes = IntrospectionResult.PDX_OBJECT_TYPE_COLUMN;
      }
      
      if( IntrospectionResult.UNDEFINED == iRes ) {
        sXptnMsg = "Unable to detect the column type for index :" + index;
      }
    } catch( IndexOutOfBoundsException xptn ) {
      sXptnMsg = "Column not available for index : " + index;
    }
    
    if( null != sXptnMsg ) {
      throw new ColumnNotFoundException( sXptnMsg );
    }
    
    return iRes;
  }

  public int getColumnType(Object tuple, int index)
      throws ColumnNotFoundException {
    return getColumnType(index);
  }

  public Object getColumnValue(Object tuple, int index)
      throws ColumnNotFoundException, ColumnValueNotAvailableException {
    return ((PdxInstanceImpl)tuple).getField(getColumnName(index));
  }

  public Class<?> getJavaType() {
    return PdxInstanceImpl.class;//TODO: should there be a dummy PdxInstance class.
  }
  
  public String getJavaTypeName() {
    return pdxInfoType.getDisplayName();
  }

  public int getResultType() {
    return PDX_TYPE_RESULT;
  }

  /**
   * Whether the given Object is compatible with this IntrospectionResult. Here,
   * we check whether the type of the actual object is same as that of the given
   * object. The PdxInstance have the meta info about the actual object.
   * 
   * @param data
   *          Object to check for compatibility
   * @return true if type of the given Object is same as the type for which this
   *         IntrospectionResult is created, false otherwise
   *         
   * @see IntrospectionResult#isCompatible(Object)        
   */
  public boolean isCompatible(Object data) {
    //checks for equality not assignable
    return pdxInfoType.equals(getPdxInfoType(data));
  }

  /**
   * Create or return existing PdxInfoType for the given object. If given object
   * - 'pdxInstanceArg' - is not of the type PdxInstanceImpl, it returns null.
   * 
   * @param pdxInstanceArg
   *          PdxInstance for which PdxInfoType is to be retrieved
   * @return new or existing PdxInfoType for the given object.
   */
  //Note: used by PdxTypesIntrospectionResult
  static PdxInfoType getPdxInfoType(Object pdxInstanceArg) {
    PdxInfoType storedPdxInfoType = null;
    
    if (pdxInstanceArg instanceof PdxInstanceImpl) {
      PdxInstanceImpl pdxInstance = (PdxInstanceImpl) pdxInstanceArg;

      storedPdxInfoType = pdxInfos.get(pdxInstance);

      if (storedPdxInfoType == null) {
        PdxType pdxTypeArg = pdxInstance.getPdxType();
        storedPdxInfoType = new PdxInfoType(pdxTypeArg.getClassName(), 
                                            pdxTypeArg.getTypeId());
      } 
    }
    
    return storedPdxInfoType; 
  }
  
  /**
   * Clears cached known Pdx Instance type Information.
   */
  //Note: used by PdxTypesIntrospectionResult
  static void clearKnownPdxTypeInfo() {
    pdxInfos.clear();
  }
}
