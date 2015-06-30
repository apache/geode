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
import com.gemstone.gemfire.pdx.PdxInstance;

/**
 * This class is used to gather & provide introspection meta-data about Objects
 * which are enums & represented by GemFire type PdxInstance. The introspection
 * is done only on the 'name' of an enum. 
 * 
 * Note: enum that implement PdxSerializable would still be accessible as 
 * PdxInstanceImpl & Data Browser will handler them using 
 * PdxIntrospectionResult.
 * 
 * @author abhishek
 * @since 1.4 (GemFire 6.6.2)
 */
public class PdxEnumIntrospectionResult implements IntrospectionResult {
  /* stores the PdxInfoType objects for the known PdxInstance objects */
  private static final Map<PdxInstance, PdxInfoType> pdxInfos = new HashMap<PdxInstance, PdxInfoType>();

  /* List of Field Names in the given PdxInstance */
  private List<String> fieldNames;
  /* Local (Data Browser side) representation of the GemFire PdxType */
  private PdxInfoType  pdxInfoType;

  //Note: used by PdxTypesIntrospectionResult
  public PdxEnumIntrospectionResult(Object pdxInstanceArg) {
    PdxInstance pdxInstance;
    if (pdxInstanceArg instanceof PdxInstance) {
      pdxInstance = (PdxInstance) pdxInstanceArg;
    } else {
      throw new IllegalArgumentException("Only PdxInstance enum class is expected.");
    }

    fieldNames  = pdxInstance.getFieldNames();
    pdxInfoType = new PdxInfoType(pdxInstance.getClassName());
    pdxInfos.put(pdxInstance, pdxInfoType);
  }

  public Class<?> getColumnClass(int index) throws ColumnNotFoundException {
    // For enum, we are exposing only 'name' which will be a String
    return String.class;
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
    // For enum, we are exposing only 'name' which will be a String/Primitive
    return IntrospectionResult.PRIMITIVE_TYPE_COLUMN;
  }

  public int getColumnType(Object tuple, int index)
      throws ColumnNotFoundException {
    return getColumnType(index);
  }

  public Object getColumnValue(Object tuple, int index)
      throws ColumnNotFoundException, ColumnValueNotAvailableException {
    return ((PdxInstance)tuple).getField(getColumnName(index));
  }

  public Class<?> getJavaType() {
    return PdxInstance.class;
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
    
    if (pdxInstanceArg instanceof PdxInstance) {
      PdxInstance pdxInstance = (PdxInstance) pdxInstanceArg;

      storedPdxInfoType = pdxInfos.get(pdxInstance);

      if (storedPdxInfoType == null) {
        storedPdxInfoType = new PdxInfoType(pdxInstance.getClassName());
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
