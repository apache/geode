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

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnNotFoundException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.ColumnValueNotAvailableException;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;
import com.gemstone.gemfire.pdx.PdxInstance;

/**
 * Wraps the IntrospectionResult types for Pdx objects which are:
 * <ol> All PdxSeriailzable objects which are available as PdxInstanceImpl
 * <ol> or enum which are available as PdxInstance
 * 
 * Wrapped type is decided using the argument passed to the constructor 
 * {@link #PdxTypesIntrospectionResult(Object, String)}.
 * 
 * @author abhishek
 * @since 1.4 (GemFire 6.6.2)
 */
public class PdxTypesIntrospectionResult implements IntrospectionResult {

  /* Methods handle to access the isEnum() methods of PdxInstance added in 6.6.2 */
  private static final Method isEnumMethod;

  /* Wrapped Pdx IntrospectionResult type */
  private IntrospectionResult wrapped;
  
  static {
    Method methodHandle = null;
    try {
      methodHandle = PdxInstance.class.getDeclaredMethod("isEnum", new Class[0]);
    } catch (SecurityException e) {
      methodHandle = null;
    } catch (NoSuchMethodException e) {
      methodHandle = null;
    } finally {
      isEnumMethod = methodHandle;
    }
  }

  /**
   * Creates a wrapper IntrospectionResult for Pdx Types.
   * 
   * @param pdxInstanceArg
   *          PdxInstance object to be introspected.
   * @param fieldTypeClass
   *          FieldType fully qualified class name
   * @throws PdxIntrospectionException
   *           if isEnum() method exists & its invocation fails on the given
   *           PdxInstance
   */
  //Note: used by PdxHelper over reflection
  public PdxTypesIntrospectionResult(Object pdxInstanceArg, String fieldTypeClass) 
    throws PdxIntrospectionException {
    PdxInstance pdxInstance = null;
    if (pdxInstanceArg instanceof PdxInstance) {
      pdxInstance = (PdxInstance) pdxInstanceArg;

      if (isEnum(pdxInstance)) {
        wrapped = new PdxEnumIntrospectionResult(pdxInstance);
//        System.out.println("PdxTypesIntrospectionResult.PdxTypesIntrospectionResult(): pdxInstance :: "+pdxInstance +", isEnum: true");
      } else {
        wrapped = new PdxIntrospectionResult(pdxInstance, fieldTypeClass);
//        System.out.println("PdxTypesIntrospectionResult.PdxTypesIntrospectionResult(): pdxInstance :: "+pdxInstance +", isEnum: false");
      }
    } else {
      throw new IllegalArgumentException("Only PdxInstance class is expected.");
    }
  }

  /**
   * Returns <code>true</code> if the given PdxInstance has isEnum() methods & 
   * if it represents a non-PdxSerializable Java enum. Hence, for 
   * GemFire >= 6.6 & < 6.6.2 this method should return <code>false</code>.
   * 
   * @param pdxInstance
   *          Pdx Instance to be checked whether it's an enum
   * @return true if the given PdxInstance has isEnum() methods & if it
   *         represents a non-PdxSerializable Java enum.; false otherwise
   * @throws PdxIntrospectionException
   *           if isEnum method exists & its invocation fails on the given
   *           PdxInstance
   */
  private boolean isEnum(PdxInstance pdxInstance) throws PdxIntrospectionException {
    boolean isEnum = false;
    
    if (isEnumMethod != null) { // PdxInstance class has isEnum() i.e. it's 6.6.2 or later
      try {
        isEnum = (Boolean) isEnumMethod.invoke(pdxInstance, new Object[0]);
      } catch (IllegalArgumentException e) {
        isEnum = false;
        throw new PdxIntrospectionException("Could not find PdxInstance.isEnum() in "+pdxInstance);
      } catch (IllegalAccessException e) {
        throw new PdxIntrospectionException("Could not find PdxInstance.isEnum() in "+pdxInstance);
      } catch (InvocationTargetException e) {
        throw new PdxIntrospectionException("Could not find PdxInstance.isEnum() in "+pdxInstance);
      }
    }
    
    return isEnum;
  }

  @Override
  public Class<?> getColumnClass(int index) throws ColumnNotFoundException {
    return wrapped.getColumnClass(index);
  }

  @Override
  public Class<?> getColumnClass(Object tuple, int index)
      throws ColumnNotFoundException {
    return wrapped.getColumnClass(tuple, index);
  }

  @Override
  public int getColumnCount() {
    return wrapped.getColumnCount();
  }

  @Override
  public int getColumnIndex(String name) throws ColumnNotFoundException {
    return wrapped.getColumnIndex(name);
  }

  @Override
  public String getColumnName(int index) throws ColumnNotFoundException {
    return wrapped.getColumnName(index);
  }

  @Override
  public int getColumnType(int index) throws ColumnNotFoundException {
    return wrapped.getColumnType(index);
  }

  @Override
  public int getColumnType(Object tuple, int index)
      throws ColumnNotFoundException {
    return wrapped.getColumnType(tuple, index);
  }

  @Override
  public Object getColumnValue(Object tuple, int index)
      throws ColumnNotFoundException, ColumnValueNotAvailableException {
    return wrapped.getColumnValue(tuple, index);
  }

  @Override
  public Class<?> getJavaType() {
    return wrapped.getJavaType();
  }

  @Override
  public String getJavaTypeName() {
    return wrapped.getJavaTypeName();
  }

  @Override
  public int getResultType() {
    return wrapped.getResultType();
  }

  @Override
  public boolean isCompatible(Object data) {
    return wrapped.isCompatible(data);
  }
  
  /**
   * Create or return existing PdxInfoType for the given object. If given object
   * - 'pdxInstanceArg' - is not of the type PdxInstance, it returns null.
   * 
   * @param pdxInstanceArg
   *          PdxInstance for which PdxInfoType is to be retrieved
   * @return new or existing PdxInfoType for the given object.
   */
  //Note: used by PdxHelper over reflection  
  static PdxInfoType getPdxInfoType(Object pdxInstanceArg) {
    //1. Check if it's a 'PdxInstanceImpl'. If yes, retrieve/return PdxInfoType for it 
    PdxInfoType pdxInfoType = PdxIntrospectionResult.getPdxInfoType(pdxInstanceArg);
    
    //2. Check if it's a 'PdxInstance'. If yes, retrieve/return PdxInfoType for it.
    if (pdxInfoType == null) {
      pdxInfoType = PdxEnumIntrospectionResult.getPdxInfoType(pdxInstanceArg);
    }
    
    return pdxInfoType;
  }
  
  /**
   * Clears cached known Pdx Instance type Information.
   */
  //Note: used by PdxHelper over reflection
  static void clearKnownPdxTypeInfo() {
    PdxIntrospectionResult.clearKnownPdxTypeInfo();
    PdxEnumIntrospectionResult.clearKnownPdxTypeInfo();
  }

}
