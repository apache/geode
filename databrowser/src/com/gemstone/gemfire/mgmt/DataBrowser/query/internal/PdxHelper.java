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

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.mgmt.DataBrowser.query.IntrospectionResult;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

/**
 * Helper class through which everything needed for PDX support is available.
 * <pre>
 * 1. Whether PDX is available 
 * 2. Whether given type is assignable to PdxInstance
 * 3. Whether given object is instance of PdxInstance  
 * <pre>
 * Using reflection, isolates loading of GemFire 6.6 types/classes used for 
 * PdxInstance from the rest of the code so that Data Browser can be used with 
 * earlier GemFire versions.
 * 
 * @author abhishek
 * @since 1.3 (GemFire 6.6)
 */
public class PdxHelper {

  /* Initial GemFire version since when PDX support started */
  private static final double PDX_SUPPORT_START_VERSION = 6.6;
  private static final double PDX_FIELDTYPE_MADE_PUBLIC_MINOR_VERSION = 6.2; //read it in continuation with 6.6 i.e. 6.6 MAJOR.MINOR & 6.2 MINOR.SUBMINOR 
  /* Class Name to use to load the PdxInstance type (java.lang.Class)  */
  private static final String PDXINSTANCE_CLASS_NAME = "com.gemstone.gemfire.pdx.PdxInstance";
  /* Class Name of the class used for IntrospectionResult for objects of type
   * PdxInstance. This class has to use internal GemFire types introduced in
   * GemFire 6.6, hence this is also accessed using reflection though it's a
   * Data Browser class */
  private static final String PDXINSTANCE_INTROSPECTION_RESULT_CLASS_NAME = "com.gemstone.gemfire.mgmt.DataBrowser.query.internal.PdxTypesIntrospectionResult";
  
  private static final String PDXINSTANCE_FIELDTYPE66_CLASS_NAME  = "com.gemstone.gemfire.pdx.internal.FieldType";
  private static final String PDXINSTANCE_FIELDTYPE662_CLASS_NAME = "com.gemstone.gemfire.pdx.FieldType";

  /* PdxHelper instance class */
  private static final PdxHelper instance = new PdxHelper();
  
  /* Class used for PdxInstance type */
  private Class<?> pdxInstanceClass;
  /* Class used for IntrospectionResult for PdxInstance type */
  private Class<? extends IntrospectionResult> pdxIntrospectionResultClass;
  /* Caching constructor of IntrospectionResult implementation used for
   * PdxInstance type. Specifically: PdxTypesIntrospectionResult(Object, String) */
  private Constructor<? extends IntrospectionResult> pdxIntrospectionResultConstructor;
  /* Caching method of IntrospectionResult implementation used for retrieving 
   * PdxInfoType for a PdxInstance type. Specifically: getPdxInfoType(Object) 
   * & clearKnownPdxInfo()*/
  private Method getPdxInfoTypeMethod;
  private Method clearKnownPdxInfoMethod;
  
  /* boolean indicating whether PDX is supported or not */
  private boolean pdxSupported = false;

  private double gemfireMajorMinorVersion    = 6.0; //oldest supported by Data Browser
  private double gemfireMinorSubMinorVersion = 0.0; //oldest supported by Data Browser
  private String fieldTypeClassName;

  /**
   * Checks whether PDX support is available. If PDX support is available,
   * initializes components required for introspecting PdxInstance
   */
  private PdxHelper() {
    String version = GemFireVersion.getGemFireVersion();
    if(version.indexOf("Beta") > 0){
	    version = version.substring(0, version.indexOf("Beta"));
    }
    String[] split = version.split("\\.");
    if (split.length >= 2) {
      String majorMinor = split[0] + "." + split[1];
      gemfireMajorMinorVersion = Double.valueOf(majorMinor);
      this.pdxSupported = gemfireMajorMinorVersion >= PDX_SUPPORT_START_VERSION;
    }
    
    if (this.pdxSupported) {
      if (split.length > 2) {
        String minorSubMinor = split[1] + "." + split[2];
        gemfireMinorSubMinorVersion = Double.valueOf(minorSubMinor);
      }
      
      fieldTypeClassName = PDXINSTANCE_FIELDTYPE662_CLASS_NAME;
      //for versions prior to 6.6.2 
      if (gemfireMajorMinorVersion == PDX_SUPPORT_START_VERSION && 
          gemfireMinorSubMinorVersion < PDX_FIELDTYPE_MADE_PUBLIC_MINOR_VERSION) {
        fieldTypeClassName = PDXINSTANCE_FIELDTYPE66_CLASS_NAME;
      }
       
      initPdxIntrospectionSupport();
    }
  }

  /**
   * Initializes local (Data Browser side) support for introspecting PdxInstance
   * types.
   */
  private void initPdxIntrospectionSupport() {
    try {
      pdxInstanceClass            = Class.forName(PDXINSTANCE_CLASS_NAME);
      pdxIntrospectionResultClass = (Class<? extends IntrospectionResult>) Class.forName(PDXINSTANCE_INTROSPECTION_RESULT_CLASS_NAME);
    } catch (ClassNotFoundException e) {
      pdxInstanceClass            = null;
      pdxIntrospectionResultClass = null;
      LogUtil.error("This GemFire version supports Pdx but could not load/find required classes.", e);
    }
    
    if (pdxIntrospectionResultClass != null) {
      try {
        pdxIntrospectionResultConstructor = pdxIntrospectionResultClass.getConstructor(new Class[] {Object.class, String.class});
        getPdxInfoTypeMethod    = pdxIntrospectionResultClass.getDeclaredMethod("getPdxInfoType", new Class[] {Object.class});
        clearKnownPdxInfoMethod = pdxIntrospectionResultClass.getDeclaredMethod("clearKnownPdxTypeInfo", new Class[0]);
      } catch (SecurityException e) {
        pdxIntrospectionResultConstructor = null;
        getPdxInfoTypeMethod              = null;
        clearKnownPdxInfoMethod           = null;
        LogUtil.error("This GemFire version supports Pdx but could not access required class elements.", e);
      } catch (NoSuchMethodException e) {
        pdxIntrospectionResultConstructor = null;
        getPdxInfoTypeMethod              = null;
        clearKnownPdxInfoMethod           = null;
        LogUtil.error("This GemFire version supports Pdx but could not find required class elements.", e);
      }
    }
    LogUtil.fine("Versions Info: "+gemfireMajorMinorVersion + "," + gemfireMinorSubMinorVersion + ", field type class : "+ fieldTypeClassName);
  }  
  
  /**
   * @return singleton for PdxHelper
   */
  public static PdxHelper getInstance() {
    return instance;
  }
  
  /**
   * @return whether the GemFire version supports PDX
   */
  public boolean isPdxSupported() {
    return this.pdxSupported;
  }

  /**
   * Checks whether PDX support is available & whether the given instance is of
   * PdxInstance GemFire type
   * 
   * @param other
   *          Object to be checked whether it's of PdxInstance GemFire type
   * @return true if PDX support is available & given object is of type
   *         PdxInstance, false otherwise
   */
  public boolean isPdxInstance(Object other) {
    if (!isPdxSupported()) {
      return false;
    }
    
    return pdxInstanceClass.isInstance(other);
  }

  /**
   * Checks whether PDX support is available & whether the given Class/type is
   * same as or sub-type of PdxInstance GemFire type
   * 
   * @param klass
   *          Class/type to be checked whether it is same as or sub-type of
   *          PdxInstance GemFire type
   * @return true if PDX support is available & given object is same as or
   *         sub-type of PdxInstance GemFire type, false otherwise
   */
  public boolean isPdxInstanceType(Class<?> klass) {
    if (!isPdxSupported()) {
      return false;
    }
    
    return pdxInstanceClass.isAssignableFrom(klass);
  }

  /**
   * Wraps the method PdxTypesIntrospectionResult.getPdxInfoType(Object) which
   * is used to Create or return existing PdxInfoType for the given object. If
   * given object - 'pdxInstanceObj' - is not of the type PdxInstanceImpl OR if
   * can call to PdxTypesIntrospectionResult.getPdxInfoType() using reflection
   * fails, it returns null.
   * 
   * @param pdxInstanceObj
   *          PdxInstance for which PdxInfoType is to be retrieved
   * @return new or existing PdxInfoType for the given object.
   * @throws PdxIntrospectionException
   *           if fails to invoke method -
   *           PdxTypesIntrospectionResult.getPdxInfoType
   */
  public PdxInfoType getPdxInfoType(Object pdxInstanceObj) throws PdxIntrospectionException {
    PdxInfoType pdxInfoType = null;
    try {
      pdxInfoType = (PdxInfoType) getPdxInfoTypeMethod.invoke(pdxIntrospectionResultClass, new Object[] {pdxInstanceObj});
    } catch (IllegalArgumentException e) {
      throw new PdxIntrospectionException(e);
    } catch (IllegalAccessException e) {
      throw new PdxIntrospectionException(e);
    } catch (InvocationTargetException e) {
      throw new PdxIntrospectionException(e);
    }
    
    return pdxInfoType;
  }
  
  /**
   * Wraps creation of new PdxTypesIntrospectionResult objects.
   * 
   * @param pdxInstanceObj
   *          PdxInstance for which PdxTypesIntrospectionResult is needed
   * @return new PdxTypesIntrospectionResult object for a given pdxInstanceObj
   * @throws PdxIntrospectionException
   *           if fails to create PdxTypesIntrospectionResult using reflection
   * @throws IllegalArgumentException
   *           if given object is not a PdxInstance
   */
  public IntrospectionResult getPdxMetaInfo(Object pdxInstanceObj) throws PdxIntrospectionException {
    IntrospectionResult pdxIntrospectionResult = null;
    try {
      pdxIntrospectionResult = pdxIntrospectionResultConstructor.newInstance(new Object[] {pdxInstanceObj, fieldTypeClassName});
    } catch (IllegalArgumentException e) {
      throw new PdxIntrospectionException(e);
    } catch (InstantiationException e) {
      throw new PdxIntrospectionException(e);
    } catch (IllegalAccessException e) {
      throw new PdxIntrospectionException(e);
    } catch (InvocationTargetException e) {
      throw new PdxIntrospectionException(e);
    }
    
    return pdxIntrospectionResult;
  }
  
  /**
   * Clears cached known Pdx Instance type Information.
   * Wraps call to PdxTypesIntrospectionResult.clearKnownPdxTypeInfo()
   */
  public void clearKnownPdxTypeInfo() {
    try {
      clearKnownPdxInfoMethod.invoke(pdxIntrospectionResultClass, new Object[0]);
    } catch (IllegalArgumentException e) {
      LogUtil.warning("Could not clear cached known Pdx Type information.", e);
    } catch (IllegalAccessException e) {
      LogUtil.warning("Could not clear cached known Pdx Type information.", e);
    } catch (InvocationTargetException e) {
      LogUtil.warning("Could not clear cached known Pdx Type information.", e);
    }
  }
}
