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


/** 
 * Local (Data Browser side) representation of the GemFire class PdxType.
 * 
 * @author abhishek
 * @since 1.3 (GemFire 6.6)
 */
public class PdxInfoType {
  /* Actual class name of the PdxInstance */
  private String className = "";
  /* Internal Type Id of the PdxInstance */
  private int    typeId    = -1;
  /* Actual class name of the PdxInstance */
  // Darrel mentioned that this doesn't mean that it is a NATIVE class/type
  // Hence, currently it's not used in Data Browser except for equals() check.
//  private boolean isNotJavaClass = false;
  
  private String displayName;
  
  /**
   * Useful for PdxInstance for enum.
   * 
   * @param className
   * @since 1.4 (GemFire 6.6.2)
   */
  public PdxInfoType(String className) {
    this.className   = className;
    this.displayName = this.className;//TODO: Is there no version for non PdxSerializable enums?
  }
  
  /**
   * Useful for PdxInstanceImpl.
   * 
   * @param className
   * @param typeId
   */
  public PdxInfoType(String className, int typeId) {
    this.className      = className;
    this.typeId         = typeId;
    this.displayName    = className + "(pdxId:" + typeId + ")";
  }
  
  /**
   * @return the className
   */
  public String getClassName() {
    return className;
  }
  
  /**
   * @return the typeId
   */
  public int getTypeId() {
    return typeId;
  }

  public String getDisplayName() {
    return displayName;
  }

  /**
   * Equality is checked on all className, typeId & isNotJavaClass.
   * 
   * @param obj
   *          object to check for equality with this PdxInfoType object
   * @return whether given object is same as this PdxInfoType object
   */
  public boolean equals(Object obj) {
    if (!(obj instanceof PdxInfoType)) {
      return false;
    }
    PdxInfoType other = (PdxInfoType) obj;   
    
    return this.className.equals(other.className) 
            && this.typeId == other.typeId;
  }
  
  /**
   * @return hashcode based on the hash code of class name & type id
   */
  public int hashCode() {
    return className.hashCode() + typeId;
  }

  /**
   * @return String representation of this object
   */
  public String toString() {
    return PdxInfoType.class.getSimpleName()+"["+className+","+typeId+"]";
  }
}
