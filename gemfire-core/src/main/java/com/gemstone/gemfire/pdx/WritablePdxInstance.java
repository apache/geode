/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.pdx;


/**
 * WritablePdxInstance is a {@link PdxInstance} that also supports field modification 
 * using the {@link #setField setField} method. 
 * To get a WritablePdxInstance call {@link PdxInstance#createWriter createWriter}.
 * 
 * @author darrel
 * @since 6.6
 */

public interface WritablePdxInstance extends PdxInstance {
  /**
   * Set the existing named field to the given value.
   * The setField method has copy-on-write semantics.
   *  So for the modifications to be stored in the cache the WritablePdxInstance 
   * must be put into a region after setField has been called one or more times.
   * 
   * @param fieldName
   *          name of the field whose value will be set
   * @param value
   *          value that will be assigned to the field
   * @throws PdxFieldDoesNotExistException if the named field does not exist
   * @throws PdxFieldTypeMismatchException if the type of the value is not compatible with the field
   */
  public void setField(String fieldName, Object value);
}
