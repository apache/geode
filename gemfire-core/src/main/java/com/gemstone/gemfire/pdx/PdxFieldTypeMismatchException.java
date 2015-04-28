/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
/**
 * 
 */
package com.gemstone.gemfire.pdx;

import com.gemstone.gemfire.GemFireException;

/**
 * Thrown if the type of a PDX field was changed or the wrong type was used.
 * PDX field types can not be changed. New fields can be added.
 * Existing fields can be removed. But once a field is added
 * its type can not be changed.
 * The writeXXX methods on {@link PdxWriter} define the field type.
 * <p>This exception can also be caused by {@link WritablePdxInstance#setField(String, Object) setField}
 * trying to set a value whose type is not compatible with the field.
 * @author darrel
 * @since 6.6
 *
 */
public class PdxFieldTypeMismatchException extends GemFireException {
  private static final long serialVersionUID = -829617162170742740L;

  /**
   * Constructs a new exception with the given message
   * @param message the message of the new exception
   */
  public PdxFieldTypeMismatchException(String message) {
    super(message);
  }
}
