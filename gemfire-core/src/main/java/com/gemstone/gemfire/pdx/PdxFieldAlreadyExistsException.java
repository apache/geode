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
 * Thrown when writing a field if the named field already exists.
 * <p>This is usually caused by the same field name being written more than once.
 * <p>It can also be caused by a field name being spelled one way when written
 * and a different way when read. Field names are case sensitive.
 * <p>It can also be caused by {@link PdxWriter#writeUnreadFields(PdxUnreadFields) writeUnreadFields}
 * being called after a field is written.
 * 
 * @author darrel
 * @since 6.6
 *
 */
public class PdxFieldAlreadyExistsException extends GemFireException {
  private static final long serialVersionUID = -6989799940994503713L;

  /**
   * Constructs a new exception with the given message.
   * @param message the message of the new exception
   */
  public PdxFieldAlreadyExistsException(String message) {
    super(message);
  }
}
