/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *========================================================================
 */

package com.gemstone.gemfire.cache;

/** Thrown when attempting to create a <code>Region.Entry</code> that already
 * exists in the <code>Region</code>.
 * @author Eric Zoerner
 *
 * @see com.gemstone.gemfire.cache.Region#create(Object, Object)
 * @see Region.Entry
 * @since 3.0
 */
public class EntryExistsException extends CacheException {

  private static final long serialVersionUID = 2925082493103537925L;

  private Object oldValue;

  /**
   * Constructs an instance of <code>EntryExistsException</code> with the specified detail message.
   * @param msg the detail message
   * @since 6.5
   */
  public EntryExistsException(String msg, Object oldValue) {
    super(msg);
    this.oldValue = oldValue;
  }

  /**
   * Returns the old existing value that caused this exception.
   */
  public Object getOldValue() {
    return this.oldValue;
  }

  /**
   * Sets the old existing value that caused this exception.
   */
  public void setOldValue(Object oldValue) {
    this.oldValue = oldValue;
  }

  @Override
  public String toString() {
    return super.toString() + ", with oldValue: " + oldValue;
  }
}
