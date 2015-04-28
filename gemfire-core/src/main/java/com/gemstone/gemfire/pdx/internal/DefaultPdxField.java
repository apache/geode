/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.pdx.internal;

import java.nio.ByteBuffer;

/**
 * Used by {@link PdxInstanceImpl#equals(Object)} to act as if it has
 * a field whose value is always the default.
 * @author darrel
 *
 */
public class DefaultPdxField extends PdxField {

  public DefaultPdxField(PdxField f) {
    super(f);
  }

  public ByteBuffer getDefaultBytes() {
    return getFieldType().getDefaultBytes();
  }

}
