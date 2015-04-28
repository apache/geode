/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.management.internal.configuration.messages;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.gemstone.gemfire.DataSerializable;

/****
 * 
 * @author bansods
 *
 */
public class SharedConfigurationStatusRequest implements DataSerializable {

  private static final long serialVersionUID = 1L;

  @Override
  public void toData(DataOutput out) throws IOException {

  }

  @Override
  public void fromData(DataInput in) throws IOException, ClassNotFoundException {

  }

}
