/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal.cache.tier.sockets.command;

import java.io.IOException;
import com.gemstone.gemfire.internal.cache.tier.Command;
import com.gemstone.gemfire.internal.cache.tier.sockets.Message;
import com.gemstone.gemfire.internal.cache.tier.sockets.Part;

/**
 * Adds a callbackArg to PutAll80
 * @author dschneider
 *
 */
public class PutAllWithCallback extends PutAll80 {
  
  private final static PutAllWithCallback singleton = new PutAllWithCallback();
  
  public static Command getCommand() {
    return singleton;
  }
  
  protected PutAllWithCallback() {
  }
  
  @Override
  protected String putAllClassName() {
    return "putAllWithCallback";
  }
  @Override
  protected Object getOptionalCallbackArg(Message msg) throws ClassNotFoundException, IOException {
    Part callbackPart = msg.getPart(5);
    return callbackPart.getObject();
  }
  @Override
  protected int getBasePartCount() {
    return 6;
  }
}
