/*=========================================================================
 * Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.internal;

import java.io.*;


/** ObjToByteArraySerializer allows an object to be serialized as a byte array
 * so that the other side sees a byte array arrive.
 *
 *  @author Darrel
 *  @since 5.0.2
 * 
 */
public interface ObjToByteArraySerializer extends DataOutput {
  /**
   * Serialize the given object v as a byte array
   * @throws IOException if something goes wrong during serialization
   */
  public void writeAsSerializedByteArray(Object v) throws IOException;
}
