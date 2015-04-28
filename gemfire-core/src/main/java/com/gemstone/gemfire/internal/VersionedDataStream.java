/*=========================================================================
 * Copyright (c) 2009-2014 Pivotal Software, Inc. All Rights Reserved. This product
 * is protected by U.S. and international copyright and intellectual
 * property laws. Pivotal products are covered by one or more patents listed
 * at http://www.pivotal.io/patents.
 *=========================================================================
 */

package com.gemstone.gemfire.internal;

import java.io.DataInput;
import java.io.DataOutput;

import com.gemstone.gemfire.DataSerializable;

/**
 * An extension to {@link DataOutput}, {@link DataInput} used internally in
 * product to indicate that the input/output stream is attached to a GemFire
 * peer having a different version. See the spec on rolling upgrades for more
 * details: <a
 * href="https://wiki.gemstone.com/display/SQLF/Rolling+upgrades">Rolling
 * Upgrades</a>.
 * 
 * Internal product classes that implement {@link DataSerializableFixedID} and
 * {@link DataSerializable} and change serialization format must check this on
 * DataInput/DataOutput (see
 * {@link InternalDataSerializer#getVersionForDataStream} methods) and deal with
 * serialization with previous {@link Version}s appropriately.
 * 
 * @author swale
 * @since 7.1
 */
public interface VersionedDataStream {

  /**
   * If the remote peer to which this input/output is connected has a lower
   * version that this member, then this returns the {@link Version} of the peer
   * else null. If the peer has a higher {@link Version}, then this member
   * cannot do any adjustment to serialization and its the remote peer's
   * responsibility to adjust the serialization/deserialization according to
   * this peer.
   */
  public Version getVersion();
}
