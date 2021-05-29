/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package org.apache.geode.internal.serialization;

import java.io.DataInput;
import java.io.DataOutput;

import org.jetbrains.annotations.Nullable;


/**
 * An extension to {@link DataOutput}, {@link DataInput} used internally in product to indicate that
 * the input/output stream is attached to a GemFire peer having a different version.
 *
 * Internal product classes that implement {@link DataSerializableFixedID}
 * and change serialization format must check this on DataInput/DataOutput
 * (see {@link SerializationContext#getSerializationVersion()} methods) and deal with serialization
 * with previous {@link KnownVersion}s appropriately.
 */
public interface VersionedDataStream {

  /**
   * If the remote peer to which this input/output is connected has a version ordinal
   * for which a {@link KnownVersion} is known (locally) then that {@link KnownVersion} is returned,
   * otherwise null is returned.
   *
   * If the peer has a version ordinal for which no {@link KnownVersion} is locally known,
   * then this member cannot do any adjustment to serialization and it's the remote
   * peer's responsibility to adjust the serialization/deserialization according to
   * this peer.
   */
  @Nullable
  KnownVersion getVersion();
}
