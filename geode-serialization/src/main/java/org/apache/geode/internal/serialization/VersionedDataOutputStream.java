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

import java.io.DataOutputStream;
import java.io.OutputStream;


/**
 * An extension of {@link DataOutputStream} that implements {@link VersionedDataStream}.
 *
 * @since GemFire 7.1
 */
public class VersionedDataOutputStream extends DataOutputStream implements VersionedDataStream {

  private final KnownVersion version;

  /**
   * Creates a VersionedDataOutputStream that wraps the specified underlying OutputStream.
   *
   * @param out the underlying output stream
   * @param version the product version that serialized object on the given {@link OutputStream}
   */
  public VersionedDataOutputStream(OutputStream out, KnownVersion version) {
    super(out);
    this.version = version;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public KnownVersion getVersion() {
    return version;
  }
}
