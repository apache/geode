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
package org.apache.geode.distributed.internal;

import java.io.DataInput;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.internal.tcpserver.InfoRequest;
import org.apache.geode.distributed.internal.tcpserver.ShutdownRequest;
import org.apache.geode.distributed.internal.tcpserver.VersionRequest;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.serialization.ObjectDeserializer;
import org.apache.geode.management.internal.configuration.messages.ClusterManagementServiceInfoRequest;
import org.apache.geode.management.internal.configuration.messages.SharedConfigurationStatusRequest;

/**
 * Reads the request a locator receives on its server port. Requests carried as a
 * {@link org.apache.geode.internal.serialization.DataSerializableFixedID} name a registered fixed
 * id and are read normally. The remaining request types name their class on the wire; those are
 * read only for the classes listed here, which is the closed set of locator requests not carried as
 * a fixed id. A request naming any other class is rejected before that class is loaded.
 *
 * <p>
 * When a new locator request type that is not a {@code DataSerializableFixedID} is added, its class
 * name must be added to {@link #EXPECTED_REQUEST_CLASS_NAMES}.
 */
public class LocatorRequestDeserializer implements ObjectDeserializer {

  @Immutable
  private static final Set<String> EXPECTED_REQUEST_CLASS_NAMES =
      Collections.unmodifiableSet(new HashSet<>(Arrays.asList(
          InfoRequest.class.getName(),
          ShutdownRequest.class.getName(),
          VersionRequest.class.getName(),
          SharedConfigurationStatusRequest.class.getName(),
          ClusterManagementServiceInfoRequest.class.getName())));

  @Override
  @SuppressWarnings("unchecked")
  public <T> T readObject(DataInput input) throws IOException, ClassNotFoundException {
    return (T) InternalDataSerializer.readRequestObject(input,
        EXPECTED_REQUEST_CLASS_NAMES::contains);
  }

  @Override
  public void invokeFromData(Object ds, DataInput in) throws IOException, ClassNotFoundException {
    InternalDataSerializer.invokeFromData(ds, in);
  }
}
