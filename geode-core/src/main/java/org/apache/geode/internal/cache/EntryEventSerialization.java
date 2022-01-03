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
package org.apache.geode.internal.cache;

import static org.apache.geode.internal.lang.SystemProperty.getProductBooleanProperty;
import static org.apache.geode.internal.lang.SystemPropertyHelper.EARLY_ENTRY_EVENT_SERIALIZATION;

import java.io.IOException;
import java.io.UncheckedIOException;

import org.apache.geode.pdx.internal.InternalPdxInstance;

public class EntryEventSerialization {

  private final boolean enabled =
      getProductBooleanProperty(EARLY_ENTRY_EVENT_SERIALIZATION).orElse(false);

  public void serializeNewValueIfNeeded(final InternalRegion region,
      final InternalEntryEvent event) {
    if (enabled) {
      doWork(region, event);
    }
  }

  private void doWork(final InternalRegion region, final InternalEntryEvent event) {
    if (region.getScope().isLocal()) {
      return;
    }
    if (region instanceof HARegion) {
      return;
    }
    if (region instanceof BucketRegionQueue) {
      return;
    }
    if (event.getCachedSerializedNewValue() != null) {
      return;
    }

    Object newValue = event.basicGetNewValue();
    if (newValue == null) {
      return;
    }
    if (newValue instanceof byte[]) {
      return;
    }
    if (Token.isToken(newValue)) {
      return;
    }

    event.setCachedSerializedNewValue(toBytes(newValue));
  }

  private byte[] toBytes(Object newValue) {
    byte[] newValueBytes;
    if (newValue instanceof InternalPdxInstance) {
      newValueBytes = toBytes((InternalPdxInstance) newValue);
    } else if (newValue instanceof CachedDeserializable) {
      newValueBytes = toBytes((CachedDeserializable) newValue);
    } else {
      newValueBytes = EntryEventImpl.serialize(newValue);
    }
    return newValueBytes;
  }

  private byte[] toBytes(final InternalPdxInstance pdxInstance) {
    try {
      return pdxInstance.toBytes();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private byte[] toBytes(final CachedDeserializable cachedDeserializable) {
    return cachedDeserializable.getSerializedValue();
  }
}
