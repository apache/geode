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
package org.apache.geode.pdx.internal;

import java.nio.ByteBuffer;

import org.apache.geode.internal.cache.GemFireCacheImpl;
import org.apache.geode.internal.tcp.ByteBufferInputStream.ByteSource;
import org.apache.geode.pdx.PdxFieldAlreadyExistsException;
import org.apache.geode.pdx.PdxUnreadFields;

/**
 * 
 * @since GemFire 6.6
 */
public class PdxUnreadData implements PdxUnreadFields {

  /**
   * This is the original type of the blob that we deserialized and did not read some of its fields.
   */
  private UnreadPdxType unreadType;
  private byte[][] unreadData;

  public PdxUnreadData() {
    // initialize may be called later
  }

  public PdxUnreadData(UnreadPdxType unreadType, PdxReaderImpl reader) {
    initialize(unreadType, reader);
  }

  public void initialize(UnreadPdxType unreadType, PdxReaderImpl reader) {
    this.unreadType = unreadType;
    int[] indexes = unreadType.getUnreadFieldIndexes();
    this.unreadData = new byte[indexes.length][];
    int i = 0;
    for (int idx : indexes) {

      ByteSource field = reader.getRaw(idx);

      // Copy the unread data into a new byte array
      this.unreadData[i] = new byte[field.capacity()];
      field.position(0);
      field.get(this.unreadData[i]);
      i++;
    }
  }

  public UnreadPdxType getUnreadType() {
    return this.unreadType;
  }

  /**
   * Returns the PdxType to use when serializing this unread data. Returns null if we don't know
   * what this type is yet.
   * 
   * @return the PdxType to use when serializing this unread data.
   */
  public PdxType getSerializedType() {
    return getUnreadType().getSerializedType();
  }

  public void setSerializedType(PdxType t) {
    getUnreadType().setSerializedType(t);
  }

  public void sendTo(PdxWriterImpl writer) {
    if (isEmpty())
      return;
    int[] indexes = this.unreadType.getUnreadFieldIndexes();
    int i = 0;
    while (i < this.unreadData.length) {
      int idx = indexes[i];
      byte[] data = this.unreadData[i];
      PdxField ft = this.unreadType.getPdxFieldByIndex(idx);
      try {
        writer.writeRawField(ft, data);
      } catch (PdxFieldAlreadyExistsException ex) {
        // fix for bug 43133
        throw new PdxFieldAlreadyExistsException(
            "Check the toData and fromData for " + this.unreadType.getClassName()
                + " to see if the field \"" + ft.getFieldName() + "\" is spelled differently.");
      }
      i++;
    }
  }

  /**
   * If o has unread data then add that unread data to copy.
   */
  public static void copy(Object o, Object copy) {
    // This method is only called by CopyHelper which is public and does not require that a Cache
    // exists.
    // So we need to call getInstance instead of getExisting.
    GemFireCacheImpl gfc = GemFireCacheImpl.getInstance();
    if (gfc == null)
      return;
    TypeRegistry tr = gfc.getPdxRegistry();
    PdxUnreadData ud = tr.getUnreadData(o);
    if (ud != null && !ud.isEmpty()) {
      tr.putUnreadData(copy, ud);
    }
  }

  public boolean isEmpty() {
    return this.unreadData == null;
  }
}
