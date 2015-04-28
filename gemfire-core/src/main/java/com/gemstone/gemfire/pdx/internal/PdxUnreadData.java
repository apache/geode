/*=========================================================================
 * Copyright (c) 2010-2014 Pivotal Software, Inc. All Rights Reserved.
 * This product is protected by U.S. and international copyright
 * and intellectual property laws. Pivotal products are covered by
 * one or more patents listed at http://www.pivotal.io/patents.
 *=========================================================================
 */
package com.gemstone.gemfire.pdx.internal;

import java.nio.ByteBuffer;

import com.gemstone.gemfire.internal.cache.GemFireCacheImpl;
import com.gemstone.gemfire.pdx.PdxFieldAlreadyExistsException;
import com.gemstone.gemfire.pdx.PdxUnreadFields;

/**
 * 
 * @author darrel
 * @since 6.6
 */
public class PdxUnreadData implements PdxUnreadFields {

  /**
   * This is the original type of the blob that we deserialized
   * and did not read some of its fields.
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
    for (int idx: indexes) {
      
      ByteBuffer field = reader.getRaw(idx);
     
      //Copy the unread data into a new byte array
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
   * Returns the PdxType to use when serializing this unread data.
   * Returns null if we don't know what this type is yet.
   * @return the PdxType to use when serializing this unread data.
   */
  public PdxType getSerializedType() {
    return getUnreadType().getSerializedType();
  }
  
  public void setSerializedType(PdxType t) {
    getUnreadType().setSerializedType(t);
 }

  public void sendTo(PdxWriterImpl writer) {
    if (isEmpty()) return;
    int [] indexes = this.unreadType.getUnreadFieldIndexes();
    int i = 0;
    while (i < this.unreadData.length) {
      int idx = indexes[i];
      byte[] data = this.unreadData[i];
      PdxField ft = this.unreadType.getPdxFieldByIndex(idx);
      try {
        writer.writeRawField(ft, data);
      } catch (PdxFieldAlreadyExistsException ex) {
        // fix for bug 43133
        throw new PdxFieldAlreadyExistsException("Check the toData and fromData for " + this.unreadType.getClassName() + " to see if the field \"" + ft.getFieldName() + "\" is spelled differently.");
      }
      i++;
    }
  }

  /**
   * If o has unread data then add that unread data to copy.
   */
  public static void copy(Object o, Object copy) {
    // This method is only called by CopyHelper which is public and does not require that a Cache exists.
    // So we need to call getInstance instead of getExisting.
    GemFireCacheImpl gfc = GemFireCacheImpl.getInstance();
    if (gfc == null) return;
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
