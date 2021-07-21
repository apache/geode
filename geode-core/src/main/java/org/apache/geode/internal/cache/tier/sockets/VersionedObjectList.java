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

package org.apache.geode.internal.cache.tier.sockets;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.Logger;

import org.apache.geode.DataSerializer;
import org.apache.geode.InternalGemFireException;
import org.apache.geode.annotations.Immutable;
import org.apache.geode.distributed.DistributedMember;
import org.apache.geode.distributed.internal.membership.InternalDistributedMember;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.versions.DiskVersionTag;
import org.apache.geode.internal.cache.versions.VersionSource;
import org.apache.geode.internal.cache.versions.VersionTag;
import org.apache.geode.internal.logging.log4j.LogMarker;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;
import org.apache.geode.logging.internal.log4j.api.LogService;

/**
 * A refinement of ObjectPartList that adds per-entry versionTags and has its own serialized form to
 * allow key lists when there are no object components.
 * <P>
 * This class also implements Externalizable so that it can be serialized as part of a
 * PutAllPartialResultException.
 *
 * @since GemFire 7.0
 */

public class VersionedObjectList extends ObjectPartList implements Externalizable {
  private static final Logger logger = LogService.getLogger();
  private static final long serialVersionUID = -8384671357532347892L;

  ArrayList<VersionTag> versionTags;

  private boolean regionIsVersioned;

  /** a flag telling us that after transmission of the list all objects should remain serialized */
  private boolean serializeValues;

  /**
   * add a new entry to the list
   *
   * @param key the entry's key
   * @param versionTag the version tag for the entry, or null if there is no version information
   *        available
   */
  public void addKeyAndVersion(Object key, VersionTag versionTag) {
    if (logger.isTraceEnabled(LogMarker.VERSIONED_OBJECT_LIST_VERBOSE)) {
      logger.trace(LogMarker.VERSIONED_OBJECT_LIST_VERBOSE,
          "VersionedObjectList.addKeyAndVersion({}; {})", key, versionTag);
    }
    if (objects.size() > 0) {
      throw new IllegalStateException("attempt to add key/version to a list containing objects");
    }
    keys.add(key);
    if (regionIsVersioned) {
      // Assert.assertTrue(versionTag != null);
      versionTags.add(versionTag);
    }
  }

  @Override
  public void addPart(Object key, Object value, byte objectType, VersionTag versionTag) {
    if (logger.isTraceEnabled(LogMarker.VERSIONED_OBJECT_LIST_VERBOSE)) {
      logger.trace(LogMarker.VERSIONED_OBJECT_LIST_VERBOSE, "addPart({}; {}; {}; {}", key, value,
          objectType, versionTag);
    }
    super.addPart(key, value, objectType, versionTag);
    if (regionIsVersioned) {
      int tagsSize = versionTags.size();
      if (keys != null && (tagsSize != keys.size() - 1)) {
        // this should not happen - either all or none of the entries should have tags
        throw new InternalGemFireException();
      }
      if (objects != null && (objects.size() > 0)
          && (tagsSize != objects.size() - 1)) {
        // this should not happen - either all or none of the entries should have tags
        throw new InternalGemFireException();
      }
      // Assert.assertTrue(versionTag != null);
      versionTags.add(versionTag);
    }
  }

  /**
   * add a versioned "map" entry to the list
   */
  public void addObject(Object key, Object value, VersionTag versionTag) {
    addPart(key, value, OBJECT, versionTag);
  }

  // public methods

  public VersionedObjectList() {
    super();
    versionTags = new ArrayList<>();
  }

  public VersionedObjectList(boolean serializeValues) {
    this();
    this.serializeValues = serializeValues;
  }

  public VersionedObjectList(int maxSize, boolean hasKeys, boolean regionIsVersioned) {
    this(maxSize, hasKeys, regionIsVersioned, false);
  }

  public VersionedObjectList(int maxSize, boolean hasKeys, boolean regionIsVersioned,
      boolean serializeValues) {
    super(maxSize, hasKeys);
    if (regionIsVersioned) {
      versionTags = new ArrayList<>(maxSize);
      this.regionIsVersioned = true;
    } else {
      versionTags = new ArrayList<>();
    }
    this.serializeValues = serializeValues;
  }

  /**
   * replace null membership IDs in version tags with the given member ID. VersionTags received from
   * a server may have null IDs because they were operations performed by that server. We transmit
   * them as nulls to cut costs, but have to do the swap on the receiving end (in the client)
   *
   */
  public void replaceNullIDs(DistributedMember sender) {
    for (VersionTag versionTag : versionTags) {
      if (versionTag != null) {
        versionTag.replaceNullIDs((InternalDistributedMember) sender);
      }
    }
  }

  @Override
  public void addObjectPartForAbsentKey(Object key, Object value) {
    addPart(key, value, KEY_NOT_AT_SERVER, null);
  }

  /**
   * Add a part for a destroyed or missing entry. If the version tag is not null this represents a
   * tombstone.
   *
   */
  public void addObjectPartForAbsentKey(Object key, Object value, VersionTag version) {
    addPart(key, value, KEY_NOT_AT_SERVER, version);
  }

  @Override
  public void addAll(ObjectPartList other) {
    if (logger.isTraceEnabled(LogMarker.VERSIONED_OBJECT_LIST_VERBOSE)) {
      logger.trace(LogMarker.VERSIONED_OBJECT_LIST_VERBOSE, "VOL.addAll(other={}; this={}", other,
          this);
    }
    int myTypeArrayLength = hasKeys ? keys.size() : objects.size();
    int otherTypeArrayLength = other.hasKeys ? other.keys.size() : other.objects.size();
    super.addAll(other);
    VersionedObjectList vother = (VersionedObjectList) other;
    regionIsVersioned |= vother.regionIsVersioned;
    versionTags.addAll(vother.versionTags);
    if (myTypeArrayLength > 0 || otherTypeArrayLength > 0) {
      int newSize = myTypeArrayLength + otherTypeArrayLength;
      if (objectTypeArray != null) {
        newSize = Math.max(newSize, objectTypeArray.length);
        if (objectTypeArray.length < newSize) { // need more room
          byte[] temp = objectTypeArray;
          objectTypeArray = new byte[newSize];
          System.arraycopy(temp, 0, objectTypeArray, 0, temp.length);
        }
      } else {
        objectTypeArray = new byte[newSize];
      }
      if (other.objectTypeArray != null) {
        System.arraycopy(other.objectTypeArray, 0, objectTypeArray, myTypeArrayLength,
            otherTypeArrayLength);
      }
    }
  }

  public List<VersionTag> getVersionTags() {
    return Collections.unmodifiableList(versionTags);
  }

  public boolean hasVersions() {
    return versionTags.size() > 0;
  }

  /** clear the version tags from this list */
  public void clearVersions() {
    versionTags = new ArrayList<>(Math.max(50, size()));
  }

  /**
   * Add a version, assuming that the key list has already been established and we are now filling
   * in version information.
   *
   * @param tag the version tag to add
   */
  public void addVersion(VersionTag tag) {
    versionTags.add(tag);
  }

  /**
   * save the current key/tag pairs in the given map
   *
   */
  public void saveVersions(Map<Object, VersionTag> vault) {
    Iterator it = iterator();
    while (it.hasNext()) {
      Entry e = it.next();
      if (e.getVersionTag() != null || !vault.containsKey(e.getKey())) {
        // bug 51850: There could be duplicated keys in removeAll. If non-singlehop, the returned
        // version tags from the each primary bucket will be conflated here.
        vault.put(e.getKey(), e.getVersionTag());
      }
    }
  }

  /**
   *
   * @return whether the source region had concurrency checks enabled
   */
  public boolean regionIsVersioned() {
    return regionIsVersioned;
  }

  /**
   * add some versionless keys
   */
  public void addAllKeys(Collection<?> keys) {
    if (!hasKeys) {
      hasKeys = true;
      this.keys = new ArrayList<>(keys);
    } else {
      this.keys.addAll(keys);
    }
  }

  @Override
  public void reinit(int maxSize) {
    super.reinit(maxSize);
    versionTags.clear();
  }

  /** sets the keys for this partlist */
  public VersionedObjectList setKeys(List<Object> newKeys) {
    keys = newKeys;
    hasKeys = (keys != null);
    return this;
  }

  public void clearObjects() {
    objects = Collections.emptyList();
    objectTypeArray = new byte[0];
  }

  @Override
  public void clear() {
    super.clear();
    versionTags.clear();
  }

  @Immutable
  private static final KnownVersion[] serializationVersions = new KnownVersion[0];

  @Override
  public KnownVersion[] getSerializationVersions() {
    return serializationVersions;
  }

  static final byte FLAG_NULL_TAG = 0;
  static final byte FLAG_FULL_TAG = 1;
  static final byte FLAG_TAG_WITH_NEW_ID = 2;
  static final byte FLAG_TAG_WITH_NUMBER_ID = 3;

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    toData(out, context, 0, regionIsVersioned ? versionTags.size() : size(), true, true);
  }

  void toData(DataOutput out, SerializationContext context,
      int startIndex, int numEntries, boolean sendKeys, boolean sendObjects)
      throws IOException {
    int flags = 0;
    boolean hasObjects = false;
    boolean hasTags = false;
    if (sendKeys && hasKeys) {
      flags |= 0x01;
    }
    if (sendObjects && !objects.isEmpty()) {
      flags |= 0x02;
      hasObjects = true;
    }
    if (versionTags.size() > 0) {
      flags |= 0x04;
      hasTags = true;
      for (VersionTag tag : versionTags) {
        if (tag != null) {
          if (tag instanceof DiskVersionTag) {
            flags |= 0x20;
          }
          break;
        }
      }
    }
    if (regionIsVersioned) {
      flags |= 0x08;
    }
    if (serializeValues) {
      flags |= 0x10;
    }
    if (logger.isTraceEnabled(LogMarker.VERSIONED_OBJECT_LIST_VERBOSE)) {
      logger.trace(LogMarker.VERSIONED_OBJECT_LIST_VERBOSE,
          "serializing {} with flags 0x{} startIndex={} numEntries={}", this,
          Integer.toHexString(flags), startIndex, numEntries);
    }
    out.writeByte(flags);
    if (sendKeys && hasKeys) {
      int numToWrite = numEntries;
      if (numToWrite + startIndex > keys.size()) {
        numToWrite = Math.max(0, keys.size() - startIndex);
      }
      InternalDataSerializer.writeUnsignedVL(numToWrite, out);
      int index = startIndex;
      for (int i = 0; i < numToWrite; i++, index++) {
        context.getSerializer().writeObject(keys.get(index), out);
      }
    }
    if (sendObjects && hasObjects) {
      int numToWrite = numEntries;
      if (numToWrite + startIndex > objects.size()) {
        numToWrite = Math.max(0, objects.size() - startIndex);
      }
      InternalDataSerializer.writeUnsignedVL(numToWrite, out);
      int idx = 0;
      int index = startIndex;
      for (int i = 0; i < numToWrite; i++, index++) {
        writeObject(objects.get(index), idx++, out, context);
      }
    }
    if (hasTags) {
      int numToWrite = numEntries;
      if (numToWrite + startIndex > versionTags.size()) {
        numToWrite = Math.max(0, versionTags.size() - startIndex);
      }
      InternalDataSerializer.writeUnsignedVL(numToWrite, out);
      Map<VersionSource, Integer> ids = new HashMap<>(numToWrite);
      int idCount = 0;
      int index = startIndex;
      for (int i = 0; i < numToWrite; i++, index++) {
        VersionTag tag = versionTags.get(index);
        if (tag == null) {
          out.writeByte(FLAG_NULL_TAG);
        } else {
          VersionSource id = tag.getMemberID();
          if (id == null) {
            out.writeByte(FLAG_FULL_TAG);
            InternalDataSerializer.invokeToData(tag, out);
          } else {
            Integer idNumber = ids.get(id);
            if (idNumber == null) {
              out.writeByte(FLAG_TAG_WITH_NEW_ID);
              idNumber = idCount++;
              ids.put(id, idNumber);
              InternalDataSerializer.invokeToData(tag, out);
            } else {
              out.writeByte(FLAG_TAG_WITH_NUMBER_ID);
              tag.toData(out, false);
              tag.setMemberID(id);
              InternalDataSerializer.writeUnsignedVL(idNumber, out);
            }
          }
        }
      }
    }
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    final boolean isDebugEnabled_VOL =
        logger.isTraceEnabled(LogMarker.VERSIONED_OBJECT_LIST_VERBOSE);
    int flags = in.readByte();
    hasKeys = (flags & 0x01) == 0x01;
    boolean hasObjects = (flags & 0x02) == 0x02;
    boolean hasTags = (flags & 0x04) == 0x04;
    regionIsVersioned = (flags & 0x08) == 0x08;
    serializeValues = (flags & 0x10) == 0x10;
    boolean persistent = (flags & 0x20) == 0x20;
    if (isDebugEnabled_VOL) {
      logger.trace(LogMarker.VERSIONED_OBJECT_LIST_VERBOSE,
          "deserializing a VersionedObjectList with flags 0x{}", Integer.toHexString(flags));
    }
    if (hasKeys) {
      int size = (int) InternalDataSerializer.readUnsignedVL(in);
      keys = new ArrayList<>(size);
      if (isDebugEnabled_VOL) {
        logger.trace(LogMarker.VERSIONED_OBJECT_LIST_VERBOSE, "reading {} keys", size);
      }
      for (int i = 0; i < size; i++) {
        keys.add(context.getDeserializer().readObject(in));
      }
    }
    if (hasObjects) {
      int size = (int) InternalDataSerializer.readUnsignedVL(in);
      if (isDebugEnabled_VOL) {
        logger.trace(LogMarker.VERSIONED_OBJECT_LIST_VERBOSE, "reading {} objects", size);
      }
      objects = new ArrayList<>(size);
      objectTypeArray = new byte[size];
      for (int i = 0; i < size; i++) {
        readObject(i, in, context);
      }
    } else {
      objects = new ArrayList<>();
    }
    if (hasTags) {
      int size = (int) InternalDataSerializer.readUnsignedVL(in);
      if (isDebugEnabled_VOL) {
        logger.trace(LogMarker.VERSIONED_OBJECT_LIST_VERBOSE, "reading {} version tags", size);
      }
      versionTags = new ArrayList<>(size);
      List<VersionSource> ids = new ArrayList<>(size);
      for (int i = 0; i < size; i++) {
        byte entryType = in.readByte();
        switch (entryType) {
          case FLAG_NULL_TAG:
            versionTags.add(null);
            break;
          case FLAG_FULL_TAG:
            versionTags.add(VersionTag.create(persistent, in));
            break;
          case FLAG_TAG_WITH_NEW_ID:
            VersionTag tag = VersionTag.create(persistent, in);
            ids.add(tag.getMemberID());
            versionTags.add(tag);
            break;
          case FLAG_TAG_WITH_NUMBER_ID:
            tag = VersionTag.create(persistent, in);
            int idNumber = (int) InternalDataSerializer.readUnsignedVL(in);
            tag.setMemberID(ids.get(idNumber));
            versionTags.add(tag);
            break;
        }
      }
    } else {
      versionTags = new ArrayList<>();
    }
  }

  private void writeObject(Object value, int index, DataOutput out,
      SerializationContext context) throws IOException {
    byte objectType = objectTypeArray[index];
    if (logger.isTraceEnabled(LogMarker.VERSIONED_OBJECT_LIST_VERBOSE)) {
      logger.trace(LogMarker.VERSIONED_OBJECT_LIST_VERBOSE, "writing object {} of type {}: {}",
          index, objectType, value);
    }
    out.writeByte(objectType);
    if (objectType == OBJECT && value instanceof byte[]) {
      if (serializeValues) {
        DataSerializer.writeByteArray((byte[]) value, out);
      } else {
        out.write((byte[]) value);
      }
    } else if (objectType == EXCEPTION) {
      // write exception as byte array so native clients can skip it
      DataSerializer.writeByteArray(CacheServerHelper.serialize(value), out);
      // write the exception string for native clients
      DataSerializer.writeString(value.toString(), out);
    } else {
      if (serializeValues) {
        DataSerializer.writeObjectAsByteArray(value, out);
      } else {
        context.getSerializer().writeObject(value, out);
      }
    }
  }


  private void readObject(int index, DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    Object value;
    objectTypeArray[index] = in.readByte();
    if (logger.isTraceEnabled(LogMarker.VERSIONED_OBJECT_LIST_VERBOSE)) {
      logger.trace(LogMarker.VERSIONED_OBJECT_LIST_VERBOSE, "reading object {} of type {}", index,
          objectTypeArray[index]);
    }
    boolean isException = objectTypeArray[index] == EXCEPTION;
    if (isException) {
      byte[] exBytes = DataSerializer.readByteArray(in);
      value = CacheServerHelper.deserialize(exBytes);
      // ignore the exception string meant for native clients
      DataSerializer.readString(in);
    } else if (serializeValues) {
      value = DataSerializer.readByteArray(in);
    } else {
      value = context.getDeserializer().readObject(in);
    }
    objects.add(value);
  }

  @Override
  public void writeExternal(ObjectOutput out) throws IOException {
    toData(out, InternalDataSerializer.createSerializationContext(out));
  }

  @Override
  public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
    fromData(in, InternalDataSerializer.createDeserializationContext(in));
  }

  @Override
  public int getDSFID() {
    return DataSerializableFixedID.VERSIONED_OBJECT_LIST;
  }

  @Override
  public String toString() {
    StringBuilder desc = new StringBuilder();
    desc.append("VersionedObjectList(regionVersioned=").append(regionIsVersioned)
        .append("; hasKeys=").append(hasKeys).append("; keys=")
        .append(keys == null ? "null" : String.valueOf(keys.size())).append("; objects=")
        .append(objects.size()).append("; isobject=")
        .append(objectTypeArray == null ? "null" : objectTypeArray.length).append("; ccEnabled=")
        .append(regionIsVersioned).append("; versionTags=")
        .append(versionTags.size()).append(")\n");
    Iterator entries = iterator();
    while (entries.hasNext()) {
      desc.append(entries.next()).append("\n");
    }
    return desc.toString();
  }

  public Set<Object> keySet() {
    if (!hasKeys) {
      return Collections.emptySet();
    } else {
      return new HashSet<>(keys);
    }
  }

  public class Entry implements Map.Entry<Object, Object> {
    int index;

    Entry(int idx) {
      index = idx;
    }

    @Override
    public Object getKey() {
      if (hasKeys) {
        return keys.get(index);
      }
      return null;
    }

    public Object getObject() {
      return objects.get(index);
    }

    public VersionTag getVersionTag() {
      if (index < versionTags.size()) {
        return versionTags.get(index);
      }
      return null;
    }

    public byte getObjectType() {
      return objectTypeArray[index];
    }

    public boolean isKeyNotOnServer() {
      return objectTypeArray[index] == KEY_NOT_AT_SERVER;
    }

    public boolean isBytes() {
      return objectTypeArray[index] == BYTES;
    }

    public boolean isException() {
      return objectTypeArray[index] == EXCEPTION;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.Map.Entry#getValue()
     */
    @Override
    public Object getValue() {
      if (index < objects.size()) {
        return objects.get(index);
      } else {
        return null;
      }
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.Map.Entry#setValue(java.lang.Object)
     */
    @Override
    public Object setValue(Object value) {
      Object result = objects.get(index);
      objects.set(index, value);
      return result;
    }

    @Override
    public String toString() {
      return "[key=" + getKey() + ",value=" + getValue() + ",v=" + getVersionTag() + "]";
    }

  }

  public Iterator iterator() {
    return new Iterator();
  }

  public class Iterator implements java.util.Iterator<Entry> {
    int index = 0;
    int size = size();

    /*
     * (non-Javadoc)
     *
     * @see java.util.Iterator#hasNext()
     */
    @Override
    public boolean hasNext() {
      return index < size;
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.Iterator#next()
     */
    @Override
    public Entry next() {
      return new Entry(index++);
    }

    /*
     * (non-Javadoc)
     *
     * @see java.util.Iterator#remove()
     */
    @Override
    public void remove() {
      throw new UnsupportedOperationException();
    }

  }

  /**
   * Chunker is used to send a VersionedObjectList to a client in pieces. It works by pretending to
   * be a VersionedObjectList during serialization and writing only a portion of the list in each
   * invocation of toData().
   *
   *
   */
  public static class Chunker implements DataSerializableFixedID {
    private int index = 0;
    private final VersionedObjectList list;
    private final int chunkSize;
    private final boolean sendKeys;
    private final boolean sendObjects;

    public Chunker(VersionedObjectList list, int chunkSize, boolean sendKeys, boolean sendObjects) {
      this.list = list;
      this.chunkSize = chunkSize;
      this.sendKeys = sendKeys;
      this.sendObjects = sendObjects;
    }

    @Override
    public int getDSFID() {
      return DataSerializableFixedID.VERSIONED_OBJECT_LIST;
    }

    @Override
    public void toData(DataOutput out,
        SerializationContext context) throws IOException {
      int startIndex = index;
      index += chunkSize;
      list.toData(out, context, startIndex, chunkSize, sendKeys, sendObjects);
    }

    @Override
    public void fromData(DataInput in,
        DeserializationContext context) throws IOException, ClassNotFoundException {
      throw new IOException("this fromData method should never be invoked");
    }

    @Override
    public KnownVersion[] getSerializationVersions() {
      return list.getSerializationVersions();
    }
  }

}
