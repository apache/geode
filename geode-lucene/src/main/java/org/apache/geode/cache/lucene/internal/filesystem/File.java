/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
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

package org.apache.geode.cache.lucene.internal.filesystem;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.file.Files;
import java.util.UUID;

import org.apache.geode.DataSerializer;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.internal.serialization.DataSerializableFixedID;
import org.apache.geode.internal.serialization.DeserializationContext;
import org.apache.geode.internal.serialization.KnownVersion;
import org.apache.geode.internal.serialization.SerializationContext;

/**
 * A file that is stored in a Apache Geode region.
 */
public class File implements DataSerializableFixedID {

  private transient FileSystem fileSystem;
  private transient int chunkSize;

  private String name;
  long length = 0;
  int chunks = 0;
  long created = System.currentTimeMillis();
  long modified = created;
  UUID id = UUID.randomUUID();
  boolean possiblyRenamed = false;

  /**
   * Constructor for serialization only
   */
  public File() {}

  File(final FileSystem fileSystem, final String name) {
    setFileSystem(fileSystem);

    this.name = name;
  }

  /**
   * @return the name
   */
  public String getName() {
    return name;
  }

  /**
   * @return the length
   */
  public long getLength() {
    return length;
  }

  /**
   * @return the created
   */
  public long getCreated() {
    return created;
  }

  /**
   * @return the modified
   */
  public long getModified() {
    return modified;
  }

  /**
   * Get an input stream that reads from the beginning the file
   *
   * The input stream is not threadsafe
   */
  public SeekableInputStream getInputStream() {
    // TODO get read lock?
    return new FileInputStream(this);
  }

  /**
   * Get an output stream that appends to the end of the file.
   */
  public OutputStream getOutputStream() {
    return new FileOutputStream(this);
  }

  void setFileSystem(final FileSystem fileSystem) {
    this.fileSystem = fileSystem;
    chunkSize = FileSystem.CHUNK_SIZE;
  }

  int getChunkSize() {
    return chunkSize;
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }

  @Override
  public KnownVersion[] getSerializationVersions() {
    return null;
  }

  @Override
  public int getDSFID() {
    return LUCENE_FILE;
  }

  @Override
  public void toData(DataOutput out,
      SerializationContext context) throws IOException {
    DataSerializer.writeString(name, out);
    out.writeLong(length);
    out.writeInt(chunks);
    out.writeLong(created);
    out.writeLong(modified);
    out.writeLong(id.getMostSignificantBits());
    out.writeLong(id.getLeastSignificantBits());
    out.writeBoolean(possiblyRenamed);
  }

  @Override
  public void fromData(DataInput in,
      DeserializationContext context) throws IOException, ClassNotFoundException {
    name = DataSerializer.readString(in);
    length = in.readLong();
    chunks = in.readInt();
    created = in.readLong();
    modified = in.readLong();
    long high = in.readLong();
    long low = in.readLong();
    id = new UUID(high, low);
    possiblyRenamed = in.readBoolean();
  }


  /**
   * Export this to a {@link java.io.File}
   */
  public void export(final java.io.File exportLocation) {
    java.io.File targetFile = new java.io.File(exportLocation, getName());
    try {
      Files.copy(getInputStream(), targetFile.toPath());
    } catch (IOException e) {
      throw new InternalGemFireError("Could not export file " + getName(), e);
    }
  }
}
