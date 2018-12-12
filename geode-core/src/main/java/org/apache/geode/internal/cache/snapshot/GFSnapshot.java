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
package org.apache.geode.internal.cache.snapshot;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.channels.FileChannel;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

import org.apache.geode.DataSerializer;
import org.apache.geode.cache.CacheClosedException;
import org.apache.geode.cache.snapshot.SnapshotIterator;
import org.apache.geode.internal.ExitCode;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.cache.InternalCache;
import org.apache.geode.internal.cache.snapshot.SnapshotPacket.SnapshotRecord;
import org.apache.geode.pdx.internal.EnumInfo;
import org.apache.geode.pdx.internal.PdxType;
import org.apache.geode.pdx.internal.TypeRegistry;

/**
 * Provides support for reading and writing snapshot files.
 */
public class GFSnapshot {
  /**
   * Writes cache entries to the snapshot.
   */
  public interface SnapshotWriter {
    /**
     * Appends a cache entry to the snapshot.
     *
     * @param entry the cache entry
     */
    void snapshotEntry(SnapshotRecord entry) throws IOException;

    /**
     * Invoked to indicate that all entries have been written to the snapshot.
     */
    void snapshotComplete() throws IOException;
  }

  /** the snapshot format version 1 */
  public static final int SNAP_VER_1 = 1;

  /** the snapshot format version 2 */
  public static final int SNAP_VER_2 = 2;

  /** the snapshot file format */
  private static final byte[] SNAP_FMT = {0x47, 0x46, 0x53};

  private GFSnapshot() {}

  public static void main(String[] args) throws Exception {
    if (args.length != 1) {
      System.out.println("Usage: GFSnapshot <file>");
      ExitCode.FATAL.doSystemExit();
    }

    GFSnapshotImporter imp = new GFSnapshotImporter(new File(args[0]), null);
    try {
      System.out.println("Snapshot format is version " + imp.getVersion());
      System.out.println("Snapshot region is " + imp.getRegionName());

      ExportedRegistry reg = imp.getPdxTypes();
      Map<Integer, PdxType> types = reg.types();
      System.out.println("Found " + types.size() + " PDX types:");
      for (Entry<Integer, PdxType> entry : types.entrySet()) {
        System.out.println("\t" + entry.getKey() + " = " + entry.getValue());
      }

      Map<Integer, EnumInfo> enums = reg.enums();
      System.out.println("Found " + enums.size() + " PDX enums: ");
      for (Entry<Integer, EnumInfo> entry : enums.entrySet()) {
        System.out.println("\t" + entry.getKey() + " = " + entry.getValue());
      }

      System.out.println();
      SnapshotRecord record;
      while ((record = imp.readSnapshotRecord()) != null) {
        System.out.println(record.getKeyObject() + " = " + record.getValueObject());
      }
    } finally {
      imp.close();
    }
  }

  /**
   * Creates a snapshot file and provides a serializer to write entries to the snapshot.
   *
   * @param snapshot the snapshot file
   * @param region the region name
   * @return the callback to allow the invoker to provide the snapshot entries
   * @throws IOException error writing the snapshot file
   */
  public static SnapshotWriter create(File snapshot, String region, InternalCache cache)
      throws IOException {
    final GFSnapshotExporter out = new GFSnapshotExporter(snapshot, region, cache);
    return new SnapshotWriter() {
      @Override
      public void snapshotEntry(SnapshotRecord entry) throws IOException {
        out.writeSnapshotEntry(entry);
      }

      @Override
      public void snapshotComplete() throws IOException {
        out.close();
      }
    };
  }

  /**
   * Reads a snapshot file.
   *
   * @param <K> the key type
   * @param <V> the value type
   * @param snapshot the snapshot file
   * @return the snapshot iterator
   *
   * @throws IOException error reading the snapshot file
   * @throws ClassNotFoundException unable to deserialize entry
   */
  public static <K, V> SnapshotIterator<K, V> read(final File snapshot, TypeRegistry typeRegistry)
      throws IOException, ClassNotFoundException {
    return new SnapshotIterator<K, V>() {
      GFSnapshotImporter in = new GFSnapshotImporter(snapshot, typeRegistry);

      private boolean foundNext;
      private Entry<K, V> next;

      @Override
      public boolean hasNext() throws IOException, ClassNotFoundException {
        if (!foundNext) {
          return moveNext();
        }
        return true;
      }

      @Override
      public Entry<K, V> next() throws IOException, ClassNotFoundException {
        if (!foundNext && !moveNext()) {
          throw new NoSuchElementException();
        }
        Entry<K, V> result = next;

        foundNext = false;
        next = null;

        return result;
      }

      @Override
      public void close() throws IOException {
        in.close();
      }

      private boolean moveNext() throws IOException, ClassNotFoundException {
        SnapshotRecord record;
        while ((record = in.readSnapshotRecord()) != null) {
          foundNext = true;

          final K key = record.getKeyObject();
          final V value = record.getValueObject();

          next = new Entry<K, V>() {
            @Override
            public K getKey() {
              return key;
            }

            @Override
            public V getValue() {
              return value;
            }

            @Override
            public V setValue(V value) {
              throw new UnsupportedOperationException();
            }
          };
          return true;
        }

        close();
        return false;
      }
    };
  }

  /**
   * Writes a snapshot file.
   */
  static class GFSnapshotExporter {
    /** the file channel, used for random access */
    private final FileChannel fc;

    /** the output stream */
    private final DataOutputStream dos;
    private final InternalCache cache;

    public GFSnapshotExporter(File out, String region, InternalCache cache) throws IOException {
      this.cache = cache;
      FileOutputStream fos = new FileOutputStream(out);
      fc = fos.getChannel();

      dos = new DataOutputStream(new BufferedOutputStream(fos));

      // write snapshot version
      dos.writeByte(SNAP_VER_2);

      // write format type
      dos.write(SNAP_FMT);

      // write temporary pdx location in bytes 4-11
      dos.writeLong(-1);

      // write region name
      dos.writeUTF(region);
    }

    /**
     * Writes an entry in the snapshot.
     *
     * @param entry the snapshot entry
     * @throws IOException unable to write entry
     */
    public void writeSnapshotEntry(SnapshotRecord entry) throws IOException {
      InternalDataSerializer.invokeToData(entry, dos);
    }

    public void close() throws IOException {
      // write entry terminator entry
      DataSerializer.writeByteArray(null, dos);

      // grab the pdx start location
      dos.flush();
      long registryPosition = fc.position();

      // write pdx types
      try {
        new ExportedRegistry(cache.getPdxRegistry()).toData(dos);
      } catch (CacheClosedException e) {
        // ignore pdx types
        new ExportedRegistry().toData(dos);
      }

      // write the pdx position
      dos.flush();
      fc.position(4);
      dos.writeLong(registryPosition);

      dos.close();
    }
  }

  /**
   * Reads a snapshot file.
   */
  static class GFSnapshotImporter {
    /** the snapshot file version */
    private final byte version;

    /** the region name */
    private final String region;

    /** the internal pdx registry (not the system-wide pdx registry) */
    private final ExportedRegistry pdx;

    /** the input stream */
    private final DataInputStream dis;

    public GFSnapshotImporter(File in, TypeRegistry typeRegistry)
        throws IOException, ClassNotFoundException {
      pdx = new ExportedRegistry();

      // read header and pdx registry
      long entryPosition;

      FileInputStream fis = new FileInputStream(in);
      FileChannel fc = fis.getChannel();
      DataInputStream tmp = new DataInputStream(fis);
      try {
        // read the snapshot file header
        version = tmp.readByte();
        if (version == SNAP_VER_1) {
          throw new IOException(
              String.format("Unsupported snapshot version: %s", SNAP_VER_1)
                  + ": " + in);

        } else if (version == SNAP_VER_2) {
          // read format
          byte[] format = new byte[3];
          tmp.readFully(format);
          if (!Arrays.equals(format, SNAP_FMT)) {
            throw new IOException(String.format("Unrecognized snapshot file type: %s",
                Arrays.toString(format)) + ": " + in);
          }

          // read pdx location
          long registryPosition = tmp.readLong();

          // read region
          region = tmp.readUTF();
          entryPosition = fc.position();

          // read pdx
          if (registryPosition != -1) {
            fc.position(registryPosition);
            pdx.fromData(tmp);
          }
        } else {
          throw new IOException(
              String.format("Unrecognized snapshot file version: %s", version)
                  + ": " + in);
        }
      } finally {
        tmp.close();
      }

      // check compatibility with the existing pdx types so we don't have to
      // do any translation...preexisting types or concurrent put ops may cause
      // this check to fail
      checkPdxTypeCompatibility(typeRegistry);
      checkPdxEnumCompatibility(typeRegistry);

      // open new stream with buffering for reading entries
      dis = new DataInputStream(new BufferedInputStream(new FileInputStream(in)));
      dis.skip(entryPosition);
    }

    /**
     * Returns the snapshot file version.
     *
     * @return the version
     */
    public byte getVersion() {
      return version;
    }

    /**
     * Returns the original pathname of the region used to create the snapshot.
     *
     * @return the region name (full pathname)
     */
    public String getRegionName() {
      return region;
    }

    /**
     * Returns the pdx types defined in the snapshot file.
     *
     * @return the pdx types
     */
    public ExportedRegistry getPdxTypes() {
      return pdx;
    }

    /**
     * Reads a snapshot entry. If the last entry has been read, a null value will be returned.
     *
     * @return the entry or null
     * @throws IOException unable to read entry
     * @throws ClassNotFoundException unable to create entry
     */
    public SnapshotRecord readSnapshotRecord() throws IOException, ClassNotFoundException {
      byte[] key = DataSerializer.readByteArray(dis);
      if (key == null) {
        return null;
      }

      byte[] value = DataSerializer.readByteArray(dis);
      return new SnapshotRecord(key, value);
    }

    public void close() throws IOException {
      dis.close();
    }

    private void checkPdxTypeCompatibility(TypeRegistry tr) {
      if (tr == null) {
        return;
      }

      for (Map.Entry<Integer, PdxType> entry : pdx.types().entrySet()) {
        tr.addImportedType(entry.getKey(), entry.getValue());
      }
    }

    private void checkPdxEnumCompatibility(TypeRegistry tr) {
      if (tr == null) {
        return;
      }

      for (Map.Entry<Integer, EnumInfo> entry : pdx.enums().entrySet()) {
        tr.addImportedEnum(entry.getKey(), entry.getValue());
      }
    }
  }
}
