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

import org.apache.geode.internal.logging.LogService;
import org.apache.logging.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * A Filesystem like interface that stores file data in geode regions.
 *
 * This filesystem is safe for use with multiple threads if the threads are not modifying the same
 * files. A single file is not safe to modify by multiple threads, even between different members of
 * the distributed system.
 *
 * Changes to a file may not be visible to other members of the system until the FileOutputStream is
 * closed.
 *
 */
public class FileSystem {
  private static final Logger logger = LogService.getLogger();

  private final Map fileAndChunkRegion;

  static final int CHUNK_SIZE = 1024 * 1024; // 1 MB
  private final FileSystemStats stats;

  /**
   * Create filesystem that will store data in the two provided regions. The fileAndChunkRegion
   * contains metadata about the files, and the chunkRegion contains the actual data. If data from
   * either region is missing or inconsistent, no guarantees are made about what this class will do,
   * so it's best if these regions are colocated and in the same disk store to ensure the data
   * remains together.
   *
   * @param fileAndChunkRegion the region to store metadata about the files
   */
  public FileSystem(Map fileAndChunkRegion, FileSystemStats stats) {
    this.fileAndChunkRegion = fileAndChunkRegion;
    this.stats = stats;
  }

  public Collection<String> listFileNames() {
    return (Collection<String>) fileAndChunkRegion.keySet().stream()
        .filter(entry -> (entry instanceof String)).collect(Collectors.toList());
  }

  public File createFile(final String name) throws IOException {
    // TODO lock region ?
    final File file = new File(this, name);
    if (null != fileAndChunkRegion.putIfAbsent(name, file)) {
      throw new IOException("File exists.");
    }
    stats.incFileCreates(1);

    // TODO unlock region ?
    return file;
  }

  public File putIfAbsentFile(String name, File file) throws IOException {
    // TODO lock region ?
    if (null != fileAndChunkRegion.putIfAbsent(name, file)) {
      throw new IOException("File exists.");
    }
    stats.incFileCreates(1);
    // TODO unlock region ?
    return file;
  }

  public File createTemporaryFile(final String name) throws IOException {
    final File file = new File(this, name);
    stats.incTemporaryFileCreates(1);
    return file;
  }

  public File getFile(final String name) throws FileNotFoundException {
    final File file = (File) fileAndChunkRegion.get(name);

    if (null == file) {
      throw new FileNotFoundException(name);
    }

    file.setFileSystem(this);
    return file;
  }

  public void deleteFile(final String name) throws FileNotFoundException {
    // TODO locks?

    // TODO - What is the state of the system if
    // things crash in the middle of removing this file?
    // Seems like a file will be left with some
    // dangling chunks at the end of the file
    File file = (File) fileAndChunkRegion.remove(name);
    if (file == null) {
      throw new FileNotFoundException(name);
    }

    if (file.possiblyRenamed == false) {
      // TODO consider removeAll with all ChunkKeys listed.
      final ChunkKey key = new ChunkKey(file.id, 0);
      while (true) {
        // TODO consider mutable ChunkKey
        if (null == fileAndChunkRegion.remove(key)) {
          // no more chunks
          break;
        }
        key.chunkId++;
      }
    }
    stats.incFileDeletes(1);
  }

  public void renameFile(String source, String dest) throws IOException {

    final File sourceFile = (File) fileAndChunkRegion.get(source);
    if (null == sourceFile) {
      throw new FileNotFoundException(source);
    }

    final File destFile = new File(this, dest);
    destFile.chunks = sourceFile.chunks;
    destFile.created = sourceFile.created;
    destFile.length = sourceFile.length;
    destFile.modified = sourceFile.modified;
    destFile.id = sourceFile.id;
    sourceFile.possiblyRenamed = true;
    // TODO - What is the state of the system if
    // things crash in the middle of moving this file?
    // Seems like we will have two files pointing
    // at the same data
    updateFile(sourceFile);
    putIfAbsentFile(dest, destFile);

    fileAndChunkRegion.remove(source);
    stats.incFileRenames(1);
  }

  byte[] getChunk(final File file, final int id) {
    final ChunkKey key = new ChunkKey(file.id, id);

    // The file's metadata indicates that this chunk shouldn't
    // exist. Purge all of the chunks that are larger than the file metadata
    if (id >= file.chunks) {
      while (fileAndChunkRegion.containsKey(key)) {
        fileAndChunkRegion.remove(key);
        key.chunkId++;
      }
      return null;
    }

    final byte[] chunk = (byte[]) fileAndChunkRegion.get(key);
    if (chunk != null) {
      stats.incReadBytes(chunk.length);
    } else {
      logger.debug("Chunk was null for file:" + file.getName() + " file id: " + key.getFileId()
          + " chunkKey:" + key.chunkId);
    }
    return chunk;
  }

  public void putChunk(final File file, final int id, final byte[] chunk) {
    final ChunkKey key = new ChunkKey(file.id, id);
    fileAndChunkRegion.put(key, chunk);
    stats.incWrittenBytes(chunk.length);
  }

  void updateFile(File file) {
    fileAndChunkRegion.put(file.getName(), file);
  }

  public Map getFileAndChunkRegion() {
    return fileAndChunkRegion;
  }


  /**
   * Export all of the files in the filesystem to the provided directory
   */
  public void export(final java.io.File exportLocation) {

    listFileNames().stream().forEach(fileName -> {
      try {
        getFile(fileName).export(exportLocation);
      } catch (FileNotFoundException e) {
        // ignore this, it was concurrently removed
      }
    });
  }

}
