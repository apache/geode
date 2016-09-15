/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * 
 *   http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.geode.cache.lucene.internal.filesystem;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

/**
 * A Filesystem like interface that stores file data in geode regions.
 * 
 * This filesystem is safe for use with multiple threads if the threads are not
 * modifying the same files. A single file is not safe to modify by multiple
 * threads, even between different members of the distributed system.
 * 
 * Changes to a file may not be visible to other members of the system until the
 * FileOutputStream is closed.
 *
 */
public class FileSystem {
  // private final Cache cache;
  private final ConcurrentMap<String, File> fileRegion;
  private final ConcurrentMap<ChunkKey, byte[]> chunkRegion;
  
  static final int CHUNK_SIZE = 1024 * 1024; //1 MB
  private final FileSystemStats stats;

  /**
   * Create filesystem that will store data in the two provided regions. The fileRegion contains
   * metadata about the files, and the chunkRegion contains the actual data. If data from either region is missing
   * or inconsistent, no guarantees are made about what this class will do, so it's best if these regions are colocated
   * and in the same disk store to ensure the data remains together.
   * @param fileRegion the region to store metadata about the files
   * @param chunkRegion the region to store actual file data.
   */
  public FileSystem(ConcurrentMap<String, File> fileRegion, ConcurrentMap<ChunkKey, byte[]> chunkRegion, FileSystemStats stats) {
    this.fileRegion = fileRegion;
    this.chunkRegion = chunkRegion;
    this.stats = stats;
  }

  public Collection<String> listFileNames() {
    return fileRegion.keySet();
  }

  public File createFile(final String name) throws IOException {
    // TODO lock region ?
    final File file = new File(this, name);
    if (null != fileRegion.putIfAbsent(name, file)) {
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
    final File file = fileRegion.get(name);
    
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
    File file = fileRegion.remove(name);
    if(file == null) {
      throw new FileNotFoundException(name);
    }

    // TODO consider removeAll with all ChunkKeys listed.
    final ChunkKey key = new ChunkKey(file.id, 0);
    while (true) {
      // TODO consider mutable ChunkKey
      if (null == chunkRegion.remove(key)) {
        // no more chunks
        break;
      }
      key.chunkId++;
    }

    stats.incFileDeletes(1);
  }
  
  public void renameFile(String source, String dest) throws IOException {
    final File sourceFile = fileRegion.get(source);
    if (null == sourceFile) {
      throw new FileNotFoundException(source);
    }
    
    final File destFile = createFile(dest);
    
    destFile.chunks = sourceFile.chunks;
    destFile.created = sourceFile.created;
    destFile.length = sourceFile.length;
    destFile.modified = sourceFile.modified;
    destFile.id = sourceFile.id;
    updateFile(destFile);
    
    // TODO - What is the state of the system if 
    // things crash in the middle of moving this file?
    // Seems like we will have two files pointing
    // at the same data

    fileRegion.remove(source);

    stats.incFileRenames(1);
  }
  
  byte[] getChunk(final File file, final int id) {
    final ChunkKey key = new ChunkKey(file.id, id);
    
    //The file's metadata indicates that this chunk shouldn't
    //exist. Purge all of the chunks that are larger than the file metadata
    if(id >= file.chunks) {
      while(chunkRegion.containsKey(key)) {
        chunkRegion.remove(key);
        key.chunkId++;
      }
      
      return null;
    }
    
    final byte[] chunk = chunkRegion.get(key);
    stats.incReadBytes(chunk.length);
    return chunk;
  }

  public void putChunk(final File file, final int id, final byte[] chunk) {
    final ChunkKey key = new ChunkKey(file.id, id);
    chunkRegion.put(key, chunk);
    stats.incWrittenBytes(chunk.length);
  }

  void updateFile(File file) {
    fileRegion.put(file.getName(), file);
  }

  public ConcurrentMap<String, File> getFileRegion() {
    return fileRegion;
  }

  public ConcurrentMap<ChunkKey, byte[]> getChunkRegion() {
    return chunkRegion;
  }

  /**
   * Export all of the files in the filesystem to the provided directory
   */
  public void export(final java.io.File exportLocation) {

    listFileNames().stream().forEach(fileName-> {
      try {
        getFile(fileName).export(exportLocation);
      }
      catch (FileNotFoundException e) {
        //ignore this, it was concurrently removed
      }

    });
  }

}
