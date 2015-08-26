package com.gemstone.gemfire.cache.lucene.internal.filesystem;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.UUID;

/**
 * A file that is stored in a gemfire region.
 */
public class File implements Serializable {
  private static final long serialVersionUID = 1L;
  
  private transient FileSystem fileSystem;
  private transient int chunkSize;

  private String name;
  long length = 0;
  int chunks = 0;
  long created = System.currentTimeMillis();
  long modified = created;
  UUID id = UUID.randomUUID();

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
   * Get an output stream that appends to the end
   * of the file.
   */
  public OutputStream getOutputStream() {
    return new FileOutputStream(this);
  }

  void setFileSystem(final FileSystem fileSystem) {
    this.fileSystem = fileSystem;
    this.chunkSize = FileSystem.CHUNK_SIZE;
  }

  int getChunkSize() {
    return chunkSize;
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }
}
