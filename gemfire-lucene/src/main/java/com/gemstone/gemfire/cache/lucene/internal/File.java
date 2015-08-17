package com.gemstone.gemfire.cache.lucene.internal;

import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;

final public class File implements Serializable {
  private static final long serialVersionUID = 1L;

  private transient FileSystem fileSystem;
  private transient int chunkSize;

  String name;
  long length = 0;
  int chunks = 0;
  long created = System.currentTimeMillis();
  long modified = created;

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

  public InputStream getInputStream() {
    // TODO get read lock?
    return new FileInputStream(this);
  }

  public OutputStream getOutputStream() {
    return new FileOutputStream(this);
  }

  void setFileSystem(final FileSystem fileSystem) {
    this.fileSystem = fileSystem;
    this.chunkSize = fileSystem.chunkSize;
  }

  int getChunkSize() {
    return chunkSize;
  }

  public FileSystem getFileSystem() {
    return fileSystem;
  }
}
