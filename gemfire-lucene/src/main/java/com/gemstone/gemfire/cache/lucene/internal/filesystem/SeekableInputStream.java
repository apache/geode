package com.gemstone.gemfire.cache.lucene.internal.filesystem;

import java.io.IOException;
import java.io.InputStream;

/**
 * An input stream that supports seeking to a particular position.
 */
public abstract class SeekableInputStream extends InputStream {
  
  /**
   * Seek to a position in the stream. The position is relative to the beginning
   * of the stream (in other words, just before the first byte that was ever
   * read).
   * 
   * @param position
   * @throws IOException if the seek goes past the end of the stream
   */
  public abstract void seek(long position) throws IOException;
  
  public abstract SeekableInputStream clone();


}
