package com.gemstone.gemfire.cache.lucene.internal.filesystem;

import java.io.IOException;
import java.io.InputStream;

/**
 * An input stream that reads chunks from
 * a File saved in the region. This input stream
 * will keep going back to the region to look for
 * chunks until nothing is found.
 */
final class FileInputStream extends InputStream {

  private final File file;
  private byte[] chunk = null;
  private int chunkPosition = 0;
  private int chunkId = 0;
  private boolean open = true;

  public FileInputStream(File file) {
    this.file = file;
    nextChunk();
  }

  @Override
  public int read() throws IOException {
    assertOpen();

    checkAndFetchNextChunk();

    if (null == chunk) {
      return -1;
    }

    return chunk[chunkPosition++] & 0xff;
  }

  @Override
  public int read(byte[] b, int off, int len) throws IOException {
    assertOpen();

    checkAndFetchNextChunk();

    if (null == chunk) {
      return -1;
    }

    int read = 0;
    while (len > 0) {
      final int min = Math.min(remaining(), len);
      System.arraycopy(chunk, chunkPosition, b, off, min);
      off += min;
      len -= min;
      chunkPosition += min;
      read += min;

      if (len > 0) {
        // we read to the end of the chunk, fetch another.
        nextChunk();
        if (null == chunk) {
          break;
        }
      }
    }

    return read;
  }

  @Override
  public int available() throws IOException {
    assertOpen();

    return remaining();
  }

  @Override
  public void close() throws IOException {
    if (open) {
      open = false;
    }
  }

  private int remaining() {
    return chunk.length - chunkPosition;
  }

  private void checkAndFetchNextChunk() {
    if (null != chunk && remaining() <= 0) {
      nextChunk();
    }
  }

  private void nextChunk() {
    chunk = file.getFileSystem().getChunk(this.file, chunkId++);
    chunkPosition = 0;
  }

  private void assertOpen() throws IOException {
    if (!open) {
      throw new IOException("Closed");
    }
  }
}
