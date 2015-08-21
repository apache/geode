package com.gemstone.gemfire.cache.lucene.internal.filesystem;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.Arrays;

final class FileOutputStream extends OutputStream {

  private final File file;
  private ByteBuffer buffer;
  private boolean open = true;
  private long length;
  private int chunks;

  public FileOutputStream(final File file) {
    this.file = file;
    buffer = ByteBuffer.allocate(file.getChunkSize());
    this.length = file.length;
    this.chunks = file.chunks;
  }

  @Override
  public void write(final int b) throws IOException {
    assertOpen();
    
    if (buffer.remaining() == 0) {
      flushBuffer();
    }

    buffer.put((byte) b);
    length++;
  }
  
  @Override
  public void write(final byte[] b, int off, int len) throws IOException {
    assertOpen();
    
    while (len > 0) {
      if (buffer.remaining() == 0) {
        flushBuffer();
      }

      final int min = Math.min(buffer.remaining(), len);
      buffer.put(b, off, min);
      off += min;
      len -= min;
      length += min;
    }
  }

  @Override
  public void close() throws IOException {
    if (open) {
      flushBuffer();
      file.modified = System.currentTimeMillis();
      file.length = length;
      file.chunks = chunks;
      file.getFileSystem().updateFile(file);
      open = false;
      buffer = null;
    }
  }

  private void flushBuffer() {
    byte[] chunk = Arrays.copyOfRange(buffer.array(), buffer.arrayOffset(), buffer.position());
    file.getFileSystem().putChunk(file, chunks++, chunk);
    buffer.rewind();
  }

  private void assertOpen() throws IOException {
    if (!open) {
      throw new IOException("Closed");
    }
  }
}
