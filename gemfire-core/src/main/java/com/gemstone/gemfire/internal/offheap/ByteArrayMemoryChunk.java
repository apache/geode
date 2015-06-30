package com.gemstone.gemfire.internal.offheap;

/**
 * The read and write methods on this implementation will throw ArrayIndexOutOfBoundsException
 * if the offset extends past the end of the underlying array of if an attempt is made to read or write past the end of the array.
 * 
 * @author darrel
 * @since 9.0
 */
public class ByteArrayMemoryChunk implements MemoryChunk {

  private final byte[] data;
  
  public ByteArrayMemoryChunk(int size) {
    this.data = new byte[size];
  }
  
  @Override
  public int getSize() {
    return this.data.length;
  }

  @Override
  public byte readByte(int offset) {
    return this.data[offset];
  }

  @Override
  public void writeByte(int offset, byte value) {
    this.data[offset] = value;
  }

  @Override
  public void readBytes(int offset, byte[] bytes) {
    readBytes(offset, bytes, 0, bytes.length);
  }

  @Override
  public void writeBytes(int offset, byte[] bytes) {
    writeBytes(offset, bytes, 0, bytes.length);
  }

  @Override
  public void readBytes(int offset, byte[] bytes, int bytesOffset, int size) {
    System.arraycopy(this.data, offset, bytes, bytesOffset, size);
  }

  @Override
  public void writeBytes(int offset, byte[] bytes, int bytesOffset, int size) {
    System.arraycopy(bytes, bytesOffset, this.data, offset, size);
  }

  @Override
  public void release() {
  }

  @Override
  public void copyBytes(int src, int dst, int size) {
    System.arraycopy(this.data, src, this.data, dst, size);
  }
}
