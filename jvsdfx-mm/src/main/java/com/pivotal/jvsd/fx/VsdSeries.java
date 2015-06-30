package com.pivotal.jvsd.fx;

import java.nio.MappedByteBuffer;
import java.util.Observable;
import java.util.Observer;

public class VsdSeries  {
  // TODO concurrency
  
  private MappedByteBuffer buffer = null;
  
  private int length = 0;
  
  
  
  
  public void addData(long time, double value) {
    if (buffer.remaining() < 8) {
      expandBuffer();
    }
    
    buffer.putLong(time);
    buffer.putDouble(value);
    length++;
    
    // TODO notify change
  }

  public int getLength() {
    return length;
  }

  private void expandBuffer() {
    // TODO Auto-generated method stub
    
  }
}
