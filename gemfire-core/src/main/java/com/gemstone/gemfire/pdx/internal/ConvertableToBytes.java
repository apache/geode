package com.gemstone.gemfire.pdx.internal;

import java.io.IOException;

public interface ConvertableToBytes {
  public byte[] toBytes() throws IOException;
}
