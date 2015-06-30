package com.gemstone.gemfire.internal;

import java.io.DataOutput;
import java.io.IOException;

/**
 * Interface to implement if your class supports sending itself directly to a DataOutput
 * during serialization.
 * Note that you are responsible for sending all the bytes that represent your instance,
 * even bytes describing your class name if those are required.
 * 
 * @author darrel
 * @since 6.6
 */
public interface Sendable {
  /**
   * Take all the bytes in the object and write them to the data output. It needs
   * to be written in the GemFire wire format so that it will deserialize correctly 
   * if DataSerializer.readObject is called.
   * 
   * @param out
   *          the data output to send this object to
   * @throws IOException
   */
  void sendTo(DataOutput out) throws IOException;
}