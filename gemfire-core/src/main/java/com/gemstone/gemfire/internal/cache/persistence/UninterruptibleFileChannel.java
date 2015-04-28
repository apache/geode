package com.gemstone.gemfire.internal.cache.persistence;

import java.io.IOException;
import java.nio.channels.Channel;
import java.nio.channels.GatheringByteChannel;
import java.nio.channels.ScatteringByteChannel;
import java.nio.channels.SeekableByteChannel;

public interface UninterruptibleFileChannel extends Channel, SeekableByteChannel, GatheringByteChannel, ScatteringByteChannel {

  void force(boolean b) throws IOException;

}