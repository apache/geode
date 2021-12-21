/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode.internal.cache.persistence;

import java.io.File;
import java.io.FileDescriptor;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;

public class UninterruptibleRandomAccessFile {
  private RandomAccessFile raf;
  private final UninterruptibleFileChannelImpl channel;
  private final File file;
  private final String mode;
  private boolean isClosed;

  public UninterruptibleRandomAccessFile(File file, String mode) throws FileNotFoundException {
    this.file = file;
    this.mode = mode;
    raf = new RandomAccessFile(file, mode);
    channel = new UninterruptibleFileChannelImpl();
  }

  public UninterruptibleFileChannel getChannel() {
    return channel;
  }

  public synchronized void reopen(long lastPosition) throws IOException {
    if (isClosed) {
      throw new IOException("Random Access File is closed");
    }
    try {
      raf.close();
    } catch (IOException e) {
      // ignore
    }
    raf = new RandomAccessFile(file, mode);
    raf.seek(lastPosition);
  }

  public synchronized void close() throws IOException {
    isClosed = true;
    raf.close();
  }

  public synchronized void setLength(long newLength) throws IOException {
    raf.setLength(newLength);

  }

  public synchronized FileDescriptor getFD() throws IOException {
    return raf.getFD();
  }

  public synchronized long getFilePointer() throws IOException {
    return raf.getFilePointer();
  }

  public synchronized void seek(long readPosition) throws IOException {
    raf.seek(readPosition);

  }

  public synchronized void readFully(byte[] valueBytes) throws IOException {
    raf.readFully(valueBytes);

  }

  public synchronized void readFully(byte[] valueBytes, int i, int valueLength) throws IOException {
    raf.readFully(valueBytes, i, valueLength);

  }

  public synchronized long length() throws IOException {
    return raf.length();
  }

  private interface FileOperation {
    long doOp(FileChannel channel) throws IOException;
  }

  private class UninterruptibleFileChannelImpl implements UninterruptibleFileChannel {

    private FileChannel delegate() {
      return raf.getChannel();
    }

    /**
     * Perform an operation on the file, reopening the file and redoing the operation if necessary
     * if we are interrupted in the middle of the operation
     */
    private long doUninterruptibly(FileOperation op) throws IOException {
      boolean interrupted = false;
      try {
        synchronized (UninterruptibleRandomAccessFile.this) {
          while (true) {
            interrupted |= Thread.interrupted();
            FileChannel d = delegate();
            long lastPosition = getFilePointer();
            try {
              return op.doOp(d);
            } catch (ClosedByInterruptException e) {
              interrupted = true;
              reopen(lastPosition);
            }
          }
        }
      } finally {
        if (interrupted) {
          Thread.currentThread().interrupt();
        }
      }
    }

    @Override
    public long read(final ByteBuffer[] dsts, final int offset, final int length)
        throws IOException {
      return doUninterruptibly(new FileOperation() {
        @Override
        public long doOp(FileChannel channel) throws IOException {
          return channel.read(dsts, offset, length);
        }
      });
    }

    @Override
    public long read(final ByteBuffer[] dsts) throws IOException {
      return doUninterruptibly(new FileOperation() {
        @Override
        public long doOp(FileChannel channel) throws IOException {
          return channel.read(dsts);
        }
      });
    }

    @Override
    public long write(final ByteBuffer[] srcs, final int offset, final int length)
        throws IOException {
      return doUninterruptibly(new FileOperation() {
        @Override
        public long doOp(FileChannel channel) throws IOException {
          return channel.write(srcs, offset, length);
        }
      });
    }

    @Override
    public long write(final ByteBuffer[] srcs) throws IOException {
      return doUninterruptibly(new FileOperation() {
        @Override
        public long doOp(FileChannel channel) throws IOException {
          return channel.write(srcs);
        }
      });
    }

    @Override
    public int read(final ByteBuffer dst) throws IOException {
      return (int) doUninterruptibly(new FileOperation() {
        @Override
        public long doOp(FileChannel channel) throws IOException {
          return channel.read(dst);
        }
      });
    }

    @Override
    public int write(final ByteBuffer src) throws IOException {
      return (int) doUninterruptibly(new FileOperation() {
        @Override
        public long doOp(FileChannel channel) throws IOException {
          return channel.write(src);
        }
      });
    }

    @Override
    public long position() throws IOException {
      return doUninterruptibly(new FileOperation() {
        @Override
        public long doOp(FileChannel channel) throws IOException {
          return channel.position();
        }
      });
    }

    @Override
    public SeekableByteChannel position(final long newPosition) throws IOException {
      doUninterruptibly(new FileOperation() {
        @Override
        public long doOp(FileChannel channel) throws IOException {
          channel.position(newPosition);
          return 0;
        }
      });
      return this;
    }

    @Override
    public long size() throws IOException {
      return doUninterruptibly(new FileOperation() {
        @Override
        public long doOp(FileChannel channel) throws IOException {
          return channel.size();
        }
      });
    }

    @Override
    public SeekableByteChannel truncate(final long size) throws IOException {
      doUninterruptibly(new FileOperation() {
        @Override
        public long doOp(FileChannel channel) throws IOException {
          channel.truncate(size);
          return 0;
        }
      });
      return this;
    }

    @Override
    public boolean isOpen() {
      return delegate().isOpen();
    }

    @Override
    public void close() throws IOException {
      UninterruptibleRandomAccessFile.this.close();
    }

    @Override
    public void force(final boolean b) throws IOException {
      doUninterruptibly(new FileOperation() {
        @Override
        public long doOp(FileChannel channel) throws IOException {
          channel.force(b);
          return 0;
        }
      });
    }

  }
}
