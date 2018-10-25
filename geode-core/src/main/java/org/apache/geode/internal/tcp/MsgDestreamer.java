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
package org.apache.geode.internal.tcp;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;

import org.apache.geode.CancelCriterion;
import org.apache.geode.InternalGemFireError;
import org.apache.geode.LogWriter;
import org.apache.geode.SystemFailure;
import org.apache.geode.distributed.DistributedSystemDisconnectedException;
import org.apache.geode.distributed.internal.DMStats;
import org.apache.geode.distributed.internal.DistributionMessage;
import org.apache.geode.distributed.internal.InternalDistributedSystem;
import org.apache.geode.distributed.internal.ReplyProcessor21;
import org.apache.geode.internal.InternalDataSerializer;
import org.apache.geode.internal.Version;
import org.apache.geode.internal.VersionedDataInputStream;

/**
 * <p>
 * MsgDestreamer supports destreaming a streamed message from a tcp Connection that arrives in
 * chunks. This allows us to receive a message without needing to read it completely into a buffer
 * before we can start deserializing it.
 *
 * @since GemFire 5.0.2
 *
 */

public class MsgDestreamer {
  /**
   * If an exception occurs during deserialization of the message it will be recorded here.
   */
  private Throwable failure;
  /**
   * Used to store the deserialized message on success.
   */
  private DistributionMessage result;
  /**
   * The current failed messages reply processor id if it has one
   */
  private int RPid;
  /**
   * The thread that will be doing the deserialization of the message.
   */
  private final DestreamerThread t;

  private int size;

  final CancelCriterion stopper;

  final Version version;

  public MsgDestreamer(DMStats stats, CancelCriterion stopper, Version v) {
    this.stopper = stopper;
    this.t = new DestreamerThread(stats, stopper);
    this.version = v;
    init();
  }

  private void init() {
    this.t.start();
  }

  public void close() {
    reset();
    this.t.close();
  }

  public void reset() {
    synchronized (this) {
      this.failure = null;
      this.result = null;
    }
    this.size = 0;
    this.t.setName("IDLE p2pDestreamer");
  }

  public void setName(String name) {
    this.t.setName("p2pDestreamer for " + name);
  }

  private void waitUntilDone() throws InterruptedException {
    if (this.t.isClosed() || Thread.interrupted())
      throw new InterruptedException();
    synchronized (this) {
      while (this.failure == null && this.result == null) {
        if (this.t.isClosed() || Thread.interrupted())
          throw new InterruptedException();
        this.wait(); // spurious wakeup ok
      }
    }
  }

  // private final String me = "MsgDestreamer<" + System.identityHashCode(this) + ">";

  // public String toString() {
  // return this.me;
  // }
  // private void logit(String s) {
  // LogWriterI18n l = getLogger();
  // if (l != null) {
  // l.fine(this + ": " + s);
  // }
  // }
  // private LogWriterI18n getLogger() {
  // LogWriterI18n result = null;
  // DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
  // if (ds != null) {
  // result = ds.getLogWriter();
  // }
  // return result;
  // }

  /**
   * Adds a chunk to be deserialized
   *
   * @param bb contains the bytes of the chunk
   * @param length the number of bytes in bb that are this chunk
   */
  public void addChunk(ByteBuffer bb, int length) throws IOException {
    // if this destreamer has failed or this chunk is empty just return
    if (this.failure == null && length > 0) {
      // logit("addChunk bb length=" + length);
      this.t.addChunk(bb, length);
      this.size += length;
    }
  }

  /**
   * Adds a chunk to be deserialize
   *
   * @param b a byte array contains the bytes of the chunk
   */
  public void addChunk(byte[] b) throws IOException {
    // if this destreamer has failed or this chunk is empty just return
    if (this.failure == null && b != null && b.length > 0) {
      // logit("addChunk length=" + b.length);
      ByteBuffer bb = ByteBuffer.wrap(b);
      this.t.addChunk(bb, b.length);
      this.size += b.length;
    }
  }

  /**
   * Returns the number of bytes added to this destreamer.
   */
  public int size() {
    return this.size;
  }

  /**
   * Waits for the deserialization to complete and returns the deserialized message.
   *
   * @throws IOException A problem occurred while deserializing the message.
   * @throws ClassNotFoundException The class of an object read from <code>in</code> could not be
   *         found
   */
  public DistributionMessage getMessage()
      throws InterruptedException, IOException, ClassNotFoundException {
    // if (Thread.interrupted()) throw new InterruptedException(); not necessary done in
    // waitUntilDone
    // this.t.join();
    waitUntilDone();
    if (this.failure != null) {
      // logit("failed with" + this.failure);
      if (this.failure instanceof ClassNotFoundException) {
        throw (ClassNotFoundException) this.failure;
      } else if (this.failure instanceof IOException) {
        throw (IOException) this.failure;
      } else {
        IOException io =
            new IOException("failure during message deserialization");
        io.initCause(this.failure);
        throw io;
      }
    } else {
      // logit("result =" + this.result);
      return this.result;
    }
  }

  /**
   * Returns the reply processor id for the current failed message. Returns 0 if it does not have
   * one. Note this method should only be called after getMessage has thrown an exception.
   */
  public int getRPid() {
    return this.RPid;
  }

  protected void setFailure(Throwable ex, int RPid) {
    synchronized (this) {
      this.failure = ex;
      this.RPid = RPid;
      this.notify();
    }
  }

  protected void setResult(DistributionMessage msg) {
    synchronized (this) {
      this.result = msg;
      this.RPid = 0;
      this.notify();
    }
  }

  /**
   * Thread used to deserialize chunks into a message.
   */
  private class DestreamerThread extends Thread {
    private volatile boolean closed = false;
    final DestreamerIS is;
    final DMStats stats;

    public DestreamerThread(DMStats stats, CancelCriterion stopper) {
      setDaemon(true);
      super.setName("IDLE p2pDestreamer");
      this.is = new DestreamerIS(this, stopper);
      this.stats = stats;
    }
    // private final String me = "DestreamerThread<" + System.identityHashCode(this) + ">";
    // public String toString() {
    // return this.me;
    // }

    public void addChunk(ByteBuffer chunk, int bbLength) throws IOException {
      ByteBuffer bb = chunk.slice();
      bb.limit(bbLength);
      this.is.addChunk(bb);
    }

    @Override
    public void run() {
      for (;;) {
        if (isClosed()) {
          return;
        }
        try {
          ReplyProcessor21.initMessageRPId();
          final Version v = version;
          DataInputStream dis =
              v == null ? new DataInputStream(this.is) : new VersionedDataInputStream(this.is, v);
          long startSer = this.stats.startMsgDeserialization();
          setResult((DistributionMessage) InternalDataSerializer.readDSFID(dis));
          this.stats.endMsgDeserialization(startSer);
        } catch (VirtualMachineError err) {
          SystemFailure.initiateFailure(err);
          // If this ever returns, rethrow the error. We're poisoned
          // now, so don't let this thread continue.
          throw err;
        } catch (Throwable ex) {
          // Whenever you catch Error or Throwable, you must also
          // catch VirtualMachineError (see above). However, there is
          // _still_ a possibility that you are dealing with a cascading
          // error condition, so you also need to check to see if the JVM
          // is still usable:
          SystemFailure.checkFailure();
          setFailure(ex, ReplyProcessor21.getMessageRPId());
        } finally {
          this.is.close();
          ReplyProcessor21.clearMessageRPId();
        }
      }
    }

    public void close() {
      this.closed = true;
      interrupt();
    }

    public boolean isClosed() {
      return this.closed;
    }
  }
  /**
   * This input stream waits for data to be available. Once it is provided, by a call to addChunk,
   * it will stream the data in from that chunk, signal that is has completed, and then wait for
   * another chunk.
   */
  private static class DestreamerIS extends InputStream {
    final Object dataMon = new Object();
    final Object doneMon = new Object();
    ByteBuffer data;
    final DestreamerThread owner;
    final CancelCriterion stopper;

    private class Stopper extends CancelCriterion {
      private final CancelCriterion stopper;

      Stopper(CancelCriterion stopper) {
        this.stopper = stopper;
      }

      /*
       * (non-Javadoc)
       *
       * @see org.apache.geode.CancelCriterion#cancelInProgress()
       */
      @Override
      public String cancelInProgress() {
        String reason = stopper.cancelInProgress();
        if (reason != null) {
          return reason;
        }
        if (owner.isClosed()) {
          return "owner is closed";
        }
        return null;
      }

      /*
       * (non-Javadoc)
       *
       * @see org.apache.geode.CancelCriterion#generateCancelledException(java.lang.Throwable)
       */
      @Override
      public RuntimeException generateCancelledException(Throwable e) {
        String reason = cancelInProgress();
        if (reason == null) {
          return null;
        }
        RuntimeException result = stopper.generateCancelledException(e);
        if (result != null) {
          return result;
        }
        return new DistributedSystemDisconnectedException("owner is closed");
      }
    }

    public DestreamerIS(DestreamerThread t, CancelCriterion stopper) {
      this.owner = t;
      this.data = null;
      this.stopper = new Stopper(stopper);
    }

    // public String toString() {
    // return this.owner.me;
    // }
    // private void logit(String s) {
    // LogWriterI18n l = getLogger();
    // if (l != null) {
    // l.fine(this + ": " + s);
    // }
    // }

    // private LogWriterI18n getLogger() {
    // LogWriterI18n result = null;
    // DistributedSystem ds = InternalDistributedSystem.getAnyInstance();
    // if (ds != null) {
    // result = ds.getLogWriter();
    // }
    // return result;
    // }

    private boolean isClosed() {
      return this.owner.isClosed();
    }

    private ByteBuffer waitForData() throws InterruptedException {
      if (isClosed() || Thread.interrupted())
        throw new InterruptedException();
      synchronized (this.dataMon) {
        ByteBuffer result = this.data;
        while (result == null) {
          if (isClosed() || Thread.interrupted())
            throw new InterruptedException();
          // logit("about to dataMon wait");
          this.dataMon.wait(); // spurious wakeup ok
          // logit("after dataMon wait");
          if (isClosed() || Thread.interrupted())
            throw new InterruptedException();
          result = this.data;
        }
        return result;
      }
    }

    private void provideData(ByteBuffer bb) {
      synchronized (this.dataMon) {
        // if (bb != null) {
        // logit("MDIS: providing bb with " +
        // bb.remaining() + " bytes");
        // }
        this.data = bb;
        // logit("dataMon notify bb=" + bb);
        this.dataMon.notify();
      }
    }

    private void waitUntilDone() throws InterruptedException {
      if (isClosed() || Thread.interrupted())
        throw new InterruptedException();
      synchronized (this.doneMon) {
        while (this.data != null) {
          if (isClosed() || Thread.interrupted())
            throw new InterruptedException();
          // logit("about to doneMon wait");
          this.doneMon.wait(); // spurious wakeup ok
          // logit("after doneMon wait");
          if (isClosed() || Thread.interrupted())
            throw new InterruptedException();
        }
      }
    }

    private void signalDone() {
      synchronized (this.doneMon) {
        this.data = null;
        // logit("doneMon notify");
        this.doneMon.notify();
      }
    }

    public void addChunk(ByteBuffer bb) throws IOException {
      provideData(bb);
      for (;;) {
        stopper.checkCancelInProgress(null);
        boolean interrupted = Thread.interrupted();
        try {
          waitUntilDone();
          break;
        } catch (InterruptedException e) {
          interrupted = true;
        } finally {
          if (interrupted) {
            Thread.currentThread().interrupt();
          }
        }
      }
    }

    private ByteBuffer waitForAvailableData() throws IOException {
      boolean available = false;
      ByteBuffer myData;
      do {
        // only the thread that sets data to null ever does this check
        // so I believe it is ok to do this check outside of sync.
        myData = this.data;
        if (myData == null) {
          for (;;) {
            if (isClosed()) {
              throw new IOException("owner closed"); // TODO
            }
            stopper.checkCancelInProgress(null);
            boolean interrupted = Thread.interrupted();
            try {
              myData = waitForData();
              break;
            } catch (InterruptedException e) {
              interrupted = true;
            } finally {
              if (interrupted) {
                Thread.currentThread().interrupt();
              }
            }
          }

          if (myData == null) {
            // someone must have called close so tell our caller
            // that we were interrupted. This fixes bug 37230.
            stopper.checkCancelInProgress(null);
            throw new InternalGemFireError("bug 37230, please report to support");
          }
          // logit("received new bb with " +
          // myData.remaining() + " bytes");
        }
        int remaining = myData.remaining();
        if (remaining <= 0) {
          signalDone();
        } else {
          available = true;
        }
      } while (!available);
      return myData;
    }

    @Override
    public void close() {
      signalDone();
    }

    /**
     * See the InputStream read method for javadocs. Note that if an attempt to read past the end of
     * the wrapped ByteBuffer is done this method throws BufferUnderflowException
     */
    @Override
    public int read() throws IOException {
      ByteBuffer bb = waitForAvailableData();
      // logit("read result=" + result);
      return (bb.get() & 0xff);
    }


    /*
     * this method is not thread safe See the InputStream read method for javadocs. Note that if an
     * attempt to read past the end of the wrapped ByteBuffer is done this method throws
     * BufferUnderflowException
     */
    @Override
    public int read(byte b[], int off, int len) throws IOException {
      ByteBuffer bb = waitForAvailableData();
      int remaining = bb.remaining();
      int bytesToRead = len;
      if (remaining < len) {
        bytesToRead = remaining;
      }
      bb.get(b, off, bytesToRead);
      // logit("read[] read=" + bytesToRead);
      return bytesToRead;
    }

    @Override
    public int available() throws IOException {
      ByteBuffer bb = this.data;
      if (bb == null) {
        return 0;
      } else {
        return bb.remaining();
      }
    }

  }

  private static LogWriter getLogger() {
    LogWriter result = null;
    InternalDistributedSystem ids = InternalDistributedSystem.unsafeGetConnectedInstance();
    if (ids != null) {
      result = ids.getLogWriter();
    }
    return result;
  }
}
