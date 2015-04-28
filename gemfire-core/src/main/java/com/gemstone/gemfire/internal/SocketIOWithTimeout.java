/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Pivotal Additions:
 * Using a ConcurrentHashMap with a LinkedBlockingDeque instead
 * Added a cleanup thread
 * Modifications to trimIdleSelector
 * 
 */

package com.gemstone.gemfire.internal;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectableChannel;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.SocketChannel;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import com.gemstone.gemfire.internal.logging.LogService;

/**
 * This supports input and output streams for a socket channels. 
 * These streams can have a timeout.
 */
public abstract class SocketIOWithTimeout {
  
  private static final Logger logger = LogService.getLogger();
  
  private SelectableChannel channel;
  private long timeout;
  private boolean closed = false;
  
  /*Pivotal Change to final*/
  private static final SelectorPool selector = new SelectorPool();
  
  /*Pivotal Addition*/
  //in seconds, the cleanup thread will first mark at the SELECTOR_TIME_OUT interval and
  //close any selectors that have been marked on the next pass through
  //This means that it will take approximately twice as long as SELECTOR_TIME_OUT to actually close
  //an unused selector
  private static final long SELECTOR_TIME_OUT = Long.getLong("gemfire.SELECTOR_TIME_OUT", 120L); 
  
  private static ScheduledExecutorService cleanUpExecutor = startSelectorCleanUpThread();

  /*End Pivotal Addition*/
  
  /* A timeout value of 0 implies wait for ever. 
   * We should have a value of timeout that implies zero wait.. i.e. 
   * read or write returns immediately.
   * 
   * This will set channel to non-blocking.
   */
  SocketIOWithTimeout(SelectableChannel channel, long timeout) 
                                                 throws IOException {
    checkChannelValidity(channel);
    
    this.channel = channel;
    this.timeout = timeout;
    // Set non-blocking
    channel.configureBlocking(false);
  }
  
  void close() {
    closed = true;
  }

  boolean isOpen() {
    return !closed && channel.isOpen();
  }

  SelectableChannel getChannel() {
    return channel;
  }
  
  /** 
   * Utility function to check if channel is ok.
   * Mainly to throw IOException instead of runtime exception
   * in case of mismatch. This mismatch can occur for many runtime
   * reasons.
   */
  static void checkChannelValidity(Object channel) throws IOException {
    if (channel == null) {
      /* Most common reason is that original socket does not have a channel.
       * So making this an IOException rather than a RuntimeException.
       */
      throw new IOException("Channel is null. Check " +
                            "how the channel or socket is created.");
    }
    
    if (!(channel instanceof SelectableChannel)) {
      throw new IOException("Channel should be a SelectableChannel");
    }    
  }
  
  /**
   * Performs actual IO operations. This is not expected to block.
   *  
   * @param buf
   * @return number of bytes (or some equivalent). 0 implies underlying
   *         channel is drained completely. We will wait if more IO is 
   *         required.
   * @throws IOException
   */
  abstract int performIO(ByteBuffer buf) throws IOException;  
  
  /**
   * Performs one IO and returns number of bytes read or written.
   * It waits up to the specified timeout. If the channel is 
   * not read before the timeout, SocketTimeoutException is thrown.
   * 
   * @param buf buffer for IO
   * @param ops Selection Ops used for waiting. Suggested values: 
   *        SelectionKey.OP_READ while reading and SelectionKey.OP_WRITE while
   *        writing. 
   *        
   * @return number of bytes read or written. negative implies end of stream.
   * @throws IOException
   */
  int doIO(ByteBuffer buf, int ops) throws IOException {
    
    /* For now only one thread is allowed. If user want to read or write
     * from multiple threads, multiple streams could be created. In that
     * case multiple threads work as well as underlying channel supports it.
     */
    if (!buf.hasRemaining()) {
      throw new IllegalArgumentException("Buffer has no data left.");
      //or should we just return 0?
    }

    while (buf.hasRemaining()) {
      if (closed) {
        return -1;
      }

      try {
        int n = performIO(buf);
        if (n != 0) {
          // successful io or an error.
          return n;
        }
      } catch (IOException e) {
        if (!channel.isOpen()) {
          closed = true;
        }
        throw e;
      }

      //now wait for socket to be ready.
      int count = 0;
      try {
        count = selector.select(channel, ops, timeout);  
      } catch (IOException e) { //unexpected IOException.
        closed = true;
        throw e;
      } 

      if (count == 0) {
        throw new SocketTimeoutException(timeoutExceptionString(channel,
                                                                timeout, ops));
      }
      // otherwise the socket should be ready for io.
    }
    
    return 0; // does not reach here.
  }
  
  /**
   * The contract is similar to {@link SocketChannel#connect(SocketAddress)} 
   * with a timeout.
   * 
   * @see SocketChannel#connect(SocketAddress)
   * 
   * @param channel - this should be a {@link SelectableChannel}
   * @param endpoint
   * @throws IOException
   */
  static void connect(SocketChannel channel, 
                      SocketAddress endpoint, int timeout) throws IOException {
    
    boolean blockingOn = channel.isBlocking();
    if (blockingOn) {
      channel.configureBlocking(false);
    }
    
    try { 
      if (channel.connect(endpoint)) {
        return;
      }

      long timeoutLeft = timeout;
      long endTime = (timeout > 0) ? (System.currentTimeMillis() + timeout): 0;
      
      while (true) {
        // we might have to call finishConnect() more than once
        // for some channels (with user level protocols)
        
        int ret = selector.select((SelectableChannel)channel, 
                                  SelectionKey.OP_CONNECT, timeoutLeft);
        
        if (ret > 0 && channel.finishConnect()) {
          return;
        }
        
        if (ret == 0 ||
            (timeout > 0 &&  
              (timeoutLeft = (endTime - System.currentTimeMillis())) <= 0)) {
          throw new SocketTimeoutException(
                    timeoutExceptionString(channel, timeout, 
                                           SelectionKey.OP_CONNECT));
        }
      }
    } catch (IOException e) {
      // javadoc for SocketChannel.connect() says channel should be closed.
      try {
        channel.close();
      } catch (IOException ignored) {}
      throw e;
    } finally {
      if (blockingOn && channel.isOpen()) {
        channel.configureBlocking(true);
      }
    }
  }
  
  /**
   * This is similar to doIO(ByteBuffer, int)} except that it
   * does not perform any I/O. It just waits for the channel to be ready
   * for I/O as specified in ops.
   * 
   * @param ops Selection Ops used for waiting
   * 
   * @throws SocketTimeoutException 
   *         if select on the channel times out.
   * @throws IOException
   *         if any other I/O error occurs. 
   */
  void waitForIO(int ops) throws IOException {
    
    if (selector.select(channel, ops, timeout) == 0) {
      throw new SocketTimeoutException(timeoutExceptionString(channel, timeout,
                                                              ops)); 
    }
  }

  public void setTimeout(long timeoutMs) {
    this.timeout = timeoutMs;
  }
    
  private static String timeoutExceptionString(SelectableChannel channel,
                                               long timeout, int ops) {
    
    String waitingFor;
    switch(ops) {
    
    case SelectionKey.OP_READ :
      waitingFor = "read"; break;
      
    case SelectionKey.OP_WRITE :
      waitingFor = "write"; break;      
      
    case SelectionKey.OP_CONNECT :
      waitingFor = "connect"; break;
      
    default :
      waitingFor = "" + ops;  
    }
    
    return timeout + " millis timeout while " +
           "waiting for channel to be ready for " + 
           waitingFor + ". ch : " + channel;    
  }
  
  public static ScheduledExecutorService startSelectorCleanUpThread() {
    ScheduledExecutorService cleanUpExecutor = Executors.newScheduledThreadPool(1, new ThreadFactory() {
      public Thread newThread(final Runnable r) {
        Thread result = new Thread(r, "selector-pool-cleanup");
        result.setDaemon(true);
        return result;
      }
    });
    cleanUpExecutor.scheduleAtFixedRate(new Runnable(){
        public void run() {
          selector.trimIdleSelectors();
        }
      }, SELECTOR_TIME_OUT, SELECTOR_TIME_OUT, TimeUnit.SECONDS);
    return cleanUpExecutor;
  }
  
  public static void stopSelectorCleanUpThread() {
    if (cleanUpExecutor != null) {
      cleanUpExecutor.shutdownNow();
    }
  }
  /**
   * This maintains a pool of selectors. These selectors are closed
   * once they are idle (unused) for a few seconds.
   */
  private static class SelectorPool {
    
    private static class SelectorInfo {
      Selector              selector;
      /**Pivotal Addition**/
      LinkedBlockingDeque<SelectorInfo> deque;
      volatile boolean markForClean = false;
      /**End Pivotal Addition**/
 
      void close() {
        if (selector != null) {
          try {
            selector.close();
          } catch (IOException e) {
            logger.warn("Unexpected exception while closing selector : ", e);
          }
        }
      }    
    }
    
    private final ConcurrentHashMap<SelectorProvider, LinkedBlockingDeque<SelectorInfo>> providerList = new ConcurrentHashMap<SelectorProvider, LinkedBlockingDeque<SelectorInfo>>();
    
    /**
     * Waits on the channel with the given timeout using one of the 
     * cached selectors. It also removes any cached selectors that are
     * idle for a few seconds.
     * 
     * @param channel
     * @param ops
     * @param timeout
     * @throws IOException
     */
    int select(SelectableChannel channel, int ops, long timeout) 
                                                   throws IOException {
     
      SelectorInfo info = get(channel);
      
      SelectionKey key = null;
      int ret = 0;
      
      try {
        while (true) {
          long start = (timeout == 0) ? 0 : System.currentTimeMillis();

          key = channel.register(info.selector, ops);
          ret = info.selector.select(timeout);
          
          if (ret != 0) {
            return ret;
          }
          
          /* Sometimes select() returns 0 much before timeout for 
           * unknown reasons. So select again if required.
           */
          if (timeout > 0) {
            timeout -= System.currentTimeMillis() - start;
            if (timeout <= 0) {
              return 0;
            }
          }
          
          if (Thread.currentThread().isInterrupted()) {
            throw new InterruptedIOException("Interruped while waiting for " +
                                             "IO on channel " + channel +
                                             ". " + timeout + 
                                             " millis timeout left.");
          }
        }
      } finally {
        if (key != null) {
          key.cancel();
        }
        
        //clear the canceled key.
        try {
          info.selector.selectNow();
        } catch (IOException e) {
          logger.info("Unexpected Exception while clearing selector : ", e); // TODO:WTF: why is this info level??
          // don't put the selector back.
          info.close();
          return ret; 
        }
        
        release(info);
      }
    }

    /**
     * Takes one selector from end of LRU list of free selectors.
     * If there are no selectors awailable, it creates a new selector.
     * 
     * @param channel 
     * @throws IOException
     */
    private SelectorInfo get(SelectableChannel channel) 
                                                         throws IOException {
      SelectorProvider provider = channel.provider();
      
      /**Pivotal Change**/
      LinkedBlockingDeque<SelectorInfo> deque = providerList.get(provider);
      if (deque == null) {
        deque = new LinkedBlockingDeque<SelectorInfo>();
        LinkedBlockingDeque<SelectorInfo> presentValue = providerList.putIfAbsent(provider, deque); 
        if (presentValue != null && deque != presentValue) {
          deque = presentValue;
        }
      } 
      /**poll instead of check empty**/       
      
      SelectorInfo selInfo = deque.pollFirst(); 
      if (selInfo != null) {
        selInfo.markForClean = false;
      } else {
        Selector selector = provider.openSelector();
        selInfo = new SelectorInfo();
        selInfo.selector = selector;
        selInfo.deque = deque;
      }
      
      /**end Pivotal Change**/
      return selInfo;
    }
    
    /**
     * puts selector back at the end of LRU list of free selectos.
     * 
     * @param info
     */
    private void release(SelectorInfo info) {
      /**Pivotal Addition **/
      info.deque.addFirst(info);
      /**End Pivotal Addition **/
    }
    
    private void trimIdleSelectors() {
      Iterator<LinkedBlockingDeque<SocketIOWithTimeout.SelectorPool.SelectorInfo>> poolIterator = providerList.values().iterator();
      while (poolIterator.hasNext()) {
        LinkedBlockingDeque<SelectorInfo> selectorPool = poolIterator.next();
        trimSelectorPool(selectorPool);
      }
    }
        
    private void trimSelectorPool(LinkedBlockingDeque<SelectorInfo> selectorPool) {
      SelectorInfo selectorInfo = selectorPool.peekLast();
      //iterate backwards and remove any selectors that have been marked
      //once we hit a selector that has yet to be marked, we can then mark the remaining
      while (selectorInfo != null && selectorInfo.markForClean) {
        selectorInfo = selectorPool.pollLast();
        //check the flag again just to be sure
        if (selectorInfo.markForClean ) {
          selectorInfo.close();
        }
        else {
          selectorPool.addFirst(selectorInfo);
        }
        selectorInfo = selectorPool.peekLast();
      }
      
      //Mark all the selectors
      Iterator<SelectorInfo> selectorIterator = selectorPool.iterator();
      while (selectorIterator.hasNext()) {
        selectorIterator.next().markForClean = true;
      }
    }
  }
}
