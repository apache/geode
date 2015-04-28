/** Notice of modification as required by the LGPL
 *  This file was modified by Gemstone Systems Inc. on
 *  $Date$
 **/
// $Id: ConnectionTableNIO.java,v 1.8 2005/11/22 13:56:39 smarlownovell Exp $

package com.gemstone.org.jgroups.blocks;


import com.gemstone.org.jgroups.util.GemFireTracer;
import com.gemstone.org.jgroups.util.ExternalStrings;
import com.gemstone.org.jgroups.Address;
import com.gemstone.org.jgroups.JGroupsVersion;
import com.gemstone.org.jgroups.protocols.TCP_NIO;
import com.gemstone.org.jgroups.stack.IpAddress;

import java.io.IOException;
import java.net.*;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.ClosedSelectorException;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.nio.channels.CancelledKeyException;
import java.nio.channels.spi.SelectorProvider;
import java.util.Iterator;
import java.util.Set;
import java.util.LinkedList;

import com.gemstone.org.jgroups.oswego.concurrent.PooledExecutor;
import com.gemstone.org.jgroups.oswego.concurrent.BoundedBuffer;
import com.gemstone.org.jgroups.oswego.concurrent.LinkedQueue;
import com.gemstone.org.jgroups.oswego.concurrent.FutureResult;
import com.gemstone.org.jgroups.oswego.concurrent.Executor;
import com.gemstone.org.jgroups.oswego.concurrent.DirectExecutor;

/**
 * Manages incoming and outgoing TCP connections. For each outgoing message to destination P, if there
 * is not yet a connection for P, one will be created. Subsequent outgoing messages will use this
 * connection.  For incoming messages, one server socket is created at startup. For each new incoming
 * client connecting, a new thread from a thread pool is allocated and listens for incoming messages
 * until the socket is closed by the peer.<br>Sockets/threads with no activity will be killed
 * after some time.
 * <p/>
 * Incoming messages from any of the sockets can be received by setting the message listener.
 *
 * @author Bela Ban, Scott Marlow, Alex Fu
 */
public class ConnectionTableNIO extends ConnectionTable implements Runnable {

   private ServerSocketChannel m_serverSocketChannel;
   private Selector m_acceptSelector;
   protected final static GemFireTracer LOG = GemFireTracer.getLog(ConnectionTableNIO.class);

   private WriteHandler[] m_writeHandlers;
   private int m_nextWriteHandler = 0;
   private Object m_lockNextWriteHandler = new Object();

   private ReadHandler[] m_readHandlers;
   private int m_nextReadHandler = 0;
   private Object m_lockNextReadHandler = new Object();

   // thread pool for processing read requests
   protected/*GemStoneAddition*/ Executor m_requestProcessors;


   /**
    * @param srv_port
    * @throws Exception
    */
   public ConnectionTableNIO(int srv_port) throws Exception {
       super(srv_port);
   }

   /**
    * @param srv_port
    * @param reaper_interval
    * @param conn_expire_time
    * @throws Exception
    */
   public ConnectionTableNIO(int srv_port, long reaper_interval,
                             long conn_expire_time) throws Exception {
       super(srv_port, reaper_interval, conn_expire_time);
   }

   /**
    * @param r
    * @param bind_addr
    * @param external_addr
    * @param srv_port
    * @param max_port
    * @throws Exception
    */
   public ConnectionTableNIO(Receiver r, InetAddress bind_addr, InetAddress external_addr, int srv_port, int max_port
   )
      throws Exception
   {
      super(r, bind_addr, external_addr, srv_port, max_port);

   }

   /**
    * @param r
    * @param bind_addr
    * @param external_addr
    * @param srv_port
    * @param max_port
    * @param reaper_interval
    * @param conn_expire_time
    * @throws Exception
    */
   public ConnectionTableNIO(Receiver r, InetAddress bind_addr, InetAddress external_addr, int srv_port, int max_port,
                             long reaper_interval, long conn_expire_time
                             ) throws Exception
   {
      super(r, bind_addr, external_addr, srv_port, max_port, reaper_interval, conn_expire_time);
   }

   /**
    * Try to obtain correct Connection (or create one if not yet existent)
    */
   @Override // GemStoneAddition
   ConnectionTable.Connection getConnection(Address dest) throws Exception
   {
      Connection conn = null;
      SocketChannel sock_ch;

      synchronized (conns)
      {
         conn = (Connection) conns.get(dest);
         if (conn == null)
         {
            InetSocketAddress destAddress = new InetSocketAddress(((IpAddress) dest).getIpAddress(),
               ((IpAddress) dest).getPort());
            sock_ch = SocketChannel.open(destAddress);
            conn = new Connection(sock_ch, dest);

            conn.sendLocalAddress(local_addr);
            // This outbound connection is ready
            conn.getReadState().setHandShakingStatus(ConnectionReadState.HANDSHAKINGFIN);

            // Set channel to be non-block only after hand shaking
            try
            {
               sock_ch.configureBlocking(false);
            } catch (IOException e)
            {
               // No way to handle the blocking socket
               conn.destroy();
               throw e;
            }

            try
            {
               if (LOG.isTraceEnabled())
                  LOG.trace("About to change new connection send buff size from " + sock_ch.socket().getSendBufferSize() + " bytes");
               sock_ch.socket().setSendBufferSize(send_buf_size);
               if (LOG.isTraceEnabled())
                  LOG.trace("Changed new connection send buff size to " + sock_ch.socket().getSendBufferSize() + " bytes");
            }
            catch (IllegalArgumentException ex)
            {
               if (log.isErrorEnabled()) log.error("exception setting send buffer size to " +
                  send_buf_size + " bytes: " + ex);
            }
            try
            {
               if (LOG.isTraceEnabled())
                  LOG.trace("About to change new connection receive buff size from " + sock_ch.socket().getReceiveBufferSize() + " bytes");
               sock_ch.socket().setReceiveBufferSize(recv_buf_size);
               if (LOG.isTraceEnabled())
                  LOG.trace("Changed new connection receive buff size to " + sock_ch.socket().getReceiveBufferSize() + " bytes");
            }
            catch (IllegalArgumentException ex)
            {
               if (log.isErrorEnabled()) log.error("exception setting receive buffer size to " +
                  send_buf_size + " bytes: " + ex);
            }

            int idx;
            synchronized (m_lockNextWriteHandler)
            {
               idx = m_nextWriteHandler = (m_nextWriteHandler + 1) % m_writeHandlers.length;
            }
            conn.setupWriteHandler(m_writeHandlers[idx]);

            // Put the new connection to the queue
            try
            {
               synchronized (m_lockNextReadHandler)
               {
                  idx = m_nextReadHandler = (m_nextReadHandler + 1) % m_readHandlers.length;
               }
               m_readHandlers[idx].add(conn);

            } catch (InterruptedException e)
            {
               if (LOG.isWarnEnabled())
                  LOG.warn("Thread (" +Thread.currentThread().getName() + ") was interrupted, closing connection", e);
               // What can we do? Remove it from table then.
               conn.destroy();
               throw e;
            }

            // Add connection to table
            addConnection(dest, conn);

            notifyConnectionOpened(dest);
            if (LOG.isInfoEnabled()) LOG.info(ExternalStrings.ConnectionTableNIO_CREATED_SOCKET_TO__0, dest);
         }
         return conn;
      }
   }

   @Override // GemStoneAddition
   protected void init()
      throws Exception
   {

      TCP_NIO NIOreceiver = (TCP_NIO)receiver;
      // use directExector if max thread pool size is less than or equal to zero.
      if(NIOreceiver.getProcessorMaxThreads() <= 0) {
         m_requestProcessors = new DirectExecutor();
      }
      else
      {
         // Create worker thread pool for processing incoming buffers
         PooledExecutor requestProcessors = new PooledExecutor(new BoundedBuffer(NIOreceiver.getProcessorQueueSize()), NIOreceiver.getProcessorMaxThreads());
         requestProcessors.setMinimumPoolSize(NIOreceiver.getProcessorMinThreads());
         requestProcessors.setKeepAliveTime(NIOreceiver.getProcessorKeepAliveTime());
         requestProcessors.waitWhenBlocked();
         requestProcessors.createThreads(NIOreceiver.getProcessorThreads());
         m_requestProcessors = requestProcessors;
      }

      m_writeHandlers = WriteHandler.create(NIOreceiver.getWriterThreads());
      m_readHandlers = new ReadHandler[NIOreceiver.getReaderThreads()];
      for (int i = 0; i < m_readHandlers.length; i++)
         m_readHandlers[i] = new ReadHandler();
   }


   /**
    * Closes all open sockets, the server socket and all threads waiting for incoming messages
    */
   @Override // GemStoneAddition
   public void stop()
   {
      if (m_serverSocketChannel.isOpen())
      {
         try
         {
            m_serverSocketChannel.close();
         }
         catch (Exception eat)
         {

         }
      }

      // Stop the main selector
      m_acceptSelector.wakeup();
      // Stop selector threads
      for (int i = 0; i < m_readHandlers.length; i++)
      {
         try
         {
            m_readHandlers[i].add(new Shutdown());
         } catch (InterruptedException e)
         {
            LOG.error(ExternalStrings.ConnectionTableNIO_THREAD__0__WAS_INTERRUPTED_FAILED_TO_SHUTDOWN_SELECTOR, Thread.currentThread().getName(), e);
            // GemStoneAddition (comment)
            // Ignore, do not reset interrupt bit.  We are trying to stop.
         }
      }
      for (int i = 0; i < m_writeHandlers.length; i++)
      {
         try
         {
            m_writeHandlers[i].QUEUE.put(new Shutdown());
            m_writeHandlers[i].m_selector.wakeup();
         } catch (InterruptedException e)
         {
            LOG.error(ExternalStrings.ConnectionTableNIO_THREAD__0__WAS_INTERRUPTED_FAILED_TO_SHUTDOWN_SELECTOR, Thread.currentThread().getName(), e);
            // GemStoneAddition (comment)
            // Ignore, do not reset interrupt bit.  We are trying to stop.
         }
      }

      // Stop the callback thread pool
      if(m_requestProcessors instanceof PooledExecutor)
         ((PooledExecutor)m_requestProcessors).shutdownNow();

      super.stop();

   }

   /**
    * Acceptor thread. Continuously accept new connections and assign readhandler/writehandler
    * to them.
    */
   @Override // GemStoneAddition
   public void run()
   {
      Connection conn = null;

      DO_WORK: // GemStoneAddition
      while (m_serverSocketChannel.isOpen())
      {
         int num = 0;
         try
         {
            num = m_acceptSelector.select();
         } catch (IOException e)
         {
            if (LOG.isWarnEnabled())
               LOG.warn("Select operation on listening socket failed", e);
            continue;   // Give up this time
         }

         if (num > 0)
         {
            Set readyKeys = m_acceptSelector.selectedKeys();
            for (Iterator i = readyKeys.iterator(); i.hasNext();)
            {
               SelectionKey key = (SelectionKey) i.next();
               i.remove();
               // We only deal with new incoming connections
               //assert key.isAcceptable();

               ServerSocketChannel readyChannel = (ServerSocketChannel) key.channel();
               SocketChannel client_sock_ch = null;
               try
               {
                  client_sock_ch = readyChannel.accept();
               } catch (IOException e)
               {
                  if (LOG.isWarnEnabled())
                     LOG.warn("Attempt to accept new connection from listening socket failed" , e);
                  // Give up this connection
                  continue;
               }

               if (LOG.isInfoEnabled())
                  LOG.info(ExternalStrings.ConnectionTableNIO_ACCEPTED_CONNECTION_CLIENT_SOCK_0, client_sock_ch.socket());

               try
               {
                  if (LOG.isTraceEnabled())
                     LOG.trace("About to change new connection send buff size from " + client_sock_ch.socket().getSendBufferSize() + " bytes");
                  client_sock_ch.socket().setSendBufferSize(send_buf_size);
                  if (LOG.isTraceEnabled())
                     LOG.trace("Changed new connection send buff size to " + client_sock_ch.socket().getSendBufferSize() + " bytes");
               }
               catch (IllegalArgumentException ex)
               {
                  if (log.isErrorEnabled()) log.error("exception setting send buffer size to " +
                     send_buf_size + " bytes: " ,ex);
               }
               catch (SocketException e)
               {
                  if (log.isErrorEnabled()) log.error("exception setting send buffer size to " +
                     send_buf_size + " bytes: " , e);
               }
               try
               {
                  if (LOG.isTraceEnabled())
                     LOG.trace("About to change new connection receive buff size from " + client_sock_ch.socket().getReceiveBufferSize() + " bytes");
                  client_sock_ch.socket().setReceiveBufferSize(recv_buf_size);
                  if (LOG.isTraceEnabled())
                     LOG.trace("Changed new connection receive buff size to " + client_sock_ch.socket().getReceiveBufferSize() + " bytes");
               }
               catch (IllegalArgumentException ex)
               {
                  if (log.isErrorEnabled()) log.error("exception setting receive buffer size to " +
                     send_buf_size + " bytes: " , ex);
               }
               catch (SocketException e)
               {
                  if (log.isErrorEnabled()) log.error("exception setting receive buffer size to " +
                     recv_buf_size + " bytes: " , e);
               }

               conn = new Connection(client_sock_ch, null);

               // Set it to be nonblocking
               try
               {
                  client_sock_ch.configureBlocking(false);

                  int idx;
                  synchronized (m_lockNextWriteHandler)
                  {
                     idx = m_nextWriteHandler = (m_nextWriteHandler + 1) % m_writeHandlers.length;
                  }
                  conn.setupWriteHandler(m_writeHandlers[idx]);

               } catch (IOException e)
               {
                  if (LOG.isWarnEnabled())
                     LOG.warn("Attempt to configure accepted connection failed" , e);
                  // Give up this connection if we cannot set it to non-block
                  conn.destroy();
                  continue;
               }
               catch (InterruptedException e)
               {
                  if (LOG.isWarnEnabled())
                     LOG.warn("Attempt to configure accepted connection was interrupted", e);
                  // Give up this connection
                  conn.destroy();
                  break DO_WORK; // GemStoneAddition. No need to set interrupt bit.
               }

               try
               {
                  int idx;
                  synchronized (m_lockNextReadHandler)
                  {
                     idx = m_nextReadHandler = (m_nextReadHandler + 1) % m_readHandlers.length;
                  }
                  m_readHandlers[idx].add(conn);

               } catch (InterruptedException e)
               {
                  if (LOG.isWarnEnabled())
                     LOG.warn("Attempt to configure read handler for accepted connection failed" , e);
                  // What can we do? Remove it from table then. -- not in table yet since we moved hand shaking
                  conn.destroy();
                  break DO_WORK; // GemStoneAddition. No need to set interrupt bit.
               }
            }   // end of iteration
         }   // end of selected key > 0
      }   // end of thread
      if (LOG.isTraceEnabled())
         LOG.trace("acceptor thread terminated");

   }


   /**
    * Finds first available port starting at start_port and returns server socket. Sets srv_port
    */
   @Override // GemStoneAddition
   protected ServerSocket createServerSocket(int start_port, int end_port) throws Exception
   {
      this.m_acceptSelector = Selector.open();
      m_serverSocketChannel = ServerSocketChannel.open();
      m_serverSocketChannel.configureBlocking(false);
      while (true)
      {
         try
         {
            if (bind_addr == null)
               m_serverSocketChannel.socket().bind(new InetSocketAddress(start_port));
            else
               m_serverSocketChannel.socket().bind(new InetSocketAddress(bind_addr, start_port), backlog);
         }
         catch (BindException bind_ex)
         {
            if (start_port == end_port)
               throw (BindException) ((new BindException("No available port to bind to")).initCause(bind_ex));
            start_port++;
            continue;
         }
         catch (SocketException bind_ex)
         {
            if (start_port == end_port)
               throw (BindException) ((new BindException("No available port to bind to")).initCause(bind_ex));
            start_port++;
            continue;
         }
         catch (IOException io_ex)
         {
            if (LOG.isErrorEnabled()) LOG.error(ExternalStrings.ConnectionTableNIO_ATTEMPT_TO_BIND_SERVERSOCKET_FAILED_PORT_0__BIND_ADDR_1, new Object[] {Integer.valueOf(start_port), bind_addr}, io_ex);
            throw io_ex;
         }
         srv_port = start_port;
         break;
      }
      m_serverSocketChannel.register(this.m_acceptSelector, SelectionKey.OP_ACCEPT);
      return m_serverSocketChannel.socket();
   }

   // Represents shutdown
   protected/*GemStoneAddition*/ static class Shutdown {
   }

   // ReadHandler has selector to deal with read, it runs in seperated thread
   private class ReadHandler implements Runnable {
      private Selector m_readSelector = null;
      private Thread m_th = null;
      private LinkedQueue m_queueNewConns = new LinkedQueue();

      public ReadHandler()
      {
         // Open the selector and register the pipe
         try
         {
            m_readSelector = Selector.open();
         } catch (IOException e)
         {
            // Should never happen
            e.printStackTrace();
            throw new IllegalStateException(e.getMessage());
         }

         // Start thread
         m_th = new Thread(null, this, "nioReadSelectorThread");
         m_th.setDaemon(true);
         m_th.start();
      }

      protected void add(Object conn) throws InterruptedException
      {
//         if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition not necessary checked in put
         m_queueNewConns.put(conn);
         wakeup();
      }

      private void wakeup()
      {
         m_readSelector.wakeup();
      }

      public void run()
      {
         while (true)
         {  // m_s can be closed by the management thread
            int events = 0;
            try
            {
               events = m_readSelector.select();
            } catch (IOException e)
            {
               if (LOG.isWarnEnabled())
                  LOG.warn("Select operation on socket failed", e);
               continue;   // Give up this time
            } catch (ClosedSelectorException e)
            {
               if (LOG.isWarnEnabled())
                  LOG.warn("Select operation on socket failed" , e);
               return;     // Selector gets closed, thread stops
            }

            if (events > 0)
            {   // there are read-ready channels
               Set readyKeys = m_readSelector.selectedKeys();
               for (Iterator i = readyKeys.iterator(); i.hasNext();)
               {
                  SelectionKey key = (SelectionKey) i.next();
                  i.remove();
                  //assert key.isReadable();
                  // Do partial read and handle call back
                  Connection conn = (Connection) key.attachment();
                  try
                  {
                     if (conn.getSocketChannel().isOpen())
                        readOnce(conn);
                     else
                     {  // no need to close connection or cancel key
                        Address peerAddr = conn.getPeerAddress();
                        synchronized (conns)
                        {
                           conns.remove(peerAddr);
                        }
                        notifyConnectionClosed(peerAddr);
                        continue;
                     }
                  } catch (IOException e)
                  {
                     if (LOG.isInfoEnabled()) LOG.info(ExternalStrings.ConnectionTableNIO_READ_OPERATION_ON_SOCKET_FAILED, e);
                     // The connection must be bad, cancel the key, close socket, then
                     // remove it from table!
                     Address peerAddr = conn.getPeerAddress();
                     key.cancel();
                     conn.destroy();
                     synchronized (conns)
                     {
                        conns.remove(peerAddr);
                     }
                     notifyConnectionClosed(peerAddr);
                     continue;
                  }
               }
            }

            // Now we look at the connection queue to get new job
            Object o = null;
            try
            {
               o = m_queueNewConns.poll(0); // get a job
            } catch (InterruptedException e)
            {
               if (LOG.isInfoEnabled()) LOG.info(ExternalStrings.ConnectionTableNIO_THREAD__0__WAS_INTERRUPTED_WHILE_POLLING_QUEUE, 
                   new Object[] {Thread.currentThread().getName()},
                   e);
               // We must give up
               return; // GemStoneAddition.  No need to set interrupt bit.
            }
            if (null == o)
               continue;
            if (o instanceof Shutdown) {
               return;
            }
            Connection conn = (Connection) o;
            SocketChannel sc = conn.getSocketChannel();
            try
            {
               sc.register(m_readSelector, SelectionKey.OP_READ, conn);
            } catch (ClosedChannelException e)
            {
               if (LOG.isInfoEnabled()) LOG.info(ExternalStrings.ConnectionTableNIO_SOCKET_CHANNEL_WAS_CLOSED_WHILE_WE_WERE_TRYING_TO_REGISTER_IT_TO_SELECTOR, e);
               // Channel becomes bad. The connection must be bad,
               // close socket, then remove it from table!
               Address peerAddr = conn.getPeerAddress();
               conn.destroy();
               synchronized (conns)
               {
                  conns.remove(peerAddr);
               }
               notifyConnectionClosed(peerAddr);
            }

         }   // end of the for-ever loop
      }

      private void readOnce(Connection conn)
         throws IOException
      {
         //assert conn != null;
         ConnectionReadState readState = conn.getReadState();
         if (readState.getHandShakingStatus() != ConnectionReadState.HANDSHAKINGFIN)  // hand shaking not finished
            if (!readForHandShaking(conn))
            {  // not finished yet
               return;
            } else
            {
               synchronized (conns)
               {
                  if (conns.containsKey(conn.getPeerAddress()))
                  {
                     if (conn.getPeerAddress().equals(getLocalAddress()))
                     {
                        if (LOG.isWarnEnabled())
                           LOG.warn(conn.getPeerAddress() + " is myself, not put it in table twice, but still read from it");
                     } else
                     {
                        if (LOG.isWarnEnabled())
                           LOG.warn(conn.getPeerAddress() + " is already there, will terminate connection");
                        throw new IOException(conn.getPeerAddress() + " is already there, terminate");
                     }
                  } else
                     addConnection(conn.getPeerAddress(), conn);
               }
               notifyConnectionOpened(conn.getPeerAddress());
            }
         if (!readState.isHeadFinished())
         {  // a brand new message coming or header is not completed
            // Begin or continue to read header
            int size = readHeader(conn);
            if (0 == size)
            {  // header is not completed
               return;
            }
         }
         // Begin or continue to read body
         if (readBody(conn) > 0)
         { // not finish yet
            return;
         }
         Address addr = conn.getPeerAddress();
         ByteBuffer buf = readState.getReadBodyBuffer();
         // Clear status
         readState.bodyFinished();
         // Assign worker thread to execute call back
         try
         {
            m_requestProcessors.execute(new ExecuteTask(addr, buf));
         } catch (InterruptedException e)
         {
           Thread.currentThread().interrupt(); // GemStoneAddition
            // Cannot do call back, what can we do?
            // Give up handling the message then
            LOG.error(ExternalStrings.ConnectionTableNIO_THREAD__0__WAS_INTERRUPTED_WHILE_ASSIGNING_EXECUTOR_TO_PROCESS_READ_REQUEST, Thread.currentThread().getName(), e);
         }
         return;
      }

      private int read(Connection conn, ByteBuffer buf)
         throws IOException
      {
         //assert buf.remaining() > 0;
         SocketChannel sc = conn.getSocketChannel();

         int num = sc.read(buf);
         if (-1 == num) // EOS
            throw new IOException("Couldn't read from socket as peer closed the socket");

         return buf.remaining();
      }

      /**
       * Read data for hand shaking.  It doesn't try to complete. If there is nothing in
       * the channel, the method returns immediately.
       *
       * @param conn The connection
       * @return true if handshaking passes; false if it's not finished yet (not an error!).
       * @throws IOException if handshaking fails
       */
      @SuppressWarnings("fallthrough") // GemStoneAddition
      private boolean readForHandShaking(Connection conn)
         throws IOException
      {
         ConnectionReadState readState = conn.getReadState();
         int i = readState.getHandShakingStatus();
         //assert i != ConnectionReadState.HANDSHAKINGFIN;
         switch (i)
         {
            case 0:
               // Step 1
               ByteBuffer handBuf = readState.getHandShakingBufferFixed();
               if (read(conn, handBuf) != 0)   // not finished step 1 yet
                  return false;
               readState.handShakingStep1Finished();
               // Let's fall down to process step 2
            // FALL THRU (GemStoneAddition)
            case 1:
               // Step 2
               handBuf = readState.getHandShakingBufferDynamic();
               if (read(conn, handBuf) != 0)   // not finished step 2 yet
                  return false;
               readState.handShakingStep2Finished();
               // Let's fall down to process step 3
            // FALL THRU (GemStoneAddition)
            case 2:
               // There is a chance that handshaking finishes in step 2
               if (ConnectionReadState.HANDSHAKINGFIN == readState.getHandShakingStatus())
                  return true;
               // Step 3
               handBuf = readState.getHandShakingBufferFixed();
               if (read(conn, handBuf) != 0)   // not finished step 3 yet
                  return false;
               readState.handShakingStep3Finished();
               // Let's fall down to process step 4
            // FALL THRU (GemStoneAddition)
            case 3:
               // Again, there is a chance that handshaking finishes in step 3
               if (ConnectionReadState.HANDSHAKINGFIN == readState.getHandShakingStatus())
                  return true;
               // Step 4
               handBuf = readState.getHandShakingBufferDynamic();
               if (read(conn, handBuf) != 0)   // not finished step 4 yet
                  return false;
               readState.handShakingStep4Finished();    // now all done
               return true;
         }
         //assert false;
         // never here
         return true;
      }

      /**
       * Read message header from channel. It doesn't try to complete. If there is nothing in
       * the channel, the method returns immediately.
       *
       * @param conn The connection
       * @return 0 if header hasn't been read completely, otherwise the size of message body
       * @throws IOException
       */
      private int readHeader(Connection conn)
         throws IOException
      {
         ConnectionReadState readState = conn.getReadState();
         ByteBuffer headBuf = readState.getReadHeadBuffer();
         //assert headBuf.remaining() > 0;

         SocketChannel sc = conn.getSocketChannel();
         while (headBuf.remaining() > 0)
         {
            int num = sc.read(headBuf);
            if (-1 == num)
            {// EOS
               throw new IOException("Peer closed socket");
            }
            if (0 == num) // no more data
               return 0;
         }
         // OK, now we get the whole header, change the status and return message size
         return readState.headFinished();
      }

      /**
       * Read message body from channel. It doesn't try to complete. If there is nothing in
       * the channel, the method returns immediately.
       *
       * @param conn The connection
       * @return remaining bytes for the message
       * @throws IOException
       */
      private int readBody(Connection conn)
         throws IOException
      {
         ByteBuffer bodyBuf = conn.getReadState().getReadBodyBuffer();
         //assert bodyBuf != null && bodyBuf.remaining() > 0;

         SocketChannel sc = conn.getSocketChannel();
         while (bodyBuf.remaining() > 0)
         {
            int num = sc.read(bodyBuf);
            if (-1 == num) // EOS
               throw new IOException("Couldn't read from socket as peer closed the socket");
            if (0 == num) // no more data
               return bodyBuf.remaining();
         }
         // OK, we finished reading the whole message! Flip it (not necessary though)
         bodyBuf.flip();
         return 0;
      }
   }

   private class ExecuteTask implements Runnable {
      Address m_addr = null;
      ByteBuffer m_buf = null;

      public ExecuteTask(Address addr, ByteBuffer buf)
      {
         m_addr = addr;
         m_buf = buf;
      }

      public void run()
      {
         receive(m_addr, m_buf.array(), m_buf.arrayOffset(), m_buf.limit());
      }
   }

   private class ConnectionReadState  {
      private final Connection m_conn;

      // Status for handshaking
      private int m_handShakingStatus = 0;    // 0(begin), 1, 2, 3, 99(finished)
      static final int HANDSHAKINGFIN = 99;
      private final ByteBuffer m_handShakingBufFixed = ByteBuffer.allocate(4 + 2 + 2);
      private ByteBuffer m_handShakingBufDynamic = null;
      // Status 1: Cookie(4) + version(2) + IP_length(2) --> use fixed buffer
      // Status 2: IP buffer(?) (4 for IPv4 but it could be IPv6)
      //           + Port(4) + if_addition(1) --> use dynamic buffer
      // Status 3: Addition_length(4) --> use fixed buffer
      // Status 99: Addition data(?) --> use dynamic buffer

      // Status for receiving message
      private boolean m_headFinished = false;
      private ByteBuffer m_readBodyBuf = null;
      private final ByteBuffer m_readHeadBuf = ByteBuffer.allocate(Connection.HEADER_SIZE);

      public ConnectionReadState(Connection conn)
      {
         m_conn = conn;
      }

      protected/*GemStoneAddition*/ void init()
      {
         // Initialize the handshaking status
         m_handShakingBufFixed.clear();
         m_handShakingBufFixed.limit(4 + 2 + 2);
      }

      ByteBuffer getHandShakingBufferFixed()
      {
         return m_handShakingBufFixed;
      }

      ByteBuffer getHandShakingBufferDynamic()
      {
         return m_handShakingBufDynamic;
      }

      ByteBuffer getReadBodyBuffer()
      {
         return m_readBodyBuf;
      }

      ByteBuffer getReadHeadBuffer()
      {
         return m_readHeadBuf;
      }

      void bodyFinished()
      {
         m_headFinished = false;
         m_readHeadBuf.clear();
         m_readBodyBuf = null;
         m_conn.updateLastAccessed();
      }

      /**
       * Status change for finishing reading the message header (data already in buffer)
       *
       * @return message size
       */
      int headFinished()
      {
         m_headFinished = true;
         m_readHeadBuf.flip();
         int messageSize = m_readHeadBuf.getInt();
         m_readBodyBuf = ByteBuffer.allocate(messageSize);
         m_conn.updateLastAccessed();
         return messageSize;
      }

      boolean isHeadFinished()
      {
         return m_headFinished;
      }

      /**
       * Status change for finishing hand shaking step1
       *
       * @throws IOException if hand shaking fails
       */
      void handShakingStep1Finished()
         throws IOException
      {
         m_handShakingStatus = 1;

         InetAddress clientIP = m_conn.sock_ch.socket().getInetAddress();
         int clientPort = m_conn.sock_ch.socket().getPort();

         m_handShakingBufFixed.flip();
         // Cookie
         byte[] bytesCookie = new byte[cookie.length];
         m_handShakingBufFixed.get(bytesCookie);
         if (!m_conn.matchCookie(bytesCookie))
            throw new SocketException("ConnectionTable.Connection.readPeerAddress(): cookie received from "
               + clientIP + ":" + clientPort
               + " does not match own cookie; terminating connection");
         // Version
         short ver = m_handShakingBufFixed.getShort();
         if (!JGroupsVersion.compareTo(ver))
         {
            if (LOG.isWarnEnabled())
               LOG.warn(new StringBuffer("packet from ").append(clientIP).append(':').append(clientPort).
                  append(" has different version (").append(ver).append(") from ours (").
                  append(JGroupsVersion.version).append("). This may cause problems"));
         }

         // Length of peer IP address, could be 0
         short len = m_handShakingBufFixed.getShort();
         m_handShakingBufDynamic = ByteBuffer.allocate(len + 4 + 1);
      }

      void handShakingStep2Finished()
         throws IOException
      {
         m_handShakingStatus = 2;
         m_handShakingBufDynamic.flip();
         // IP address
         byte[] ip = new byte[m_handShakingBufDynamic.remaining() - 4 - 1];
         m_handShakingBufDynamic.get(ip);
         InetAddress addr = InetAddress.getByAddress(ip);
         // Port
         int port = m_handShakingBufDynamic.getInt();
         m_conn.peer_addr = new IpAddress(addr, port);
         // If there is additional data
         boolean ifAddition = !(m_handShakingBufDynamic.get() == 0x00);
         if (!ifAddition)
         {  // handshaking finishes
            m_handShakingStatus = HANDSHAKINGFIN;
            return;
         }
         m_handShakingBufFixed.clear();
         m_handShakingBufFixed.limit(4);
      }

      void handShakingStep3Finished()
      {
         m_handShakingStatus = 3;
         m_handShakingBufFixed.flip();
         // Length of additional data
         int len = m_handShakingBufFixed.getInt();
         if (0 == len)
         {   // handshaking finishes
            m_handShakingStatus = HANDSHAKINGFIN;
            return;
         }
         m_handShakingBufDynamic = ByteBuffer.allocate(len);
      }

      void handShakingStep4Finished()
      {
         m_handShakingStatus = HANDSHAKINGFIN;    // Finishes
         m_handShakingBufDynamic.flip();
         // Additional data
         byte[] addition = new byte[m_handShakingBufDynamic.remaining()];
         m_handShakingBufDynamic.get(addition);
         ((IpAddress) m_conn.peer_addr).setAdditionalData(addition);
      }

      int getHandShakingStatus()
      {
         return m_handShakingStatus;
      }

      void setHandShakingStatus(int s)
      {
         m_handShakingStatus = s;
      }
   }

   class Connection extends ConnectionTable.Connection  {
      protected/*GemStoneAddition*/ SocketChannel sock_ch = null;
      private WriteHandler m_writeHandler;
      private SelectorWriteHandler m_selectorWriteHandler;
      private final ConnectionReadState m_readState;

      private static final int HEADER_SIZE = 4;
      final ByteBuffer headerBuffer = ByteBuffer.allocate(HEADER_SIZE);

      Connection(SocketChannel s, Address peer_addr)
      {
         super(s.socket(), peer_addr);
         sock_ch = s;
         m_readState = new ConnectionReadState(this);
         m_readState.init();
      }

      protected ConnectionReadState getReadState()
      {
         return m_readState;
      }

      protected/*GemStoneAddition*/ void setupWriteHandler(WriteHandler hdlr) throws InterruptedException
      {
//        if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition not necessary checked in add
         m_writeHandler = hdlr;
         m_selectorWriteHandler = hdlr.add(sock_ch);
      }

      @Override // GemStoneAddition
      void destroy()
      {
         closeSocket();
      }

      @Override // GemStoneAddition
      void doSend(byte[] buffie, int offset, int length) throws Exception
      {
         FutureResult result = new FutureResult();
         m_writeHandler.write(sock_ch, ByteBuffer.wrap(buffie, offset, length), result, m_selectorWriteHandler);
         Exception ex = result.getException();
         if (ex != null)
         {
            if (LOG.isErrorEnabled())
               LOG.error(ExternalStrings.ConnectionTableNIO_FAILED_SENDING_MESSAGE, ex);
            if (ex.getCause() instanceof IOException)
               throw (IOException) ex.getCause();
            throw ex;
         }
         result.get();
      }


      SocketChannel getSocketChannel()
      {
         return sock_ch;
      }

      @Override // GemStoneAddition
      void closeSocket()
      {

         if (sock_ch != null)
         {
            try
            {
               if(sock_ch.isConnected()) {
                  sock_ch.close();
               }
            }
            catch (Exception eat)
            {

            }
            sock_ch = null;
         }
      }

   }


   /**
    * Handle writing to non-blocking NIO connection.
    */
   private static class WriteHandler implements Runnable {
      // Create a queue for write requests
      protected/*GemStoneAddition*/ final LinkedQueue QUEUE = new LinkedQueue();

      protected/*GemStoneAddition*/ Selector m_selector;
      private int m_pendingChannels;                 // count of the number of channels that have pending writes
      // note that this variable is only accessed by one thread.

      // allocate and reuse the header for all buffer write operations
      private ByteBuffer m_headerBuffer = ByteBuffer.allocate(Connection.HEADER_SIZE);


      /**
       * create instances of WriteHandler threads for sending data.
       *
       * @param workerThreads is the number of threads to create.
       */
      protected static WriteHandler[] create(int workerThreads)
      {
         WriteHandler[] handlers = new WriteHandler[workerThreads];
         for (int looper = 0; looper < workerThreads; looper++)
         {
            handlers[looper] = new WriteHandler();
            try
            {
               handlers[looper].m_selector = SelectorProvider.provider().openSelector();
            }
            catch (IOException e)
            {
               if (LOG.isErrorEnabled()) LOG.error(e);
            }

            Thread thread = new Thread(handlers[looper], "nioWriteHandlerThread");
            thread.setDaemon(true);
            thread.start();
         }
         return handlers;
      }

      /**
       * Add a new channel to be handled.
       *
       * @param channel
       */
      protected SelectorWriteHandler add(SocketChannel channel) throws InterruptedException
      {
         if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition
         SelectorWriteHandler hdlr = new SelectorWriteHandler(channel, m_selector, m_headerBuffer);
         return hdlr;
      }

      /**
       * Writes buffer to the specified socket connection.  This is always performed asynchronously.  If you want
       * to perform a synchrounous write, call notification.`get() which will block until the write operation is complete.
       * Best practice is to call notification.getException() which may return any exceptions that occured during the write
       * operation.
       *
       * @param channel      is where the buffer is written to.
       * @param buffer       is what we write.
       * @param notification may be specified if you want to know how many bytes were written and know if an exception
       *                     occurred.
       */
      protected/*GemStoneAddition*/ void write(SocketChannel channel, ByteBuffer buffer, FutureResult notification, SelectorWriteHandler hdlr) throws InterruptedException
      {
//         if (Thread.interrupted()) throw new InterruptedException(); // GemStoneAddition not necessary checked in put
         QUEUE.put(new WriteRequest(channel, buffer, notification, hdlr));
      }

      private void close(SelectorWriteHandler entry)
      {
         entry.cancel();
      }

      private void handleChannelError(Selector selector, SelectorWriteHandler entry, SelectionKey selKey, Throwable error)
      {
         // notify callers of the exception and drain all of the send buffers for this channel.
         do
         {
            if (error != null)
               entry.notifyError(error);
         }
         while (entry.next());
         close(entry);
      }

      // process the write operation
      private void processWrite(Selector selector)
      {
         Set keys = selector.selectedKeys();
         Object arr[] = keys.toArray();
         for (int looper = 0; looper < arr.length; looper++)
         {
            SelectionKey key = (SelectionKey) arr[looper];
            SelectorWriteHandler entry = (SelectorWriteHandler) key.attachment();
            boolean needToDecrementPendingChannels = false;
            try
            {
               if (0 == entry.write())
               {  // write the buffer and if the remaining bytes is zero,
                  // notify the caller of number of bytes written.
                  entry.notifyObject(Integer.valueOf(entry.getBytesWritten()));
                  // switch to next write buffer or clear interest bit on socket channel.
                  if (!entry.next())
                  {
                     needToDecrementPendingChannels = true;
                  }
               }

            }
            catch (IOException e)
            {
               needToDecrementPendingChannels = true;
               // connection must of closed
               handleChannelError(selector, entry, key, e);
            }
            finally
            {
               if (needToDecrementPendingChannels)
                  m_pendingChannels--;
            }
         }
         keys.clear();
      }

      public void run()
      {
         while (m_selector.isOpen())
         {
            try
            {
               WriteRequest queueEntry;
               Object o;

               // When there are no more commands in the Queue, we will hit the blocking code after this loop.
               while (null != (o = QUEUE.poll(0)))
               {
                  if (o instanceof Shutdown)    // Stop the thread
                  {
                     return;
                  }
                  queueEntry = (WriteRequest) o;

                  if (queueEntry.getHandler().add(queueEntry))
                  {
                     // If the add operation returns true, than means that a buffer is available to be written to the
                     // corresponding channel and channel's selection key has been modified to indicate interest in the
                     // 'write' operation.
                     // If the add operation threw an exception, we will not increment m_pendingChannels which
                     // seems correct as long as a new buffer wasn't added to be sent.
                     // Another way to view this is that we don't have to protect m_pendingChannels on the increment
                     // side, only need to protect on the decrement side (this logic of this run() will be incorrect
                     // if m_pendingChannels is set incorrectly).
                     m_pendingChannels++;
                  }

                  try
                  {
                     // process any connections ready to be written to.
                     if (m_selector.selectNow() > 0)
                     {
                        processWrite(m_selector);
                     }
                  }
                  catch (IOException e)
                  {  // need to understand what causes this error so we can handle it properly
                     if (LOG.isErrorEnabled()) LOG.error(ExternalStrings.ConnectionTableNIO_SELECTNOW_OPERATION_ON_WRITE_SELECTOR_FAILED_DIDNT_EXPECT_THIS_TO_OCCUR_PLEASE_REPORT_THIS, e);
                     return;             // if select fails, give up so we don't go into a busy loop.
                  }
               }

               // if there isn't any pending work to do, block on queue to get next request.
               if (m_pendingChannels == 0)
               {
                  o = QUEUE.take();
                  if (o instanceof Shutdown){    // Stop the thread
                     return;
                  }
                  queueEntry = (WriteRequest) o;
                  if (queueEntry.getHandler().add(queueEntry))
                     m_pendingChannels++;
               }
               // otherwise do a blocking wait select operation.
               else
               {
                  try
                  {
                     if ((m_selector.select()) > 0)
                     {
                        processWrite(m_selector);
                     }
                  }
                  catch (IOException e)
                  {  // need to understand what causes this error
                     if (LOG.isErrorEnabled()) LOG.error(ExternalStrings.ConnectionTableNIO_FAILURE_WHILE_WRITING_TO_SOCKET, e);
                  }
               }
            }
            catch (InterruptedException e)
            {
               if (LOG.isErrorEnabled()) LOG.error(ExternalStrings.ConnectionTableNIO_THREAD__0__WAS_INTERRUPTED, Thread.currentThread().getName(), e);
               break; // GemStoneAddition.  No need to set interrupt bit.
            }
      catch (Throwable e)     // Log throwable rather than terminating this thread.
            {                       // We are a daemon thread so we shouldn't prevent the process from terminating if
               // the controlling thread decides that should happen.
               if (LOG.isErrorEnabled()) LOG.error(ExternalStrings.ConnectionTableNIO_THREAD__0__CAUGHT_THROWABLE, Thread.currentThread().getName(), e);
            }
         }
      }
   }


   // Wrapper class for passing Write requests.  There will be an instance of this class for each socketChannel
   // mapped to a Selector.
   public static class SelectorWriteHandler  {

      private final LinkedList m_writeRequests = new LinkedList();  // Collection of writeRequests
      private boolean m_headerSent = false;
      private SocketChannel m_channel;
      private SelectionKey m_key;
      private Selector m_selector;
      private int m_bytesWritten = 0;
      private boolean m_enabled = false;
      private ByteBuffer m_headerBuffer;

      SelectorWriteHandler(SocketChannel channel, Selector selector, ByteBuffer headerBuffer)
      {
         m_channel = channel;
         m_selector = selector;
         m_headerBuffer = headerBuffer;
      }

      private void register(Selector selector, SocketChannel channel) throws ClosedChannelException
      {
         // register the channel but don't enable OP_WRITE until we have a write request.
         m_key = channel.register(selector, 0, this);
      }

      // return true if selection key is enabled when it wasn't previous to call.
      private boolean enable()
      {
         boolean rc = false;

         try
         {
            if (m_key == null)
            {     // register the socket on first access,
                  // we are the only thread using this variable, so no sync needed.
               register(m_selector, m_channel);
            }
         }
         catch (ClosedChannelException e)
         {
            return rc;
         }

         if (!m_enabled)
         {
            rc = true;
            try
            {
               m_key.interestOps(SelectionKey.OP_WRITE);
            }
            catch (CancelledKeyException e)
            {    // channel must of closed
               return false;
            }
            m_enabled = true;
         }
         return rc;
      }

      private void disable()
      {
         if (m_enabled)
         {
            try
            {
               m_key.interestOps(0);               // pass zero which means that we are not interested in being
                                                   // notified of anything for this channel.
            }
            catch (CancelledKeyException eat)      // If we finished writing and didn't get an exception, then
            {                                      // we probably don't need to throw this exception (if they try to write
                                                   // again, we will then throw an exception).
            }
            m_enabled = false;
         }
      }

      protected void cancel()
      {
         m_key.cancel();
      }

      boolean add(WriteRequest entry)
      {
         m_writeRequests.add(entry);
         return enable();
      }

      WriteRequest getCurrentRequest()
      {
         return (WriteRequest) m_writeRequests.getFirst();
      }

      SocketChannel getChannel()
      {
         return m_channel;
      }

      ByteBuffer getBuffer()
      {
         return getCurrentRequest().getBuffer();
      }

      FutureResult getCallback()
      {
         return getCurrentRequest().getCallback();
      }

      int getBytesWritten()
      {
         return m_bytesWritten;
      }

      void notifyError(Throwable error)
      {
         if (getCallback() != null)
            getCallback().setException(error);
      }

      void notifyObject(Object result)
      {
         if (getCallback() != null)
            getCallback().set(result);
      }

      /**
       * switch to next request or disable write interest bit if there are no more buffers.
       *
       * @return true if another request was found to be processed.
       */
      boolean next()
      {
         m_headerSent = false;
         m_bytesWritten = 0;

         m_writeRequests.removeFirst();            // remove current entry
         boolean rc = !m_writeRequests.isEmpty();
         if (!rc)                                  // disable select for this channel if no more entries
            disable();
         return rc;
      }

      /**
       * @return bytes remaining to write.  This function will only throw IOException, unchecked exceptions are not
       *         expected to be thrown from here.  It is very important for the caller to know if an unchecked exception can
       *         be thrown in here.  Please correct the following throws list to include any other exceptions and update
       *         caller to handle them.
       * @throws IOException
       */
      int write() throws IOException
      {
         // Send header first.  Note that while we are writing the shared header buffer,
         // no other threads can access the header buffer as we are the only thread that has access to it.
         if (!m_headerSent)
         {
            m_headerSent = true;
            m_headerBuffer.clear();
            m_headerBuffer.putInt(getBuffer().remaining());
            m_headerBuffer.flip();
            do
            {
               getChannel().write(m_headerBuffer);
            }                                      // we should be able to handle writing the header in one action but just in case, just do a busy loop
            while (m_headerBuffer.remaining() > 0);

         }

         m_bytesWritten += (getChannel().write(getBuffer()));

         return getBuffer().remaining();
      }

   }

   public static class WriteRequest  {
      private final SocketChannel m_channel;
      private final ByteBuffer m_buffer;
      private final FutureResult m_callback;
      private final SelectorWriteHandler m_hdlr;

      WriteRequest(SocketChannel channel, ByteBuffer buffer, FutureResult callback, SelectorWriteHandler hdlr)
      {
         m_channel = channel;
         m_buffer = buffer;
         m_callback = callback;
         m_hdlr = hdlr;
      }

      SelectorWriteHandler getHandler()
      {
         return m_hdlr;
      }

      SocketChannel getChannel()
      {
         return m_channel;
      }

      ByteBuffer getBuffer()
      {
         return m_buffer;
      }

      FutureResult getCallback()
      {
         return m_callback;
      }

   }

}
