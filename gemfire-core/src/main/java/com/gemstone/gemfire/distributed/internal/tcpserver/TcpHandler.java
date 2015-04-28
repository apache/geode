package com.gemstone.gemfire.distributed.internal.tcpserver;

import java.io.IOException;

import com.gemstone.gemfire.cache.GemFireCache;
import com.gemstone.gemfire.distributed.DistributedSystem;
import com.gemstone.gemfire.distributed.internal.SharedConfiguration;

/**
 * A handler which responds to messages for the {@link TcpServer}
 * @author dsmith
 * @since 5.7
 */
public interface TcpHandler {
  /**
   * Process a request and return a response
   * @param request
   * @return the response, or null if there is no reponse
   * @throws IOException
   */
  Object processRequest(Object request) throws IOException;
  
  void endRequest(Object request,long startTime);
  
  void endResponse(Object request,long startTime);
  
  /**
   * Perform any shutdown code in the handler after the TCP server
   * has closed the socket.
   */
  void shutDown();
  
  /**
   * Informs the handler that TcpServer is restarting with the given
   * distributed system and cache
   * @param sharedConfig TODO
   */
  void restarting(DistributedSystem ds, GemFireCache cache, SharedConfiguration sharedConfig);
  
  /**
   * Initialize the handler with the TcpServer. Called before the TcpServer
   * starts accepting connections.
   */
  void init(TcpServer tcpServer);
}
