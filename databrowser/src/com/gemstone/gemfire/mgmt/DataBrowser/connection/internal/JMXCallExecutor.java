/*
 * =========================================================================
 * (c) Copyright 2002-2011, GemStone Systems, Inc. All Rights Reserved.
 *  1260 NW Waterhouse Ave., Suite 200, Beaverton, OR 97006
 *  All Rights Reserved.
 * =========================================================================
 */

package com.gemstone.gemfire.mgmt.DataBrowser.connection.internal;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import com.gemstone.gemfire.mgmt.DataBrowser.connection.ConnectionFactory;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.ConnectionFailureException;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.DSConfiguration;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.GemFireConnection;
import com.gemstone.gemfire.mgmt.DataBrowser.connection.JMXOperationFailureException;
import com.gemstone.gemfire.mgmt.DataBrowser.utils.LogUtil;

/**
 * This is an utility class which can be used to invoke JMX operations with
 * timeout value.
 *
 * @author hgadre
 */
public class JMXCallExecutor {

  public static final int DEFAULT_OPERATION_TIMEOUT = 300000;

  private static DaemonThreadFactory factory = new DaemonThreadFactory();

  private long operationTimeout = DEFAULT_OPERATION_TIMEOUT; // In seconds

  /**
   * The constructor.
   *
   * @param opTimeout
   *          the timeout value of the operation.
   */
  public JMXCallExecutor(long opTimeout) {
    operationTimeout = opTimeout;
  }

  /**
   * Default constructor.
   */
  public JMXCallExecutor() {
  }


  /**
   * This method provides a timeout based facility to make a connection to the JMX Manager.
   * @param config DSConfig object.
   * @return a connected JMXBuilder instance.
   * @throws JMXOperationFailureException in case of timeouts or exceptions
   */
  public GemFireConnection makeConnection(final DSConfiguration config)
      throws ConnectionFailureException, JMXOperationFailureException {
    ExecutorService executor = Executors.newSingleThreadExecutor(factory);
    final BlockingQueue<Object> mailbox = new ArrayBlockingQueue<Object>(1);
    executor.submit(new Runnable() {
      public void run() {
        try {
          GemFireConnection connection = ConnectionFactory.createGemFireConnection(config);
          if (!mailbox.offer(connection)) {
            LogUtil.info("The user has cancelled JMX connect operation. ");
            connection.close();
          }
          else {
            LogUtil.info("The user has successfully connected to JMX Manager at " + config.getHost() + " : " + config.getPort());
          }

        } catch(ConnectionFailureException ex) {
          mailbox.offer(ex);
        } catch (Throwable t) {
          mailbox.offer(new JMXOperationFailureException(t));
        }
      }
    });

    Object result;
    try {
      LogUtil.info("Starting to poll for the results of connect to JMX manager operation.");
      result = mailbox.poll(operationTimeout, TimeUnit.MILLISECONDS);
      if (result == null) {
        if (!mailbox.offer(""))
          result = mailbox.take();
      }
    } catch (InterruptedException e) {
      throw new JMXOperationFailureException(e.getMessage(), e);
    } finally {
      executor.shutdownNow();
    }

    if (result instanceof JMXOperationFailureException) {
      throw (JMXOperationFailureException)result;
    } else if(result instanceof ConnectionFailureException) {
      throw (ConnectionFailureException)result;
    }

    if (result == null || !(result instanceof GemFireConnection))
      throw new JMXOperationFailureException(
          "Could not connect to GemFire JMX Manager at " + config.getHost() + ":"
              + config.getPort() + " in " + operationTimeout + " milliseconds.");

    return (GemFireConnection)result;
  }

  /**
   * This class provides a facility to create daemon threads.
   */
  private static class DaemonThreadFactory implements ThreadFactory {
    public Thread newThread(Runnable r) {
      Thread t = Executors.defaultThreadFactory().newThread(r);
      t.setName("JMXCallExecutorThread");
      t.setDaemon(true);
      return t;
    }
  }
}
