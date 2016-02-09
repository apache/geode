/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.gemstone.gemfire.internal;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.LineNumberReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.StringReader;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Properties;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.junit.After;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;

import com.gemstone.gemfire.internal.logging.LogService;
import com.gemstone.gemfire.util.test.TestUtil;
import com.gemstone.gemfire.test.dunit.ThreadUtils;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

/**
 * Test creation of server sockets and client sockets with various JSSE
 * configurations.
 */
@Category(IntegrationTest.class)
public class JSSESocketJUnitTest {
  public @Rule TestName name = new TestName();
  
  private static final org.apache.logging.log4j.Logger logger = LogService.getLogger();

  ServerSocket acceptor;
  Socket server;
  
  static ByteArrayOutputStream baos = new ByteArrayOutputStream( );
  
  private int randport = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
  
  @Before
  public void setUp( ) throws Exception {
    System.out.println( "\n\n########## setup " + name.getMethodName() + " ############\n\n" );
    server = null;
    acceptor = null;
    baos.reset();
  }
  
  @After
  public void tearDown( ) throws Exception {
    System.out.println( "\n\n########## teardown " + name.getMethodName() + " ############\n\n" );
    
    if ( server != null ) {
      server.close();
    }
    if ( acceptor != null ) {
      acceptor.close();
    }
    System.out.println( baos.toString() );
  }
  
  //----- test methods ------
  
  @Test
  public void testSSLSocket( ) throws Exception {
    final Object[] receiver = new Object[1];
    
    TestAppender.create();
    
    {
      System.setProperty( "gemfire.mcast-port", "0");
      System.setProperty( "gemfire.ssl-enabled", "true" );
      System.setProperty( "gemfire.ssl-require-authentication", "true" );
      System.setProperty( "gemfire.ssl-ciphers", "any" );
      System.setProperty( "gemfire.ssl-protocols", "TLSv1.2" );
      
      File jks = findTestJKS();
      System.setProperty( "javax.net.ssl.trustStore", jks.getCanonicalPath() );
      System.setProperty( "javax.net.ssl.trustStorePassword", "password" );
      System.setProperty( "javax.net.ssl.keyStore", jks.getCanonicalPath() );
      System.setProperty( "javax.net.ssl.keyStorePassword", "password" );
    }
    
    assertTrue(SocketCreator.getDefaultInstance().useSSL());
    
    Thread serverThread = startServer( receiver );
    
    Socket client = SocketCreator.getDefaultInstance().connectForServer( InetAddress.getByName("localhost"), randport );
    
    ObjectOutputStream oos = new ObjectOutputStream( client.getOutputStream() );
    String expected = new String( "testing " + name.getMethodName() );
    oos.writeObject( expected );
    oos.flush();
    
    ThreadUtils.join(serverThread, 30 * 1000);
    
    client.close();
    if ( expected.equals( receiver[0] ) ) {
      System.out.println( "received " + receiver[0] + " as expected." );
    } else {
      throw new Exception( "Expected \"" + expected + "\" but received \"" + receiver[0] + "\"" );
    }
    
    String logOutput = baos.toString();
    StringReader sreader = new StringReader( logOutput );
    LineNumberReader reader = new LineNumberReader( sreader );
    int peerLogCount = 0;
    String line = null;
    while( (line = reader.readLine()) != null ) {
      
      if (line.matches( ".*peer CN=.*" ) ) {
        System.out.println( "Found peer log statement." );
        peerLogCount++;
      }
    }
    if ( peerLogCount != 2 ) {
      throw new Exception( "Expected to find to peer identities logged." );
    }
  }
  
  /** not actually related to this test class, but this is as good a place
      as any for this little test of the client-side ability to tell gemfire
      to use a given socket factory.  We just test the connectForClient method
      to see if it's used */
  @Test
  public void testClientSocketFactory() {
    System.getProperties().put("gemfire.clientSocketFactory",
      TSocketFactory.class.getName());
    System.getProperties().remove( "gemfire.ssl-enabled");
    SocketCreator.getDefaultInstance(new Properties());
    factoryInvoked = false;
    try {
      try {
        Socket sock = SocketCreator.getDefaultInstance().connectForClient("localhost", 12345,
          0);
        sock.close();
        fail("socket factory was not invoked");
      } catch (IOException e) {
        assertTrue("socket factory was not invoked: " + factoryInvoked, factoryInvoked);
      }
    } finally {
      System.getProperties().remove("gemfire.clientSocketFactory");
      SocketCreator.getDefaultInstance().initializeClientSocketFactory();
    }
  }
  
  static boolean factoryInvoked;
  
  public static class TSocketFactory implements com.gemstone.gemfire.distributed.ClientSocketFactory
  {
    public TSocketFactory() {
    }
    
    public Socket createSocket(InetAddress address, int port) throws IOException {
      JSSESocketJUnitTest.factoryInvoked = true;
      throw new IOException("splort!");
    }
  }
  
  
  //------------- utilities -----
  
  private File findTestJKS() {
    return new File(TestUtil.getResourcePath(getClass(), "/ssl/trusted.keystore"));
  }
  
  private Thread startServer( final Object[] receiver ) throws Exception {
    final ServerSocket ss = SocketCreator.getDefaultInstance().createServerSocket( randport, 0, InetAddress.getByName( "localhost" ) );
    
    
    Thread t = new Thread( new Runnable() {
      public void run( ) {
        try {
        Socket s = ss.accept();
        SocketCreator.getDefaultInstance().configureServerSSLSocket( s );
        ObjectInputStream ois = new ObjectInputStream( s.getInputStream() );
        receiver[0] = ois.readObject( );
        server = s;
        acceptor = ss;
        } catch ( Exception e ) {
          e.printStackTrace();
          receiver[0] = e;
        }
      }
    }, name.getMethodName() + "-server" );
    t.start();
    return t;
  }

  public static final class TestAppender extends AbstractAppender {
    private static final String APPENDER_NAME = TestAppender.class.getName();
    private final static String SOCKET_CREATOR_CLASSNAME= SocketCreator.class.getName();
    
    private TestAppender() {
      super(APPENDER_NAME, null, PatternLayout.createDefaultLayout());
      start();
    }

    public static Appender create() {
      Appender appender = new TestAppender();
      Logger socketCreatorLogger = (Logger) LogManager.getLogger(SOCKET_CREATOR_CLASSNAME);
      LoggerConfig config = socketCreatorLogger.getContext().getConfiguration().getLoggerConfig(SOCKET_CREATOR_CLASSNAME);
      config.addAppender(appender, Level.INFO, null);
      return appender;
    }

    @SuppressWarnings("synthetic-access")
    @Override
    public void append(final LogEvent event) {
      try {
        baos.write(new String(event.getMessage().getFormattedMessage() + "\n").getBytes());
      } catch (IOException ioex) {
        logger.warn(ioex);
      }
    }
  }
}
