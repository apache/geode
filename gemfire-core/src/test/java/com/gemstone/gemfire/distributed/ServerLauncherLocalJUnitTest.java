package com.gemstone.gemfire.distributed;

import static org.junit.Assert.*;
import static org.hamcrest.CoreMatchers.*;

import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.net.BindException;
import java.net.InetAddress;

import com.gemstone.gemfire.cache.Cache;
import com.gemstone.gemfire.cache.DataPolicy;
import com.gemstone.gemfire.cache.Scope;
import com.gemstone.gemfire.distributed.AbstractLauncher.Status;
import com.gemstone.gemfire.distributed.ServerLauncher.Builder;
import com.gemstone.gemfire.distributed.ServerLauncher.ServerState;
import com.gemstone.gemfire.distributed.internal.DistributionConfig;
import com.gemstone.gemfire.internal.AvailablePort;
import com.gemstone.gemfire.internal.AvailablePortHelper;
import com.gemstone.gemfire.internal.GemFireVersion;
import com.gemstone.gemfire.internal.SocketCreator;
import com.gemstone.gemfire.internal.cache.AbstractCacheServer;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheCreation;
import com.gemstone.gemfire.internal.cache.xmlcache.CacheXmlGenerator;
import com.gemstone.gemfire.internal.cache.xmlcache.RegionAttributesCreation;
import com.gemstone.gemfire.internal.process.ProcessControllerFactory;
import com.gemstone.gemfire.internal.process.ProcessType;
import com.gemstone.gemfire.internal.process.ProcessUtils;
import com.gemstone.gemfire.test.junit.categories.IntegrationTest;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

/**
 * Integration tests for ServerLauncher as a local API in the local JVM.
 *
 * @author Kirk Lund
 * @author David Hoots
 * @author John Blum
 * @see com.gemstone.gemfire.distributed.AbstractLauncher
 * @see com.gemstone.gemfire.distributed.ServerLauncher
 * @see com.gemstone.gemfire.distributed.ServerLauncher.Builder
 * @see com.gemstone.gemfire.distributed.ServerLauncher.ServerState
 * @see com.gemstone.gemfire.internal.AvailablePortHelper
 * @since 8.0
 */
@Category(IntegrationTest.class)
public class ServerLauncherLocalJUnitTest extends AbstractServerLauncherJUnitTestCase {
  
  @Before
  public final void setUpServerLauncherLocalTest() throws Exception {
    disconnectFromDS();
    System.setProperty(ProcessType.TEST_PREFIX_PROPERTY, getUniqueName()+"-");
  }

  @After
  public final void tearDownServerLauncherLocalTest() throws Exception {    
    disconnectFromDS();
  }
  
  protected Status getExpectedStopStatusForNotRunning() {
    return Status.NOT_RESPONDING;
  }

  @Test
  public void testBuilderSetProperties() throws Throwable {
    this.launcher = new Builder()
        .setDisableDefaultServer(true)
        .setForce(true)
        .setMemberName(getUniqueName())
        .set(DistributionConfig.DISABLE_AUTO_RECONNECT_NAME, "true")
        .set(DistributionConfig.LOG_LEVEL_NAME, "config")
        .set(DistributionConfig.MCAST_PORT_NAME, "0")
        .build();

    assertNotNull(this.launcher);
    
    try {
      assertEquals(Status.ONLINE, this.launcher.start().getStatus());
      waitForServerToStart(this.launcher);
  
      final Cache cache = this.launcher.getCache();
  
      assertNotNull(cache);
  
      final DistributedSystem distributedSystem = cache.getDistributedSystem();
  
      assertNotNull(distributedSystem);
      assertEquals("true", distributedSystem.getProperties().getProperty(DistributionConfig.DISABLE_AUTO_RECONNECT_NAME));
      assertEquals("config", distributedSystem.getProperties().getProperty(DistributionConfig.LOG_LEVEL_NAME));
      assertEquals("0", distributedSystem.getProperties().getProperty(DistributionConfig.MCAST_PORT_NAME));
      assertEquals(getUniqueName(), distributedSystem.getProperties().getProperty(DistributionConfig.NAME_NAME));

    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      assertNull(this.launcher.getCache());
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }

  @Test
  public void testIsAttachAPIFound() throws Exception {
    final ProcessControllerFactory factory = new ProcessControllerFactory();
    assertTrue(factory.isAttachAPIFound());
  }
  
  @Test
  public void testStartCreatesPidFile() throws Throwable {
    // build and start the Server locally
    final Builder builder = new Builder()
        .setDisableDefaultServer(true)
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config")
        .set(DistributionConfig.MCAST_PORT_NAME, "0");

    this.launcher = builder.build();
    assertNotNull(this.launcher);

    try {
      this.launcher.start();
      waitForServerToStart(this.launcher);
      assertEquals(Status.ONLINE, this.launcher.status().getStatus());

      // validate the pid file and its contents
      this.pidFile = new File(builder.getWorkingDirectory(), ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));
      assertEquals(getPid(), pid);

      assertEquals(Status.ONLINE, this.launcher.status().getStatus());
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
      
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForFileToDelete(this.pidFile);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }

  @Test
  public void testStartDeletesStaleControlFiles() throws Throwable {
    // create existing control files
    this.stopRequestFile = new File(ProcessType.SERVER.getStopRequestFileName());
    this.stopRequestFile.createNewFile();
    assertTrue(this.stopRequestFile.exists());

    this.statusRequestFile = new File(ProcessType.SERVER.getStatusRequestFileName());
    this.statusRequestFile.createNewFile();
    assertTrue(this.statusRequestFile.exists());

    this.statusFile = new File(ProcessType.SERVER.getStatusFileName());
    this.statusFile.createNewFile();
    assertTrue(this.statusFile.exists());
    
    // build and start the server
    final Builder builder = new Builder()
        .setDisableDefaultServer(true)
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config")
        .set(DistributionConfig.MCAST_PORT_NAME, "0");

    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());
    this.launcher.start();
    
    try {
      waitForServerToStart(this.launcher);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
    
    try {
      // validate the pid file and its contents
      this.pidFile = new File(ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));
      assertEquals(getPid(), pid);
      
      // validate stale control files were deleted
      assertFalse(this.stopRequestFile.exists());
      assertFalse(this.statusRequestFile.exists());
      assertFalse(this.statusFile.exists());
      
      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(logFileName).exists());
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForFileToDelete(this.pidFile);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }
  
  @Test
  public void testStartOverwritesStalePidFile() throws Throwable {
    // create existing pid file
    this.pidFile = new File(ProcessType.SERVER.getPidFileName());
    assertFalse("Integer.MAX_VALUE shouldn't be the same as local pid " + Integer.MAX_VALUE, Integer.MAX_VALUE == ProcessUtils.identifyPid());
    writePid(this.pidFile, Integer.MAX_VALUE);

    // build and start the server
    final Builder builder = new Builder()
        .setDisableDefaultServer(true)
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config")
        .set(DistributionConfig.MCAST_PORT_NAME, "0");

    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());
    this.launcher.start();
    
    try {
      waitForServerToStart(this.launcher);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
    
    try {
      // validate the pid file and its contents
      assertTrue(this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));
      assertEquals(getPid(), pid);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForFileToDelete(this.pidFile);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }

  /**
   * Confirms fix for #47778.
   */
  @Test
  public void testStartUsingDisableDefaultServerLeavesPortFree() throws Throwable {
    // build and start the server
    assertTrue(AvailablePort.isPortAvailable(this.serverPort, AvailablePort.SOCKET));
    
    // build and start the server
    final Builder builder = new Builder()
        .setDisableDefaultServer(true)
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config")
        .set(DistributionConfig.MCAST_PORT_NAME, "0");
    
    this.launcher = builder.build();

    // wait for server to start
    try {
      // if start succeeds without throwing exception then #47778 is fixed
      this.launcher.start();
      waitForServerToStart(this.launcher);

      // validate the pid file and its contents
      this.pidFile = new File(ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));
      assertEquals(getPid(), pid);

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(logFileName).exists());

      // verify server did not a port
      assertTrue(AvailablePort.isPortAvailable(this.serverPort, AvailablePort.SOCKET));
      
      final ServerState status = this.launcher.status();
      final String portString = status.getPort();
      assertEquals("Port should be \"\" instead of " + portString, "", portString);
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    // stop the server
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForFileToDelete(this.pidFile);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }

  /**
   * Confirms fix for #47778.
   */
  @Test
  public void testStartUsingDisableDefaultServerSkipsPortCheck() throws Throwable {
    // generate one free port and then use TEST_OVERRIDE_DEFAULT_PORT_PROPERTY
    this.socket = SocketCreator.getDefaultInstance().createServerSocket(this.serverPort, 50, null, -1);
    assertFalse(AvailablePort.isPortAvailable(this.serverPort, AvailablePort.SOCKET));
    
    // build and start the server
    final Builder builder = new Builder()
        .setDisableDefaultServer(true)
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config")
        .set(DistributionConfig.MCAST_PORT_NAME, "0");

    this.launcher = builder.build();

    // wait for server to start
    try {
      // if start succeeds without throwing exception then #47778 is fixed
      this.launcher.start();
      waitForServerToStart(this.launcher);

      // validate the pid file and its contents
      this.pidFile = new File(ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));
      assertEquals(getPid(), pid);

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(logFileName).exists());
      
      final ServerState status = this.launcher.status();
      final String portString = status.getPort();
      assertEquals("Port should be \"\" instead of " + portString, "", portString);
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
    
    // stop the server
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForFileToDelete(this.pidFile);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  
    // verify port is still in use
    this.errorCollector.checkThat(AvailablePort.isPortAvailable(this.serverPort, AvailablePort.SOCKET), is(equalTo(false)));
  }

  @Test
  @Ignore("Need to rewrite this without using dunit.Host")
  public void testStartUsingForceOverwritesExistingPidFile() throws Throwable {
  }/*
    assertTrue(getUniqueName() + " is broken if PID == Integer.MAX_VALUE", ProcessUtils.identifyPid() != Integer.MAX_VALUE);
    
    // create existing pid file
    this.pidFile = new File(ProcessType.SERVER.getPidFileName());
    final int realPid = Host.getHost(0).getVM(3).invokeInt(ProcessUtils.class, "identifyPid");
    assertFalse(realPid == ProcessUtils.identifyPid());
    writePid(this.pidFile, realPid);

    // build and start the server
    final Builder builder = new Builder()
        .setDisableDefaultServer(true)
        .setForce(true)
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config")
        .set(DistributionConfig.MCAST_PORT_NAME, "0");

    assertTrue(builder.getForce());
    this.launcher = builder.build();
    assertTrue(this.launcher.isForcing());
    this.launcher.start();

    // collect and throw the FIRST failure
    Throwable failure = null;

    try {
      waitForServerToStart(this.launcher);

      // validate the pid file and its contents
      assertTrue(this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));
      assertEquals(getPid(), pid);
      
      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(logFileName).exists());
      
    } catch (Throwable e) {
      logger.error(e);
      if (failure == null) {
        failure = e;
      }
    }

    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForFileToDelete(this.pidFile);
    } catch (Throwable e) {
      logger.error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    if (failure != null) {
      throw failure;
    }
  } // testStartUsingForceOverwritesExistingPidFile
  */

  /**
   * Confirms part of fix for #47664
   */
  @Test
  public void testStartUsingServerPortOverridesCacheXml() throws Throwable {
    // verifies part of the fix for #47664
    
    // generate two free ports
    final int[] freeTCPPorts = AvailablePortHelper.getRandomAvailableTCPPorts(2);
    assertTrue(AvailablePort.isPortAvailable(freeTCPPorts[0], AvailablePort.SOCKET));
    assertTrue(AvailablePort.isPortAvailable(freeTCPPorts[1], AvailablePort.SOCKET));
    
    // write out cache.xml with one port
    final CacheCreation creation = new CacheCreation();
    final RegionAttributesCreation attrs = new RegionAttributesCreation(creation);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setDataPolicy(DataPolicy.REPLICATE);
    creation.createRegion(getUniqueName(), attrs);
    creation.addCacheServer().setPort(freeTCPPorts[0]);
    
    File cacheXmlFile = this.temporaryFolder.newFile(getUniqueName() + ".xml");
    final PrintWriter pw = new PrintWriter(new FileWriter(cacheXmlFile), true);
    CacheXmlGenerator.generate(creation, pw);
    pw.close();
    
    System.setProperty(DistributionConfig.CACHE_XML_FILE_NAME, cacheXmlFile.getCanonicalPath());
    
    // start server
    final Builder builder = new Builder()
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .setServerPort(freeTCPPorts[1])
        .set(DistributionConfig.LOG_LEVEL_NAME, "config")
        .set(DistributionConfig.MCAST_PORT_NAME, "0");

    this.launcher = builder.build();
    this.launcher.start();
  
    // wait for server to start up
    try {
      waitForServerToStart(this.launcher);
  
      // validate the pid file and its contents
      this.pidFile = new File(ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));
      assertEquals(getPid(), pid);

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(logFileName).exists());

      // verify server used --server-port instead of default or port in cache.xml
      assertTrue(AvailablePort.isPortAvailable(freeTCPPorts[0], AvailablePort.SOCKET));
      assertFalse(AvailablePort.isPortAvailable(freeTCPPorts[1], AvailablePort.SOCKET));
      
      final ServerState status = this.launcher.status();
      final String portString = status.getPort();
      final int port = Integer.valueOf(portString);
      assertEquals("Port should be " + freeTCPPorts[1] + " instead of " + port, freeTCPPorts[1], port);
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
      
    // stop the server
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForFileToDelete(this.pidFile);
      assertFalse("PID file still exists!", pidFile.exists());
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }
  
  /**
   * Confirms part of fix for #47664
   */
  @Test
  public void testStartUsingServerPortUsedInsteadOfDefaultCacheXml() throws Throwable {
    // write out cache.xml with one port
    final CacheCreation creation = new CacheCreation();
    final RegionAttributesCreation attrs = new RegionAttributesCreation(creation);
    attrs.setScope(Scope.DISTRIBUTED_ACK);
    attrs.setDataPolicy(DataPolicy.REPLICATE);
    creation.createRegion(getUniqueName(), attrs);
    creation.addCacheServer();
    
    File cacheXmlFile = this.temporaryFolder.newFile(getUniqueName() + ".xml");
    final PrintWriter pw = new PrintWriter(new FileWriter(cacheXmlFile), true);
    CacheXmlGenerator.generate(creation, pw);
    pw.close();
    
    System.setProperty(DistributionConfig.CACHE_XML_FILE_NAME, cacheXmlFile.getCanonicalPath());
      
    // start server
    final Builder builder = new Builder()
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .setServerPort(this.serverPort)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config")
        .set(DistributionConfig.MCAST_PORT_NAME, "0");

    this.launcher = builder.build();
    this.launcher.start();
  
    // wait for server to start up
    try {
      waitForServerToStart(this.launcher);
  
      // validate the pid file and its contents
      this.pidFile = new File(ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertTrue(ProcessUtils.isProcessAlive(pid));
      assertEquals(getPid(), pid);

      // validate log file was created
      final String logFileName = getUniqueName()+".log";
      assertTrue("Log file should exist: " + logFileName, new File(logFileName).exists());

      // verify server used --server-port instead of default
      assertFalse(AvailablePort.isPortAvailable(this.serverPort, AvailablePort.SOCKET));
      
      final int port = Integer.valueOf( this.launcher.status().getPort());
      assertEquals("Port should be " + this.serverPort + " instead of " + port, this.serverPort, port);
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
      
    // stop the server
    try {
      assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
      waitForFileToDelete(this.pidFile);
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }

  @Test
  public void testStartWithDefaultPortInUseFails() throws Throwable {
    // generate one free port and then use TEST_OVERRIDE_DEFAULT_PORT_PROPERTY
    this.socket = SocketCreator.getDefaultInstance().createServerSocket(this.serverPort, 50, null, -1);
    assertFalse(AvailablePort.isPortAvailable(this.serverPort, AvailablePort.SOCKET));
    
    // build and start the server
    final Builder builder = new Builder()
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config")
        .set(DistributionConfig.MCAST_PORT_NAME, "0");

    this.launcher = builder.build();
    
    RuntimeException expected = null;
    try {
      this.launcher.start();
     
      // why did it not fail like it's supposed to?
      final String property = System.getProperty(AbstractCacheServer.TEST_OVERRIDE_DEFAULT_PORT_PROPERTY);
      assertNotNull(property);
      assertEquals(this.serverPort, Integer.valueOf(property).intValue());
      assertFalse(AvailablePort.isPortAvailable(this.serverPort, AvailablePort.SOCKET));
      
      fail("Server port is " + this.launcher.getCache().getCacheServers().get(0).getPort());
      fail("ServerLauncher start should have thrown RuntimeException caused by BindException");
    } catch (RuntimeException e) {
      expected = e;
      assertNotNull(expected.getMessage());
      // BindException text varies by platform
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
    
    try {
      assertNotNull(expected);
      final Throwable cause = expected.getCause();
      assertNotNull(cause);
      assertTrue(cause instanceof BindException);
      // BindException string varies by platform
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    try {
      this.pidFile = new File (ProcessType.SERVER.getPidFileName());
      assertFalse("Pid file should not exist: " + this.pidFile, this.pidFile.exists());
      
      // creation of log file seems to be random -- look into why sometime
      final String logFileName = getUniqueName()+".log";
      assertFalse("Log file should not exist: " + logFileName, new File(logFileName).exists());
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
    
    // just in case the launcher started...
    ServerState status = null;
    try {
      status = this.launcher.stop();
    } catch (Throwable t) { 
      // ignore
    }
    
    try {
      waitForFileToDelete(this.pidFile);
      assertEquals(getExpectedStopStatusForNotRunning(), status.getStatus());
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }

  @Test
  @Ignore("Need to rewrite this without using dunit.Host")
  public void testStartWithExistingPidFileFails() throws Throwable {
  }/*
    // create existing pid file
    final int realPid = Host.getHost(0).getVM(3).invokeInt(ProcessUtils.class, "identifyPid");
    assertFalse("Remote pid shouldn't be the same as local pid " + realPid, realPid == ProcessUtils.identifyPid());

    this.pidFile = new File(ProcessType.SERVER.getPidFileName());
    writePid(this.pidFile, realPid);
    
    // build and start the server
    final Builder builder = new Builder()
        .setDisableDefaultServer(true)
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config")
        .set(DistributionConfig.MCAST_PORT_NAME, "0");

    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());

    // collect and throw the FIRST failure
    Throwable failure = null;
    RuntimeException expected = null;
    
    try {
      this.launcher.start();
      fail("ServerLauncher start should have thrown RuntimeException caused by FileAlreadyExistsException");
    } catch (RuntimeException e) {
      expected = e;
      assertNotNull(expected.getMessage());
      assertTrue(expected.getMessage().contains("A PID file already exists and a Server may be running in"));
    } catch (Throwable e) {
      logger.error(e);
      if (failure == null) {
        failure = e;
      }
    }

    // just in case the launcher started...
    ServerState status = null;
    try {
      status = this.launcher.stop();
    } catch (Throwable t) { 
      // ignore
    }
    
    try {
      assertNotNull(expected);
      final Throwable cause = expected.getCause();
      assertNotNull(cause);
      assertTrue(cause instanceof FileAlreadyExistsException);
      assertTrue(cause.getMessage().contains("Pid file already exists: "));
      assertTrue(cause.getMessage().contains("vf.gf.server.pid for process " + realPid));
    } catch (Throwable e) {
      logger.error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    try {
      delete(this.pidFile);
      final Status theStatus = status.getStatus();
      assertFalse(theStatus == Status.STARTING);
      assertFalse(theStatus == Status.ONLINE);
    } catch (Throwable e) {
      logger.error(e);
      if (failure == null) {
        failure = e;
      }
    }
    
    if (failure != null) {
      throw failure;
    }
  } // testStartWithExistingPidFileFails
  */
  
  /**
   * Confirms fix for #47665.
   */
  @Test
  public void testStartUsingServerPortInUseFails() throws Throwable {
    // generate one free port and then use TEST_OVERRIDE_DEFAULT_PORT_PROPERTY
    final int freeTCPPort = AvailablePort.getRandomAvailablePort(AvailablePort.SOCKET);
    this.socket = SocketCreator.getDefaultInstance().createServerSocket(freeTCPPort, 50, null, -1);
    
    // build and start the server
    final Builder builder = new Builder()
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .setServerPort(freeTCPPort)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config")
        .set(DistributionConfig.MCAST_PORT_NAME, "0");

    this.launcher = builder.build();
    
    RuntimeException expected = null;
    try {
      this.launcher.start();
      fail("ServerLauncher start should have thrown RuntimeException caused by BindException");
    } catch (RuntimeException e) {
      expected = e;
      assertNotNull(expected.getMessage());
      // BindException string varies by platform
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
    
    try {
      assertNotNull(expected);
      final Throwable cause = expected.getCause();
      assertNotNull(cause);
      assertTrue(cause instanceof BindException);
      // BindException string varies by platform
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    try {
      this.pidFile = new File (ProcessType.SERVER.getPidFileName());
      assertFalse("Pid file should not exist: " + this.pidFile, this.pidFile.exists());
      
      // creation of log file seems to be random -- look into why sometime
      final String logFileName = getUniqueName()+".log";
      assertFalse("Log file should not exist: " + logFileName, new File(logFileName).exists());
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
    
    // just in case the launcher started...
    ServerState status = null; // TODO: this could result in NPE later
    try {
      status = this.launcher.stop();
    } catch (Throwable t) { 
      // ignore
    }
    
    try {
      waitForFileToDelete(this.pidFile);
      assertEquals(getExpectedStopStatusForNotRunning(), status.getStatus());
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }
  
  @Test
  public void testStatusUsingPid() throws Throwable {
    // build and start the server
    final Builder builder = new Builder()
        .setDisableDefaultServer(true)
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config")
        .set(DistributionConfig.MCAST_PORT_NAME, "0");
    
    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());
    
    ServerLauncher pidLauncher = null;
    try {
      this.launcher.start();
      waitForServerToStart(this.launcher);
      
      this.pidFile = new File(ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertEquals(ProcessUtils.identifyPid(), pid);
  
      pidLauncher = new Builder().setPid(pid).build();
      assertNotNull(pidLauncher);
      assertFalse(pidLauncher.isRunning());

      final ServerState actualStatus = pidLauncher.status();
      assertNotNull(actualStatus);
      assertEquals(Status.ONLINE, actualStatus.getStatus());
      assertEquals(pid, actualStatus.getPid().intValue());
      assertTrue(actualStatus.getUptime() > 0);
      assertEquals(new File(System.getProperty("user.dir")).getCanonicalPath(), actualStatus.getWorkingDirectory());
      //assertEquals(???, actualStatus.getJvmArguments());
      assertEquals(ManagementFactory.getRuntimeMXBean().getClassPath(), actualStatus.getClasspath());
      assertEquals(GemFireVersion.getGemFireVersion(), actualStatus.getGemFireVersion());
      assertEquals(System.getProperty("java.version"),  actualStatus.getJavaVersion());
      assertEquals(new File(System.getProperty("user.dir")).getCanonicalPath() + File.separator + getUniqueName() + ".log", actualStatus.getLogFile());
      assertEquals(InetAddress.getLocalHost().getCanonicalHostName(), actualStatus.getHost());
      assertEquals(getUniqueName(), actualStatus.getMemberName());
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    if (pidLauncher == null) {
      try {
        assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
        waitForFileToDelete(this.pidFile);
      } catch (Throwable e) {
        this.errorCollector.addError(e);
      }
      
    } else {
      try {
        assertEquals(Status.STOPPED, pidLauncher.stop().getStatus());
        waitForFileToDelete(this.pidFile);
      } catch (Throwable e) {
        this.errorCollector.addError(e);
      }
    }
  }
  
  @Test
  public void testStatusUsingWorkingDirectory() throws Throwable {
    // build and start the server
    final Builder builder = new Builder()
        .setDisableDefaultServer(true)
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config")
        .set(DistributionConfig.MCAST_PORT_NAME, "0");
    
    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());
    
    ServerLauncher dirLauncher = null;
    try {
      this.launcher.start();
      waitForServerToStart(this.launcher);
      
      this.pidFile = new File(ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertEquals(ProcessUtils.identifyPid(), pid);
  
      final String workingDir = new File(System.getProperty("user.dir")).getCanonicalPath();
      dirLauncher = new Builder().setWorkingDirectory(workingDir).build();
      assertNotNull(dirLauncher);
      assertFalse(dirLauncher.isRunning());

      final ServerState actualStatus = dirLauncher.status();
      assertNotNull(actualStatus);
      assertEquals(Status.ONLINE, actualStatus.getStatus());
      assertEquals(pid, actualStatus.getPid().intValue());
      assertTrue(actualStatus.getUptime() > 0);
      assertEquals(new File(System.getProperty("user.dir")).getCanonicalPath(), actualStatus.getWorkingDirectory());
      //assertEquals(???, actualStatus.getJvmArguments());
      assertEquals(ManagementFactory.getRuntimeMXBean().getClassPath(), actualStatus.getClasspath());
      assertEquals(GemFireVersion.getGemFireVersion(), actualStatus.getGemFireVersion());
      assertEquals(System.getProperty("java.version"),  actualStatus.getJavaVersion());
      assertEquals(new File(System.getProperty("user.dir")).getCanonicalPath() + File.separator + getUniqueName() + ".log", actualStatus.getLogFile());
      assertEquals(InetAddress.getLocalHost().getCanonicalHostName(), actualStatus.getHost());
      assertEquals(getUniqueName(), actualStatus.getMemberName());
      
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    if (dirLauncher == null) {
      try {
        assertEquals(Status.STOPPED, this.launcher.stop().getStatus());
        waitForFileToDelete(this.pidFile);
      } catch (Throwable e) {
        this.errorCollector.addError(e);
      }
      
    } else {
      try {
        assertEquals(Status.STOPPED, dirLauncher.stop().getStatus());
        waitForFileToDelete(this.pidFile);
      } catch (Throwable e) {
        this.errorCollector.addError(e);
      }
    }
  }
  
  @Test
  public void testStopUsingPid() throws Throwable {
    // build and start the server
    final Builder builder = new Builder()
        .setDisableDefaultServer(true)
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config")
        .set(DistributionConfig.MCAST_PORT_NAME, "0");

    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());

    ServerLauncher pidLauncher = null;
    
    try {
      this.launcher.start();
      waitForServerToStart(this.launcher);
  
      // validate the pid file and its contents
      this.pidFile = new File(ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertEquals(ProcessUtils.identifyPid(), pid);

      pidLauncher = new Builder().setPid(pid).build();
      assertNotNull(pidLauncher);
      assertFalse(pidLauncher.isRunning());
      
      // stop the server
      final ServerState serverState = pidLauncher.stop();
      assertNotNull(serverState);
      assertEquals(Status.STOPPED, serverState.getStatus());
    
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    try {
      this.launcher.stop();
    } catch (Throwable e) {
      // ignore
    }

    try {
      // verify the PID file was deleted
      waitForFileToDelete(this.pidFile); // TODO
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }
  
  @Test
  public void testStopUsingWorkingDirectory() throws Throwable {
    // build and start the server
    final Builder builder = new Builder()
        .setDisableDefaultServer(true)
        .setMemberName(getUniqueName())
        .setRedirectOutput(true)
        .set(DistributionConfig.LOG_LEVEL_NAME, "config")
        .set(DistributionConfig.MCAST_PORT_NAME, "0");

    assertFalse(builder.getForce());
    this.launcher = builder.build();
    assertFalse(this.launcher.isForcing());

    ServerLauncher dirLauncher = null;
    try {
      this.launcher.start();
      waitForServerToStart(this.launcher);
    
      // validate the pid file and its contents
      this.pidFile = new File(ProcessType.SERVER.getPidFileName());
      assertTrue(this.pidFile.exists());
      final int pid = readPid(this.pidFile);
      assertTrue(pid > 0);
      assertEquals(ProcessUtils.identifyPid(), pid);

      final String workingDir = new File(System.getProperty("user.dir")).getCanonicalPath();
      dirLauncher = new Builder().setWorkingDirectory(workingDir).build();
      assertNotNull(dirLauncher);
      assertFalse(dirLauncher.isRunning());
      
      // stop the server
      final ServerState serverState = dirLauncher.stop();
      assertNotNull(serverState);
      assertEquals(Status.STOPPED, serverState.getStatus());
    
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }

    try {
      this.launcher.stop();
    } catch (Throwable e) {
      // ignore
    }

    try {
      // verify the PID file was deleted
      waitForFileToDelete(this.pidFile); // TODO
    } catch (Throwable e) {
      this.errorCollector.addError(e);
    }
  }
}
