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
package org.apache.geode.examples.replicated;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReplicatedTest {

  //TODO: parameterize
  private String startScript = "/Users/wmarkito/Pivotal/ASF/incubator-geode/geode-examples/replicated/scripts/startAll.sh";
  private String stopScript = "/Users/wmarkito/Pivotal/ASF/incubator-geode/geode-examples/replicated/scripts/stopAll.sh";
  private boolean processRunning = false;
  private ShellUtil shell;
  private Process process;

  private int waitTimeForScript=1000;

  @Before
  public void setup() {
    if (processRunning) {
      stop();
    }
  }

  @Test
  public void checkIfScriptsExists() throws IOException {
//
//    ClassLoader classLoader = getClass().getClassLoader();
//
//    File file = new File(classLoader.getResource("startAll.sh").getFile());
//
//    System.out.println(file.getCanonicalPath());

    assertTrue(Paths.get(startScript).toFile().exists());
    assertTrue(Paths.get(stopScript).toFile().exists());
  }


  private Process start() {
    Process p = new ShellUtil().exec(startScript).get();
    processRunning = true;
    return p;
  }

  private Process stop() {
    Process p = new ShellUtil().exec(stopScript).get();
    processRunning = false;
    return p;
  }


  @Test
  public void testStartAndStop() throws InterruptedException {
    boolean status = false;
    int exitCode = -1;

    process = start();
    status =  process.waitFor(waitTimeForScript, TimeUnit.SECONDS);
    exitCode = process.exitValue();
    verify(status, exitCode);

    process = stop();
    status = process.waitFor(waitTimeForScript, TimeUnit.SECONDS);
    exitCode = process.exitValue();
    verify(status, exitCode);
  }

  private void verify(boolean status, int exitCode) {
    assertEquals(exitCode, 0);
    assertEquals(status, true);
  }

  @After
  public void tearDown() {
    if (processRunning) {
      stop();
    }
  }


//  @Test
//  public void testStopScript() {
//    assertEquals(0, new ShellUtil().exec(stopScript).get().exitValue());
//  }


//  @Test
//  public void test() {
//    Producer producer = new Producer();
//    Consumer consumer = new Consumer();
//
//    producer.populateRegion();
//    int numEntries = consumer.countEntries();
//
//    assertEquals(BaseClient.NUM_ENTRIES, numEntries);
//  }
//
//  private Cache setupCacheServer() {
//    Cache cache = new CacheFactory()
//            .set("name", "geode-server")
//            .set("mcast-port", "0")
//            .set("log-file", "ReplicatedTest.log")
//            .set("log-level", "config")
//            .create();
//
//    RegionFactory<String, String> regionFactory = cache.createRegionFactory();
//
//    regionFactory.setDataPolicy(DataPolicy.REPLICATE);
//
//    Region myRegion = regionFactory.create(BaseClient.REGION_NAME);
//
//    assertNotNull("The /myExample Region was not properly configured and initialized!", myRegion);
////    assertEquals(BaseClient.REGION_NAME, myRegion.getFullPath());
//    assertEquals(BaseClient.REGION_NAME, myRegion.getName());
//    assertTrue(myRegion.isEmpty());
//
//    CacheServer cacheServer = cache.addCacheServer();
//
//    cacheServer.setPort(0);
//    cacheServer.setMaxConnections(5);
//
//    try {
//      cacheServer.start();
//    } catch (IOException e) {
//      throw new RuntimeException(e);
//    }
//    assertTrue("Cache Server is not running!", cacheServer.isRunning());
//
//    return cache;
//  }

}
