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
package batterytest.greplogs;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Pattern;

public class ExpectedStrings {

  private ExpectedStrings() {}

  public static boolean skipLogMsgs(String type) {
    if (type.equals("junit") || type.equals("java") || type.equals("query")
        || type.equals("dunit")) {
      return true;
    } else {
      return false;
    }
  }

  public static List create(String type) {
    List expected = new ArrayList();

    expected.add(Pattern.compile("@todo"));
    expected.add(Pattern.compile("Random seed"));
    expected.add(Pattern.compile("Caused by"));
    expected.add(Pattern.compile("continuing test"));
    expected.add(Pattern.compile("continuing with test"));
    expected.add(Pattern.compile("Test failed with errors"));
    expected.add(Pattern.compile("Test reported failure"));
    expected.add(Pattern.compile("TASK REPORT"));
    expected.add(Pattern.compile("Test reported hang"));
    expected.add(Pattern.compile("Proceeding past hung test"));
    expected.add(Pattern.compile("waited too long for result"));
    expected.add(Pattern.compile("gskill"));
    expected.add(Pattern.compile("HANG --"));
    expected.add(Pattern.compile("Leaving the vms running"));
    expected.add(Pattern.compile("Non-sanctioned build detected"));
    expected.add(Pattern.compile("may result in severe civil"));
    expected.add(Pattern.compile("This concludes your test run"));
    expected.add(Pattern.compile("TEST:"));
    expected.add(Pattern.compile("\\(will reattempt\\)"));
    expected.add(Pattern.compile("Removing disk files"));
    expected.add(Pattern.compile("cannot find a successor due to shutdown:"));
    expected.add(Pattern.compile("aborted due to shutdown:"));
    expected.add(Pattern.compile("due to cache closure:"));
    expected.add(Pattern.compile("Got expected "));
    expected.add(Pattern.compile("Caught expected "));
    expected.add(Pattern.compile("caught expected "));
    expected.add(Pattern.compile("Found expected warning"));
    expected.add(Pattern.compile("CacheClosedException: The cache is closed."));
    expected.add(Pattern.compile("Invoked MembershipNotifierHook"));
    expected.add(Pattern.compile("java.io.IOException: Connection reset by peer"));
    expected.add(Pattern.compile("client connections exceeds the licensed limit"));
    // Exclude this since the only tests with securty enabled, expect to see
    // these and if they don't then the test fails
    expected.add(Pattern.compile("NotAuthorizedException"));
    expected.add(Pattern.compile("above critical heap threshold"));
    expected.add(Pattern.compile("below critical heap threshold"));
    expected.add(Pattern.compile("checkForForcedDisconnect processed Exception"));
    expected.add(Pattern.compile("operation generated expected \\S+Exception"));

    expected.add(Pattern.compile("ExpectedString"));
    expected.add(Pattern.compile("ExpectedStrings"));

    expected.add(Pattern.compile("PassWithExpectedSevereJUnitTest"));
    expected.add(Pattern.compile("FailWithErrorInOutputJUnitTest"));
    expected.add(Pattern.compile("PassWithExpectedErrorJUnitTest"));
    expected.add(Pattern.compile("FailWithSevereInOutputJUnitTest"));
    expected.add(Pattern.compile("SystemAlertManager: A simple Alert."));

    expected.add(Pattern.compile("org.apache.geode.management.DependenciesNotFoundException"));

    // expected.add(Pattern.compile("Java version older than"));
    // expected.add(Pattern.compile("Minimum system requirements not met. Unexpected behavior may
    // result in additional errors."));

    if (type.equals("junit") || type.equals("java") || type.equals("query")) {
      expected.add(Pattern.compile("TEST EXCEPTION"));
      expected.add(Pattern.compile("testLogLevels"));
      expected.add(Pattern.compile("On iteration"));
      expected.add(Pattern.compile("signal count"));
      // Remove when davidw fixes
      expected.add(Pattern.compile("Expected"));
      // below here for gfx unit tests
      expected.add(Pattern.compile("Valid documents must have a"));
      expected.add(Pattern.compile("Loaded java.lang.ClassCastException"));
      expected.add(Pattern.compile("Loaded java.io.InvalidClassException"));
      expected.add(Pattern.compile("Loaded java.lang.NullPointerException"));
      expected.add(Pattern.compile("Loaded java.lang.ArrayIndexOutOfBoundsException"));
      expected.add(Pattern.compile("Loaded java.lang.IndexOutOfBoundsException"));
      expected.add(Pattern.compile("SucessfulTest:"));
      expected.add(Pattern.compile("SQLException: Database 'newDB' not found"));
      expected.add(Pattern.compile("SQLException: Database 'newDB1' not found"));
      expected.add(Pattern.compile("IGNORE_EXCEPTION_test"));
      expected.add(Pattern.compile("Unsupported at this time"));
      expected.add(Pattern.compile("DiskAccessException occurred as expected"));
      expected.add(Pattern.compile("Oplog::createOplog:Exception in preblowing the file"));
    } else if (type.equals("dunit")) {
      expected.add(Pattern.compile("INCOMPATIBLE_ROOT"));
      expected.add(Pattern.compile("connecting to locator"));
      expected.add(Pattern.compile("ItsOkayForMyClassNotToBeFound"));
      expected.add(Pattern.compile("Test Exception"));
      expected.add(Pattern.compile("make sure exceptions from close callbacks"));
      expected.add(Pattern.compile("Please ignore"));
      expected.add(Pattern.compile("I have been thrown from TestFunction"));
      expected.add(Pattern.compile("No admin on"));
      expected.add(Pattern.compile("nonExistentMethod"));
      expected.add(Pattern.compile("Expected exception"));
      expected.add(Pattern.compile("ConnectionPoolTestNonSerializable"));
      expected.add(Pattern.compile("One or more DUnit tests failed"));
      expected.add(Pattern.compile("ReplyException"));
      expected.add(Pattern.compile("fine 2"));
      expected.add(Pattern.compile("TESTING A VERY UNIQUE"));
      expected.add(Pattern.compile("-01-01"));
      expected.add(Pattern.compile("testNBRegionDestructionDuringGetInitialImage"));
      expected.add(Pattern.compile("SQLException: Database 'newDB' not found"));
      expected.add(Pattern.compile("SQLException: Failed to start database 'newDB'"));
      expected.add(Pattern.compile("SQLException: Database 'newDB1' not found"));
      expected.add(Pattern.compile("INCORRECT_localhost"));
      expected.add(Pattern.compile(
          "WARNING: Failed to check connection: java.net.ConnectException: Connection refused"));
      expected.add(
          Pattern.compile("WARNING: Failed to call the method close..:java.rmi.ConnectException:"));
      expected.add(Pattern.compile(
          "WARNING: Failed to restart: java.rmi.NoSuchObjectException: no such object in table"));
      expected.add(Pattern.compile(
          "WARNING: Failed to restart: java.rmi.ConnectException: Connection refused to host: .* nested exception is:"));
      expected.add(Pattern
          .compile("UnitTests terminating abnormally after a client had a fatal task error"));
      expected.add(Pattern.compile("Doing stack dump on all"));
      expected.add(Pattern.compile("Unit test result: FAILED ==> Unsuccessfully ran JUnit tests"));
      expected.add(Pattern.compile("IGNORE_EXCEPTION_test"));
      expected.add(Pattern.compile("SIGQUIT received, dumping threads"));
      expected.add(Pattern.compile("Sleeping \\d+ seconds between stack dumps"));
      expected.add(Pattern.compile("Redundancy has dropped below"));
      expected.add(Pattern.compile("Could not find any server to host redundant client"));
      expected.add(Pattern.compile("Could not find any server to host primary client"));
      expected.add(Pattern.compile("Could not find any server to create redundant client"));
      expected.add(Pattern.compile("Could not find any server to create primary client"));
      expected.add(Pattern.compile("Pool unexpected closed socket on server"));
      expected.add(Pattern.compile("SocketTimeoutException"));
      expected.add(Pattern.compile("Could not initialize a primary queue on startup"));
      expected.add(Pattern.compile(
          "java.lang.IllegalArgumentException: Sample timestamp must be greater than previous timestamp"));
      // The following 2 strings are ignored due to bug 52042
      expected.add(Pattern.compile("failed accepting client connection"));
      expected.add(Pattern.compile("Acceptor received unknown communication"));
    } else if (type.equals("smoke")) {
      expected.add(Pattern.compile("Doing stack dump on all"));
      expected.add(Pattern.compile("SIGQUIT received, dumping threads"));
      expected.add(Pattern.compile("Sleeping \\d+ seconds between stack dumps"));
      expected.add(Pattern.compile("Could not find Spring Shell library"));
    } else if (type.equals("perf")) {
      expected.add(Pattern.compile("Doing stack dump on all"));
      expected.add(Pattern.compile("SIGQUIT received, dumping threads"));
      expected.add(Pattern.compile("Sleeping \\d+ seconds between stack dumps"));
    } else if (type.equals("moresmoke")) {
      expected.add(Pattern.compile(" expected error"));
      expected.add(Pattern.compile("Doing stack dump on all"));
      expected.add(Pattern.compile("SIGQUIT received, dumping threads"));
      expected.add(Pattern.compile("Sleeping \\d+ seconds between stack dumps"));
    } else {
      expected.add(Pattern.compile("runbattery\\(\\) returned false"));
      expected.add(Pattern.compile(" expected error"));
      expected.add(Pattern.compile("Doing stack dump on all"));
      expected.add(Pattern.compile("SIGQUIT received, dumping threads"));
      expected.add(Pattern.compile("Sleeping \\d+ seconds between stack dumps"));
      expected.add(Pattern.compile("HydraTask_initializeExpectException"));
      expected.add(Pattern.compile("java.net.ConnectException: Connection refused"));
    }
    return expected;
  }
}
