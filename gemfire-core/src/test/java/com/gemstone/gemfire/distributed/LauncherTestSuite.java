package com.gemstone.gemfire.distributed;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

@RunWith(Suite.class)
@Suite.SuiteClasses({
   AbstractLauncherJUnitTest.class,
   AbstractLauncherServiceStatusJUnitTest.class,
   
   LauncherMemberMXBeanJUnitTest.class,

   LocatorLauncherJUnitTest.class,
   LocatorLauncherLocalJUnitTest.class,
   LocatorLauncherLocalFileJUnitTest.class,
   LocatorLauncherRemoteJUnitTest.class,
   LocatorLauncherRemoteFileJUnitTest.class,

   ServerLauncherJUnitTest.class,
   ServerLauncherLocalJUnitTest.class,
   ServerLauncherLocalFileJUnitTest.class,
   ServerLauncherRemoteJUnitTest.class,
   ServerLauncherRemoteFileJUnitTest.class,
   ServerLauncherWithSpringJUnitTest.class,
})
/**
 * Suite of tests for the Launcher classes.
 * 
 * @author Kirk Lund
 */
public class LauncherTestSuite {
}
