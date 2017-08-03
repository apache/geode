package org.apache.geode.cache30;

import org.apache.geode.cache.CacheFactory;
import org.apache.geode.cache.CacheXmlException;
import org.apache.geode.distributed.ConfigurationProperties;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.util.Properties;
import java.util.Scanner;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.fail;

@Category(UnitTest.class)
public class CacheXmlNotFoundUnitTest {

  /**
   * unit test for <a href="https://issues.apache.org/jira/browse/GEODE-3038">GEODE-3038</a>
   * Tests that an error about missing cache-xml file is indeed printed in the text log file.
   * The test {@link org.apache.geode.cache30.CacheXml66DUnitTest#testNonExistentFile()} is supposed to test the same, but is not enough.
   * It only checks for an CacheXmlException exception to be thrown. Also in that test a log is printed into STDOUT,
   * and we do see our error there, but that is not the case when we work with the real text log, specified via "log-file" param.
   */
  @Test
  public void testCacheXmlNotFoundInRealLog() throws Exception {

    String CACHE_SERVER_LOG = "cacheXmlNotFoundUnitTest.log";
    Properties props = new Properties();
    props.put(ConfigurationProperties.LOG_FILE, CACHE_SERVER_LOG);
    props.put(ConfigurationProperties.CACHE_XML_FILE, "non-existing-cache-xml");

    CacheFactory factory = new CacheFactory(props);

    String errorMessage = "";
    try {
      factory.create();
      fail("Should have thrown a CacheXmlException");
    } catch (CacheXmlException e) {
      errorMessage = e.getLocalizedMessage();
    }

    // looking for an error in the text log file
    Scanner scanner = new Scanner(new File(CACHE_SERVER_LOG));

    boolean found = false;
    while (scanner.hasNextLine() && !found) {
      found = scanner.nextLine().contains(errorMessage);
    }
    assertTrue("there should be a line about cache-xml-nof-found in a log file", found);
  }
}
