package org.apache.geode.pdx;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.apache.geode.cache.Cache;
import org.apache.geode.cache.CacheFactory;

/**
 * A test class to document and make clear what JSONFormatter will and won't parse as far as simple
 * examples.
 */
public class JSONFormatterFailuresTest {
  private Cache cache;

  @Before
  public void setUp() throws Exception {
    cache = new CacheFactory().create();
  }

  @After
  public void tearDown() {
    cache.close();
  }

  @Test(expected = JSONFormatterException.class)
  public void emptyListFailsToParse() {
    JSONFormatter.fromJSON("[]");
  }

  @Test(expected = JSONFormatterException.class)
  public void listOfASingleNumberFailsToParse() {
    JSONFormatter.fromJSON("[1]");
  }

  @Test(expected = JSONFormatterException.class)
  public void numberFailsToParse() {
    JSONFormatter.fromJSON("1");
  }

  @Test(expected = JSONFormatterException.class)
  public void emptyInputFailsToParse() {
    JSONFormatter.fromJSON("");
  }

  @Test(expected = JSONFormatterException.class)
  public void emptyInputInStringFailsToParse() {
    JSONFormatter.fromJSON("\"\"");
  }

  @Test(expected = JSONFormatterException.class)
  public void arbitraryInputFailsToParse() {
    JSONFormatter.fromJSON("hi");
  }

  @Test(expected = JSONFormatterException.class)
  public void simpleStringFailsToParse() {
    JSONFormatter.fromJSON("\"hi\"");
  }

  @Test(expected = JSONFormatterException.class)
  public void nullFailsToParse() {
    JSONFormatter.fromJSON("null");
  }

  @Test(expected = JSONFormatterException.class)
  public void falseFailsToParse() {
    JSONFormatter.fromJSON("false");
  }

  @Test(expected = JSONFormatterException.class)
  public void trueFailsToParse() {
    JSONFormatter.fromJSON("true");
  }

  @Test
  public void emptyObjectParses() {
    JSONFormatter.fromJSON("{}");
  }

  @Test
  public void simpleObjectParses() {
    JSONFormatter.fromJSON("{\"a\": 2}");
  }
}
