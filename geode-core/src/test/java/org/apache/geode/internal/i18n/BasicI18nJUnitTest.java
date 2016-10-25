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
package org.apache.geode.internal.i18n;

import static org.junit.Assert.*;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Locale;
import java.util.Set;

import org.junit.After;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.i18n.StringId;
import org.apache.geode.test.junit.categories.IntegrationTest;

/**
 * This class tests all basic i18n functionality.
 */
@Category(IntegrationTest.class)
public class BasicI18nJUnitTest {
  
  private static final Locale DEFAULT_LOCALE = Locale.getDefault();
  //static final Class DEFAULT_RESOURCEBUNDLE = StringIdResourceBundle_ja.class;
  //static final Class JAPAN_RESOURCEBUNDLE = StringIdResourceBundle_ja.class;

  private static final StringId messageId = (StringId)LocalizedStrings.TESTING_THIS_IS_A_TEST_MESSAGE;
  private static final String englishMessage = "This is a test message.";
  private static final String japaneseMessage = "msgID " + messageId.id + ": " + "これはテストメッセージである。";

  private static final Integer messageArg = new Integer(1);
  private static final StringId messageIdWithArg = (StringId)LocalizedStrings.TESTING_THIS_MESSAGE_HAS_0_MEMBERS;
  private static final String englishMessageWithArg = "Please ignore: This message has 1 members.";
  private static final String japaneseMessageWithArg = "msgID " + messageIdWithArg.id + ": Please ignore: このメッセージに 1 メンバーがある。";
  private static final String englishMessageWithArgMissing = "Please ignore: This message has {0} members.";
  private static final String japaneseMessageWithArgMissing = "msgID " + messageIdWithArg.id + ": Please ignore: このメッセージに {0} メンバーがある。";

  @After
  public void tearDown() {
    //reset to the original
    StringId.setLocale(DEFAULT_LOCALE);
  }

  @Test
  public void testSetLocale() {
    //Verify we are starting in a known state
    assertTrue(DEFAULT_LOCALE.equals(getCurrentLocale()));
    StringId.setLocale(Locale.FRANCE);
    assertTrue(Locale.FRANCE.equals(getCurrentLocale()));

    StringId.setLocale(Locale.FRANCE);
    assertTrue(Locale.FRANCE.equals(getCurrentLocale()));
    StringId s = LocalizedStrings.UNSUPPORTED_AT_THIS_TIME;
    assertTrue(s.toString().equals(s.toLocalizedString()));

    StringId.setLocale(Locale.JAPAN);
    assertTrue(Locale.JAPAN.equals(getCurrentLocale()));
    if (getActiveResourceBundle().usingRawMode()) {
      assertTrue(s.toString().equals(s.toLocalizedString()));
    } else {
      assertFalse(s.toString().equals(s.toLocalizedString()));
    }
  }

  /**
   * Tests {@link StringId#toLocalizedString()} and {@link StringId#toString()}
   * Currently three languages are tested: English, French, and Japanese.
   * French is tested under the assumption that it will never be implemented and
   * thus tests an unimplemented language.
   * Each Locale is tested for these things:
   * 1) toString() no arguments
   * 2) toLocalizedString() no arguments
   * 3) toString(new Integer(1)) one argument
   * 4) toLocalizedString(new Integer(1)) one argument
   * 5) toString() wrong number or arguments, too many
   * 6) toLocalizedString() wrong number or arguments, too many
   * 7) toString() wrong number or arguments, too few
   * 8) toLocalizedString() wrong number or arguments, too few
   */
  @Test
  public void testToLocalizedString() {
    Object[] missingArgs = new Object[] {};
    Object[] extraArgs = new Object[] { messageArg, "should not print" };

    assertEquals(messageId.toString(), englishMessage);
    assertEquals(messageId.toLocalizedString(), englishMessage);
    assertEquals(messageIdWithArg.toString(messageArg), englishMessageWithArg);
    assertEquals(messageIdWithArg.toLocalizedString(messageArg),
                 englishMessageWithArg);
    assertEquals(messageIdWithArg.toString(extraArgs), englishMessageWithArg);
    assertEquals(messageIdWithArg.toLocalizedString(extraArgs),
                 englishMessageWithArg);
    assertEquals(messageIdWithArg.toString(missingArgs),
                 englishMessageWithArgMissing);
    assertEquals(messageIdWithArg.toLocalizedString(missingArgs),
                 englishMessageWithArgMissing);

    /**
     * This is assuming that French isn't implemented and will still get the
     * English resource bundle.  The second assert will fail if this isn't the
     * case.
     **/
    StringId.setLocale(Locale.FRANCE);
    assertTrue(Locale.FRANCE.equals(getCurrentLocale()));
    assertEquals(messageId.toString(), englishMessage);
    assertEquals(messageId.toLocalizedString(), englishMessage);
    //Now with a message expecting one argument
    assertEquals(messageIdWithArg.toString(messageArg), englishMessageWithArg);
    assertEquals(messageIdWithArg.toLocalizedString(messageArg),
                 englishMessageWithArg);
    assertEquals(messageIdWithArg.toString(extraArgs), englishMessageWithArg);
    assertEquals(messageIdWithArg.toLocalizedString(extraArgs),
                 englishMessageWithArg);
    assertEquals(messageIdWithArg.toString(missingArgs),
                 englishMessageWithArgMissing);
    assertEquals(messageIdWithArg.toLocalizedString(missingArgs),
                 englishMessageWithArgMissing);

    /**
     * We no longer bundle the JAPAN localized strings with the product
     * so the following now expects english msgs.
     */
    StringId.setLocale(Locale.JAPAN);
    assertTrue(Locale.JAPAN.equals(getCurrentLocale()));
    assertEquals(messageId.toString(), englishMessage);
    if (getActiveResourceBundle().usingRawMode()) {
      assertEquals(messageId.toLocalizedString(), englishMessage);
    } else {
      assertEquals(messageId.toLocalizedString(), japaneseMessage);
    }
    //Now with a message expecting one argument
    assertEquals(messageIdWithArg.toString(messageArg), englishMessageWithArg);
    if (getActiveResourceBundle().usingRawMode()) {
      assertEquals(messageIdWithArg.toLocalizedString(messageArg),
                   englishMessageWithArg);
    } else {
      assertEquals(messageIdWithArg.toLocalizedString(messageArg),
                   japaneseMessageWithArg);
    }
    assertEquals(messageIdWithArg.toString(extraArgs), englishMessageWithArg);
    if (getActiveResourceBundle().usingRawMode()) {
      assertEquals(messageIdWithArg.toLocalizedString(extraArgs),
                   englishMessageWithArg);
    } else {
      assertEquals(messageIdWithArg.toLocalizedString(extraArgs),
                   japaneseMessageWithArg);
    }
    assertEquals(messageIdWithArg.toString(missingArgs),
                 englishMessageWithArgMissing);
    if (getActiveResourceBundle().usingRawMode()) {
      assertEquals(messageIdWithArg.toLocalizedString(missingArgs),
                   englishMessageWithArgMissing);
    } else {
      assertEquals(messageIdWithArg.toLocalizedString(missingArgs),
                   japaneseMessageWithArgMissing);
    }
  }

  @Test
  public void testEnglishLanguage() {
    StringId.setLocale(Locale.ENGLISH);
    assertEquals(messageId.toLocalizedString(), englishMessage);
  }

  @Test
  public void testJapaneseLanguage() {
    StringId.setLocale(Locale.JAPANESE);
    if (getActiveResourceBundle().usingRawMode()) {
      assertEquals(messageId.toLocalizedString(), englishMessage);
    } else {
      assertEquals(messageId.toLocalizedString(), japaneseMessage);
    }
  }

  @Test
  public void testAlternateEnglishCountries() {
    StringId.setLocale(Locale.CANADA);
    assertEquals(messageId.toLocalizedString(), englishMessage);

    StringId.setLocale(Locale.UK);
    assertEquals(messageId.toLocalizedString(), englishMessage);
  }

  /**
   * Use reflection to find all instances of StringId defined within
   * the classes listed in StringIdDefiningClasses and verify
   * there are not any duplicate indexes used.
   */
  @Test
  public void testVerifyStringIdsAreUnique() {
    final Set<Integer> allStringIds = new HashSet<Integer>(3000);

    //Save all duplicate ids and report them at the end
    final Set<StringId> duplicates = new HashSet<StringId>();

    for(StringId instance : getAllStringIds()) {
      boolean isUnique = allStringIds.add(((StringId)instance).id);
      //Duplicate ids between 0-1023 are allowed since they are duplicated
      //between String bundles to minimize compiler dependencies.
      if((! isUnique) && ((StringId)instance).id >= 1024) {
         boolean status = duplicates.add(instance);
         assertTrue("Failed to add " + instance + "to the list of"
                    + " duplicates because of duplicate duplicates",
                    status);
      }
    }
    if(! duplicates.isEmpty()) {
      StringBuilder err = new StringBuilder();
      err.append("The following duplicate StringIds were found:");
      for(StringId i : duplicates) {
        err.append("\n")
           .append(((StringId)i).id)
           .append(" : ")
           .append(i.getRawText());
      }
      fail(err.toString() + "\nSearched in "+getStringIdDefiningClasses());
    }
  }

  /**
   * Verify the "Formated" form of the string matches the raw text form.
   * This test is to catch issues like bug #40146
   * This tests the English version in the US locale.
   */
  @Test
  public void testVerifyStringsAreProperlyEscaped_US() {
    verifyStringsAreProperlyEscaped(Locale.US);
  }

  /**
   * Verify the "Formated" form of the string matches the raw text form.
   * This test is to catch issues like bug #40146
   * This tests the English version in the UK locale.
   */
  @Test
  public void testVerifyStringsAreProperlyEscaped_UK() {
    verifyStringsAreProperlyEscaped(Locale.UK);
  }

  /**
   * Verify the "Formated" form of the string matches the raw text form.
   * This test is to catch issues like bug #40146
   * This tests the English version in the JAPAN locale.
   */
  @Test
  public void testVerifyStringsAreProperlyEscaped_JAPAN() {
    verifyStringsAreProperlyEscaped(Locale.JAPAN);
  }

  /**
   * Get all of the StringId instances via reflection.
   * @return a set of all StringId declarations within the product
   */
  private Set<StringId> getAllStringIds() {
    final Set<StringId> allStringIds = new HashSet<StringId>();
    for(String className : getStringIdDefiningClasses()) {
      try {
        Class<?> c = Class.forName(className);
        Field[] fields = c.getDeclaredFields();
        final String msg = "Found no StringIds in " + className;
        assertTrue(msg, fields.length > 0);
        for(Field f : fields) {
          f.setAccessible(true);
          StringId instance = (StringId) f.get(null);
          allStringIds.add(instance);
        }
      } catch (ClassNotFoundException cnfe) {
        throw new AssertionError(cnfe.toString(), cnfe);
      } catch (Exception e) {
        String exMsg = "Reflection attempt failed while attempting to find all"
                + " StringId instances. ";
        throw new AssertionError(exMsg + e.toString(), e);
      }
    }
    return allStringIds;
  }

  /**
   * Lists all of the classes that define StringId instances
   * @return a set of all classes that contain StringId declarations
   */
  private Set<String> getStringIdDefiningClasses() {
    final Set<String> StringIdDefiningClasses = new LinkedHashSet<String>();
    final String pkg = "org.apache.geode.internal.i18n.";
//    StringIdDefiningClasses.add(pkg + "ParentLocalizedStrings");
    StringIdDefiningClasses.add(pkg + "LocalizedStrings");
    StringIdDefiningClasses.add("org.apache.geode.management.internal.ManagementStrings");
    return StringIdDefiningClasses;
  }

  /**
   * Helper to access a private method via reflection, thus allowing
   * us to not expose them to customers.
   */
  private Locale getCurrentLocale() {
    Class<?> c = StringId.class;
    Locale locale = null;
    try {
      Method m = c.getDeclaredMethod("getCurrentLocale");
      m.setAccessible(true);
      locale = (Locale)m.invoke(null);
    } catch (Exception e) {
      String msg = "Reflection attempt failed for StringId.getCurrentLocale ";
      throw new AssertionError( msg + e.toString(), e);
    }
    return locale;
  }

  /**
   *  Helper to access a private method via reflection, thus allowing
   *  us to not expose them to customers.
   */
  private AbstractStringIdResourceBundle getActiveResourceBundle() {
    Class<?> c = StringId.class;
    AbstractStringIdResourceBundle rb = null;
    try {
      Method m = c.getDeclaredMethod("getActiveResourceBundle");
      m.setAccessible(true);
      rb = (AbstractStringIdResourceBundle)m.invoke(null);
    } catch (Exception e) {
      String msg = "Reflection attempt failed for StringId.getActiveResourceBundle ";
      throw new AssertionError(msg + e.toString(), e);
    }
    return rb;
  }

  /**
   * Check that the "raw" string matches the "formatted" string after taking
   * into account known changes due to the fomatting process.
   * For example:
   * <code>
   * "I''m using a contraction." should become "I'm using a contraction."
   * </code>
   */
  private void verifyStringsAreProperlyEscaped(Locale loc) {
    StringId.setLocale(loc);

    final Set<StringId> misquoted = new HashSet<StringId>();

    final Object[] identityArgs = new Object[100];

    for (int index = 0; index < identityArgs.length; index++) {
      identityArgs[index] = index;
    }

    final AbstractStringIdResourceBundle rb = getActiveResourceBundle();
    for(StringId instance : getAllStringIds()) {
      String raw = rb.getString(instance);
      String altered = raw.replaceAll("''", "'");
      altered = altered.replaceAll("\\{([0-9]+)[^\\}]*\\}", "$1");
      if (!rb.usingRawMode()) {
        altered = "msgID " + ((StringId)instance).id + ": " + altered;
      }
      String formatted = null;
      try {
        formatted = instance.toLocalizedString(identityArgs);
      } catch(IllegalArgumentException iae) {
        String testName = this.getClass().getName().replaceAll("\\.", "/")
                + ".class";
        String exMsg = "Improper message id=" + ((StringId)instance).id + "\n"
                + "Usually this is caused by an unmatched or nested \"{\"\n"
                + "Examples:\t\"{0]\" or \"{ {0} }\"\n"
                + "This is just the first failure, it is in your interest"
                + " to rebuild and run just this one test.\n"
                + "build.sh run-java-tests -Djunit.testcase="
                + testName;
        throw new AssertionError(exMsg, iae);
      }
      if(! altered.equals(formatted)) {
        System.err.println("altered:   " + altered);
        System.err.println("formatted: " + formatted);
        misquoted.add(instance);
      }
    }
    if(! misquoted.isEmpty()) {
      StringBuffer err = new StringBuffer();
      err.append("These errors are usually resolved by replacing ");
      err.append("\"'\" with \"''\".\n");
      err.append("If the error is in the non-english version then ");
      err.append("alter the text in StringIdResouceBundle_{lang}.txt.\n");
      err.append("The following misquoted StringIds were found:");
      for(StringId i : misquoted) {
        err.append("\n")
                .append("StringId id=")
                .append(((StringId)i).id)
                .append(" : text=\"")
                .append(i.getRawText())
                .append("\"");
      }
      fail(err.toString());
    }
  }
}
