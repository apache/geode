package org.apache.geode.cache;

import static org.junit.Assert.fail;

import org.apache.geode.internal.cache.InternalRegionArguments;
import org.apache.geode.internal.cache.LocalRegion;
import org.apache.geode.test.junit.categories.UnitTest;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Category(UnitTest.class)
public class RegionNameValidationJUnitTest {
  private static final Pattern NAME_PATTERN = Pattern.compile("[aA-zZ0-9-_.]+");
  private static final String REGION_NAME = "MyRegion";


  @Test
  public void testInvalidNames() {
    InternalRegionArguments ira = new InternalRegionArguments();
    ira.setInternalRegion(false);
    try {
      LocalRegion.validateRegionName(null, ira);
      fail();
    } catch (IllegalArgumentException ignore) {
    }
    try {
      LocalRegion.validateRegionName("", ira);
      fail();
    } catch (IllegalArgumentException ignore) {
    }
    try {
      LocalRegion.validateRegionName("FOO" + Region.SEPARATOR, ira);
      fail();
    } catch (IllegalArgumentException ignore) {
    }

  }

  @Test
  public void testExternalRegionNames() {
    InternalRegionArguments ira = new InternalRegionArguments();
    ira.setInternalRegion(false);
    validateCharacters(ira);
    try {
      LocalRegion.validateRegionName("__InvalidInternalRegionName", ira);
    } catch (IllegalArgumentException ignore) {
    }
  }

  @Test
  public void testInternalRegionNames() {
    InternalRegionArguments ira = new InternalRegionArguments();
    ira.setInternalRegion(true);
    LocalRegion.validateRegionName("__ValidInternalRegionName", ira);
  }

  private void validateCharacters(InternalRegionArguments ira) {
    for (int x = 0; x < Character.MAX_VALUE; x++) {
      String name = (char) x + REGION_NAME;
      Matcher matcher = NAME_PATTERN.matcher(name);
      if (matcher.matches()) {
        LocalRegion.validateRegionName(name, ira);
      } else {
        try {
          LocalRegion.validateRegionName(name, ira);
          fail("Should have received an IllegalArgumentException");
        } catch (IllegalArgumentException ignore) {
        }
      }
    }
  }
}
