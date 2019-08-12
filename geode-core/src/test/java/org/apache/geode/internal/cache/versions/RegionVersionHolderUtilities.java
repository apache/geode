package org.apache.geode.internal.cache.versions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Arrays;
import java.util.List;

public class RegionVersionHolderUtilities {
  static void assertSameExceptions(List<RVVException> actualExceptions,
      RVVException[] exceptions) {
    List<RVVException> expectedExceptions = Arrays.asList(exceptions);
    assertThat(actualExceptions)
        .withFailMessage("Expected exceptions %s but got %s", expectedExceptions, actualExceptions)
        .hasSize(exceptions.length);

    for (int i = 0; i < exceptions.length; i++) {
      assertThat(actualExceptions.get(i).sameAs(expectedExceptions.get(i)))
          .withFailMessage("Expected exceptions %s but got %s", expectedExceptions,
              actualExceptions)
          .isTrue();
    }
  }
}
