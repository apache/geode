package org.apache.geode.test.junit.assertions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

public class TabularResultModelRowAssert<T> extends TabularResultModelSliceAssert<T> {
  TabularResultModelRowAssert(TabularResultModelAssert tabularResultModelAssert,
      List<T> valuesInRow) {
    super(tabularResultModelAssert, valuesInRow);
  }

  /**
   * verifies the expected number of values in the row
   */
  public final TabularResultModelRowAssert<T> hasSize(int expected) {
    assertThat(this.values).hasSize(expected);
    return this;
  }
}
