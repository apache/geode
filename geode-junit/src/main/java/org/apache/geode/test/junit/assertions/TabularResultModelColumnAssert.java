package org.apache.geode.test.junit.assertions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

public class TabularResultModelColumnAssert<T> extends TabularResultModelSliceAssert<T> {
  TabularResultModelColumnAssert(TabularResultModelAssert tabularResultModelAssert,
      List<T> valuesInColumn) {
    super(tabularResultModelAssert, valuesInColumn);
  }

  /**
   * Verifies that the selected column contains the given values only once.
   */
  @SafeVarargs
  public final TabularResultModelAssert containsOnlyOnce(T... values) {
    assertThat(this.values).containsOnlyOnce(values);
    return parent;
  }

  /**
   * Verifies that the selected column contains exactly the given values and nothing else, <b>in any
   * order</b>.
   */
  @SafeVarargs
  public final TabularResultModelAssert containsExactlyInAnyOrder(T... values) {
    assertThat(this.values).containsExactlyInAnyOrder(values);
    return parent;
  }

  /**
   * verifies the expected number of values in the column
   */
  public final TabularResultModelColumnAssert<T> hasSize(int expected) {
    assertThat(this.values).hasSize(expected);
    return this;
  }
}
