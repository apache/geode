package org.apache.geode.test.junit.assertions;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;

class TabularResultModelSliceAssert<T> {
  protected TabularResultModelAssert parent;
  protected List<T> values;

  TabularResultModelSliceAssert(TabularResultModelAssert tabularResultModelAssert,
      List<T> valuesInSlice) {
    this.parent = tabularResultModelAssert;
    this.values = valuesInSlice;
  }

  /**
   * Verifies that the selected row or column contains the given values, in any order.
   */
  @SafeVarargs
  public final TabularResultModelAssert contains(T... values) {
    assertThat(this.values).contains(values);
    return parent;
  }

  /**
   * Verifies that the selected row or column contains only the given values and nothing else, in
   * any order and ignoring duplicates (i.e. once a value is found, its duplicates are also
   * considered found).
   */
  @SafeVarargs
  public final TabularResultModelAssert containsOnly(T... values) {
    assertThat(this.values).containsOnly(values);
    return parent;
  }

  /**
   * Verifies that the selected row or column contains exactly the given values and nothing else,
   * <b>in order</b>.
   */
  @SafeVarargs
  public final TabularResultModelAssert containsExactly(T... values) {
    assertThat(this.values).containsExactly(values);
    return parent;
  }

  /**
   * Verifies that the selected row or column contains at least one of the given values.
   */
  @SafeVarargs
  public final TabularResultModelAssert containsAnyOf(T... values) {
    assertThat(this.values).containsAnyOf(values);
    return parent;
  }

  /**
   * Verifies that the selected row or column does not contain the given values.
   */
  @SafeVarargs
  public final TabularResultModelAssert doesNotContain(T... values) {
    assertThat(this.values).doesNotContain(values);
    return parent;
  }

  /**
   * Verifies that all values in the selected row or column are present in the given values.
   */
  @SafeVarargs
  public final TabularResultModelAssert isSubsetOf(T... values) {
    assertThat(this.values).isSubsetOf(values);
    return parent;
  }
}
