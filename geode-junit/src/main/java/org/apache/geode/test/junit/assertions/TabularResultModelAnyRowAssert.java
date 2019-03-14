package org.apache.geode.test.junit.assertions;

import static org.assertj.core.api.Assertions.fail;

import org.apache.commons.lang3.StringUtils;

public class TabularResultModelAnyRowAssert<T extends String> {
  protected TabularResultModelAssert parent;

  TabularResultModelAnyRowAssert(TabularResultModelAssert tabularResultModelAssert) {
    this.parent = tabularResultModelAssert;
  }

  /**
   * Verifies that any row contains the given values, in any order.
   */
  @SafeVarargs
  public final TabularResultModelAssert contains(T... rowValues) {
    for (int i = 0; i < parent.getActual().getRowSize(); i++) {
      try {
        parent.hasRow(i).contains(rowValues);
        return parent;
      } catch (AssertionError ignore) {
      }
    }
    fail("Did not find " + StringUtils.join(rowValues, " and ") + " in any rows");
    return parent;
  }

  /**
   * Verifies that any row contains only the given values and nothing else, in any order and
   * ignoring duplicates (i.e. once a value is found, its duplicates are also considered found).
   */
  @SafeVarargs
  public final TabularResultModelAssert containsOnly(T... rowValues) {
    for (int i = 0; i < parent.getActual().getRowSize(); i++) {
      try {
        parent.hasRow(i).containsOnly(rowValues);
        return parent;
      } catch (AssertionError ignore) {
      }
    }
    fail("Did not find only " + StringUtils.join(rowValues, " and ") + " in any rows");
    return parent;
  }

  /**
   * Verifies that any row contains exactly the given values and nothing else, <b>in order</b>.
   */
  @SafeVarargs
  public final TabularResultModelAssert containsExactly(T... rowValues) {
    for (int i = 0; i < parent.getActual().getRowSize(); i++) {
      try {
        parent.hasRow(i).containsExactly(rowValues);
        return parent;
      } catch (AssertionError ignore) {
      }
    }
    fail("Did not find exactly [" + StringUtils.join(rowValues, ", ") + "] in any rows");
    return parent;
  }

  /**
   * Verifies that any row contains at least one of the given values.
   */
  @SafeVarargs
  public final TabularResultModelAssert containsAnyOf(T... rowValues) {
    for (int i = 0; i < parent.getActual().getRowSize(); i++) {
      try {
        parent.hasRow(i).containsAnyOf(rowValues);
        return parent;
      } catch (AssertionError ignore) {
      }
    }
    fail("Did not find " + StringUtils.join(rowValues, " or ") + " in any rows");
    return parent;
  }
}
