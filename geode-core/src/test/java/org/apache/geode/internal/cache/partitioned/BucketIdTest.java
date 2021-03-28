package org.apache.geode.internal.cache.partitioned;


import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Test;

public class BucketIdTest {

  @SuppressWarnings("ResultOfMethodCallIgnored")
  @Test
  public void throwsExceptionsWhenOutOfBounds() {
    assertThatThrownBy(() -> BucketId.valueOf(-1)).isInstanceOf(IndexOutOfBoundsException.class);
    assertThatThrownBy(() -> BucketId.valueOf(BucketId.max + 1))
        .isInstanceOf(IndexOutOfBoundsException.class);
  }

  @Test
  public void returnsSameValueForMultipleCalls() {
    for (int i = 0; i < BucketId.max; i++) {
      assertThat(BucketId.valueOf(i)).isSameAs(BucketId.valueOf(i));
    }
  }

  @Test
  public void returnsValueForIntValue() {
    for (int i = 0; i < BucketId.max; i++) {
      assertThat(BucketId.valueOf(i).intValue()).isEqualTo(i);
    }
  }

}
