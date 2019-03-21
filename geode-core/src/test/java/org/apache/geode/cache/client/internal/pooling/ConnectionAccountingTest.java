package org.apache.geode.cache.client.internal.pooling;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.Test;

public class ConnectionAccountingTest {
  @Test
  public void constructorSetsMinMax() {
    ConnectionAccounting a = new ConnectionAccounting(1, 2);
    assertThat(a.getMin()).isEqualTo(1);
    assertThat(a.getMax()).isEqualTo(2);
    assertThat(a.getCount()).isEqualTo(0);
  }

  @Test
  public void canCreateWhenUnderMax() {
    ConnectionAccounting a = new ConnectionAccounting(0, 1);
    assertThat(a.getCount()).isEqualTo(0);

    assertThat(a.tryCreate()).isTrue();
    assertThat(a.getCount()).isEqualTo(1);
  }

  @Test
  public void cantCreateWhenAtMax() {
    ConnectionAccounting a = new ConnectionAccounting(0, 1);
    a.create();
    assertThat(a.getCount()).isEqualTo(1);

    assertThat(a.tryCreate()).isFalse();
    assertThat(a.getCount()).isEqualTo(1);
  }

  @Test
  public void createRegardlessOfMax() {
    ConnectionAccounting a = new ConnectionAccounting(0, 1);
    a.create();
    assertThat(a.getCount()).isEqualTo(1);

    a.create();
    assertThat(a.getCount()).isEqualTo(2);
    assertThat(a.getMax()).isEqualTo(1);
  }

  @Test
  public void cancelCreateDecrementsCount() {
    ConnectionAccounting a = new ConnectionAccounting(0, 1);
    a.tryCreate();
    assertThat(a.getCount()).isEqualTo(1);

    a.cancelTryCreate();
    assertThat(a.getCount()).isEqualTo(0);
  }

  @Test
  public void tryDestroyDestroysAConnectionOverMax() {
    ConnectionAccounting a = new ConnectionAccounting(0, 1);
    a.create();
    a.create();
    assertThat(a.getCount()).isEqualTo(2);

    assertThat(a.tryDestroy()).isTrue();
  }

  @Test
  public void tryDoesNotDestroyAtMax() {
    ConnectionAccounting a = new ConnectionAccounting(0, 1);
    a.create();
    assertThat(a.getCount()).isEqualTo(1);

    assertThat(a.tryDestroy()).isFalse();
  }

  @Test
  public void cancelTryDestroyIncrementsCount() {
    ConnectionAccounting a = new ConnectionAccounting(0, 1);
    a.create();
    a.create();
    a.tryDestroy();
    assertThat(a.getCount()).isEqualTo(1);

    a.cancelTryDestroy();
    assertThat(a.getCount()).isEqualTo(2);
  }

  @Test
  public void destroyAndIsUnderMinimumReturnsTrueGoingBelowMin() {
    ConnectionAccounting a = new ConnectionAccounting(1, 2);
    a.create();
    assertThat(a.getCount()).isEqualTo(1);

    assertThat(a.destroyAndIsUnderMinimum(1)).isTrue();
    assertThat(a.getCount()).isEqualTo(0);
  }

  @Test
  public void destroyAndIsUnderMinimumReturnsFalseGoingToMin() {
    ConnectionAccounting a = new ConnectionAccounting(1, 2);
    a.create();
    a.create();
    assertThat(a.getCount()).isEqualTo(2);

    assertThat(a.destroyAndIsUnderMinimum(1)).isFalse();
    assertThat(a.getCount()).isEqualTo(1);
  }

  @Test
  public void destroyAndIsUnderMinimumReturnsFalseStayingAboveMin() {
    ConnectionAccounting a = new ConnectionAccounting(1, 2);
    a.create();
    a.create();
    a.create();
    assertThat(a.getCount()).isEqualTo(3);

    assertThat(a.destroyAndIsUnderMinimum(1)).isFalse();
    assertThat(a.getCount()).isEqualTo(2);
  }

  @Test
  public void destroyAndIsUnderMinimumDecrementsByMultiple() {
    ConnectionAccounting a = new ConnectionAccounting(1, 2);
    a.create();
    a.create();
    a.create();
    assertThat(a.getCount()).isEqualTo(3);

    a.destroyAndIsUnderMinimum(3);
    assertThat(a.getCount()).isEqualTo(0);
  }

}