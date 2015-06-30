package com.gemstone.gemfire.cache.query.internal.aggregate;

/**
 * Computes the final non distinct average for a partitioned region based query.
 * This aggregator is instantiated on the PR query node.
 * 
 * @author ashahid
 *
 */
public class AvgPRQueryNode extends Sum {
  private int count = 0;

  /**
   * Takes the input of data received from bucket nodes. The data is of the form
   * of two element array. The first element is the number of values, while the
   * second element is the sum of the values.
   */
  @Override
  public void accumulate(Object value) {
    Object[] array = (Object[]) value;
    this.count += ((Integer) array[0]).intValue();
    super.accumulate(array[1]);
  }

  @Override
  public Object terminate() {
    double sum = ((Number) super.terminate()).doubleValue();
    double result = sum / count;
    return downCast(result);
  }
}
