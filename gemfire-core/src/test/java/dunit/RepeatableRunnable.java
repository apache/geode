package dunit;

/**
 * A RepeatableRunnable is an object that implements a method that
 * can be invoked repeatably without causing any side affects.
 *
 * @author  dmonnie
 */
public interface RepeatableRunnable {
  
  public void runRepeatingIfNecessary(long repeatTimeoutMs);
  
}
