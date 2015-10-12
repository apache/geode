package com.gemstone.gemfire.test.golden;

/**
 * Compares test output to golden text file using string equality
 * 
 * @author Kirk Lund
 */
public class StringGoldenComparator extends GoldenComparator {

  protected StringGoldenComparator(final String[] expectedProblemLines) {
    super(expectedProblemLines);
  }
  
  @Override
  protected boolean compareLines(final String actualLine, final String goldenLine) {
    if (actualLine == null) {
      return goldenLine == null;
    }
    return actualLine.equals(goldenLine);
  }
}
