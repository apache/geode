package com.gemstone.gemfire.test.golden;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;

/**
 * Custom GoldenComparator which returns the string of the golden file name
 * as the output for simple unit testing of the quickstart testing framework.
 * 
 * @author Kirk Lund
 */
public class GoldenStringComparator extends RegexGoldenComparator {

  protected GoldenStringComparator(final String[] expectedProblemLines) {
    super(expectedProblemLines);
  }
  
  @Override
  protected Reader readGoldenFile(final String goldenFileName) throws IOException {
    return new StringReader(goldenFileName);
  }
}
