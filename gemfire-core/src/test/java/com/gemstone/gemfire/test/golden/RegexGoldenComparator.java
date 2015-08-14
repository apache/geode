package com.gemstone.gemfire.test.golden;

import java.util.regex.Pattern;

/**
 * Compares test output to golden text file using regex pattern matching
 * 
 * @author Kirk Lund
 */
public class RegexGoldenComparator extends GoldenComparator {
  
  protected RegexGoldenComparator(final String[] expectedProblemLines) {
    super(expectedProblemLines);
  }
  
  @Override
  protected boolean compareLines(final String actualLine, final String goldenLine) {
    debug("RegexGoldenComparator:compareLines comparing \" + actualLine + \" to \" + goldenLine + \"");
    return Pattern.compile(goldenLine).matcher(actualLine).matches();
  }
}
