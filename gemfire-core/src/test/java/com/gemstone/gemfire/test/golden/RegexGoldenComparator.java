package com.gemstone.gemfire.test.golden;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Compares test output to golden text file using regex pattern matching
 * 
 * @author Kirk Lund
 */
public class RegexGoldenComparator extends GoldenComparator {
  
  protected RegexGoldenComparator(String[] expectedProblemLines) {
    super(expectedProblemLines);
  }
  
  @Override
  protected boolean compareLines(String actualLine, String goldenLine) {
    logger.debug(GoldenTestCase.GOLDEN_TEST, "RegexGoldenComparator:compareLines comparing \"{}\" to \"{}\"", actualLine, goldenLine);
    Matcher matcher = Pattern.compile(goldenLine).matcher(actualLine);
    return matcher.matches();
  }
}
