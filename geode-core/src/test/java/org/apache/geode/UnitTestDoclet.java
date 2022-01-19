/*
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.geode;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.text.BreakIterator;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.Set;
import java.util.TreeSet;

import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.DocErrorReporter;
import com.sun.javadoc.MethodDoc;
import com.sun.javadoc.RootDoc;
import junit.framework.TestCase;
import perffmwk.Formatter;

/**
 * This class is a Javadoc
 * <A href="http://java.sun.com/j2se/1.4.2/docs/tooldocs/javadoc/overview.html">doclet</A> that
 * generates a text file that summarizes unit test classes and methods.
 *
 * @see com.sun.javadoc.Doclet
 *
 *
 * @since GemFire 3.0
 */
public class UnitTestDoclet {

  /**
   * Returns the number of arguments for the given command option (include the option itself)
   */
  public static int optionLength(String option) {
    if (option.equals("-output")) {
      return 2;

    } else {
      // Unknown option
      return 0;
    }
  }

  public static boolean validOptions(String[][] options, DocErrorReporter reporter) {
    boolean sawOutput = false;

    for (String[] option : options) {
      if (option[0].equals("-output")) {
        File output = new File(option[1]);
        if (output.exists() && output.isDirectory()) {
          reporter.printError("Output file " + output + " is a directory");
          return false;

        } else {
          sawOutput = true;
        }
      }
    }

    if (!sawOutput) {
      reporter.printError("Missing -output");
      return false;
    }

    return true;
  }

  /**
   * The entry point for the doclet
   */
  public static boolean start(RootDoc root) {
    String[][] options = root.options();

    File outputFile = null;
    for (String[] option : options) {
      if (option[0].equals("-output")) {
        outputFile = new File(option[1]);
      }
    }

    if (outputFile == null) {
      root.printError("Internal Error: No output file");
      return false;

    } else {
      root.printNotice("Generating " + outputFile);
    }

    try {
      PrintWriter pw = new PrintWriter(new FileWriter(outputFile));
      Formatter.center("GemFire Unit Test Summary", pw);
      Formatter.center(new Date().toString(), pw);
      pw.println("");

      ClassDoc[] classes = root.classes();
      Arrays.sort(classes, (Comparator) (o1, o2) -> {
        ClassDoc c1 = (ClassDoc) o1;
        ClassDoc c2 = (ClassDoc) o2;
        return c1.qualifiedName().compareTo(c2.qualifiedName());
      });
      for (ClassDoc c : classes) {
        if (!c.isAbstract() && isUnitTest(c)) {
          document(c, pw);
        }
      }

      pw.flush();
      pw.close();

    } catch (IOException ex) {
      StringWriter sw = new StringWriter();
      ex.printStackTrace(new PrintWriter(sw, true));
      root.printError(sw.toString());
      return false;
    }

    return true;
  }

  /**
   * Returns whether or not a class is a unit test. That is, whether or not it is a subclass of
   * {@link junit.framework.TestCase}.
   */
  private static boolean isUnitTest(ClassDoc c) {
    if (c == null) {
      return false;

    } else if (c.qualifiedName().equals(TestCase.class.getName())) {
      return true;

    } else {
      return isUnitTest(c.superclass());
    }
  }

  /**
   * Summarizes the test methods of the given class
   */
  public static void document(ClassDoc c, PrintWriter pw) throws IOException {

    pw.println(c.qualifiedName());

    {
      String comment = c.commentText();
      if (comment != null && !comment.equals("")) {
        pw.println("");
        indent(comment, 4, pw);
        pw.println("");
      }
    }

    MethodDoc[] methods = getTestMethods(c);
    for (MethodDoc method : methods) {
      pw.print("  ");
      pw.println(method.name());

      String comment = method.commentText();
      if (comment != null && !comment.equals("")) {
        pw.println("");
        indent(comment, 6, pw);
        pw.println("");
      }
    }

    pw.println("");
  }

  /**
   * Returns an array containing all of the "test" methods (including those that are inherited) for
   * the given class.
   */
  private static MethodDoc[] getTestMethods(ClassDoc c) {
    Set set = new TreeSet();
    while (c != null) {
      MethodDoc[] methods = c.methods();
      for (MethodDoc method : methods) {
        if (method.isPublic() && method.parameters().length == 0
            && method.name().startsWith("test")) {
          set.add(method);
        }
      }

      c = c.superclass();
    }

    return (MethodDoc[]) set.toArray(new MethodDoc[0]);
  }

  /**
   * Indents a block of text a given amount.
   */
  private static void indent(String text, final int indent, PrintWriter pw) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < indent; i++) {
      sb.append(" ");
    }
    String spaces = sb.toString();

    pw.print(spaces);

    int printed = indent;
    boolean firstWord = true;

    BreakIterator boundary = BreakIterator.getWordInstance();
    boundary.setText(text);
    int start = boundary.first();
    for (int end = boundary.next(); end != BreakIterator.DONE; start = end, end = boundary.next()) {

      String word = text.substring(start, end);

      if (printed + word.length() > 72) {
        pw.println("");
        pw.print(spaces);
        printed = indent;
        firstWord = true;
      }

      if (word.charAt(word.length() - 1) == '\n') {
        pw.write(word, 0, word.length() - 1);

      } else if (firstWord && Character.isWhitespace(word.charAt(0))) {
        pw.write(word, 1, word.length() - 1);

      } else {
        pw.print(word);
      }
      printed += (end - start);
      firstWord = false;
    }

    pw.println("");
  }

}
