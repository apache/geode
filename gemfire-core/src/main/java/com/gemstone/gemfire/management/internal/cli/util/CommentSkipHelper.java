/*
 * =========================================================================
 *  Copyright (c) 2002-2014 Pivotal Software, Inc. All Rights Reserved.
 *  This product is protected by U.S. and international copyright
 *  and intellectual property laws. Pivotal products are covered by
 *  more patents listed at http://www.pivotal.io/patents.
 * ========================================================================
 */
package com.gemstone.gemfire.management.internal.cli.util;

import com.gemstone.gemfire.management.internal.cli.util.spring.Assert;

/**
 * Utility based on code extracted from
 * {@link org.springframework.shell.core.AbstractShell#executeCommand(String)}
 * 
 * @author Abhishek Chaudhari
 * @since 7.0
 */
// @original-author Ben Alex
public class CommentSkipHelper {
  private boolean inBlockComment;
  
  public String skipComments(String line) {
    // We support simple block comments; ie a single pair per line
    if (!inBlockComment && line.contains("/*") && line.contains("*/")) {
      blockCommentBegin();
      String lhs = line.substring(0, line.lastIndexOf("/*"));
      if (line.contains("*/")) {
        line = lhs + line.substring(line.lastIndexOf("*/") + 2);
        blockCommentFinish();
      } else {
        line = lhs;
      }
    } else if (!inBlockComment && line.contains("/*")) {//GemStone Added
      blockCommentBegin();
    }
    if (inBlockComment) {
      if (!line.contains("*/")) {
        return null;
      }
      blockCommentFinish();
      line = line.substring(line.lastIndexOf("*/") + 2);
    }
    // We also support inline comments (but only at start of line, otherwise valid
    // command options like http://www.helloworld.com will fail as per ROO-517)
    if (!inBlockComment && (line.trim().startsWith("//") || line.trim().startsWith("#"))) { // # support in ROO-1116
      line = "";
    }
    // Convert any TAB characters to whitespace (ROO-527)
    line = line.replace('\t', ' ');
    return line;
  }

  public void blockCommentBegin() {
    /**asdsfsdf /*asdsdfsdsd */
    //why dis-allow this??? It's allowed in Java. It was probably because '/*' is considered as a command by Roo.
    Assert.isTrue(!inBlockComment, "Cannot open a new block comment when one already active");
    inBlockComment = true;
  }

  public void blockCommentFinish() {
    Assert.isTrue(inBlockComment, "Cannot close a block comment when it has not been opened");
    inBlockComment = false;
  }

  public void reset() {
    inBlockComment = false;
  }
}
