/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.geode.management.internal.cli.util;

import org.springframework.util.Assert;

/**
 * Utility based on code extracted from
 * {@link org.springframework.shell.core.AbstractShell#executeCommand(String)}
 * 
 * @since GemFire 7.0
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
        return null; // TODO: should this throw an exception instead?
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

  private void blockCommentBegin() {
    /**asdsfsdf /*asdsdfsdsd */
    //why dis-allow this??? It's allowed in Java. It was probably because '/*' is considered as a command by Roo.
    Assert.isTrue(!inBlockComment, "Cannot open a new block comment when one already active");
    inBlockComment = true;
  }

  private void blockCommentFinish() {
    Assert.isTrue(inBlockComment, "Cannot close a block comment when it has not been opened");
    inBlockComment = false;
  }

  private void reset() { // TODO: delete
    inBlockComment = false;
  }
}
