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
package org.apache.geode.management.internal.cli.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.Before;
import org.junit.Test;


public class CommentSkipHelperTest {

  private CommentSkipHelper commentSkipHelper;

  @Before
  public void setUp() {
    commentSkipHelper = new CommentSkipHelper();
  }

  @Test
  public void nullShouldThrowNullPointerException() {
    assertThatThrownBy(() -> commentSkipHelper.skipComments(null))
        .isExactlyInstanceOf(NullPointerException.class);
  }

  @Test
  public void emptyStringShouldReturnEmptyString() {
    assertThat(commentSkipHelper.skipComments("")).isEqualTo("");
  }

  @Test
  public void stringWithDoubleSlashCommentShouldReturnString() {
    String command = "start locator --name=loc1 //";
    assertThat(commentSkipHelper.skipComments(command)).isEqualTo(command);
  }

  @Test
  public void stringWithSlashAsterCommentShouldRemoveComment() {
    String command = "start locator /* starting locator */ --name=loc1";
    assertThat(commentSkipHelper.skipComments(command))
        .isEqualTo("start locator  --name=loc1");
  }

  @Test
  public void stringWithCommentWithoutSpacesShouldRemoveComment() { // TODO: possible bug
    String command = "start locator/* starting locator */--name=loc1";
    assertThat(commentSkipHelper.skipComments(command)).isEqualTo("start locator--name=loc1");
  }

  @Test
  public void stringWithOpenCommentShouldReturnNull() { // TODO: possible bug
    String command = "start locator /* --name=loc1";
    assertThat(commentSkipHelper.skipComments(command)).isNull();
  }

  @Test
  public void stringWithCloseCommentShouldReturnString() { // TODO: possible bug
    String command = "start locator */ --name=loc1";
    assertThat(commentSkipHelper.skipComments(command)).isEqualTo(command);
  }

  @Test
  public void stringWithMultiLineCommentShouldRemoveComment() {
    String command = "start locator /*\n some \n comment \n */ --name=loc1";
    assertThat(commentSkipHelper.skipComments(command))
        .isEqualTo("start locator  --name=loc1");
  }

  @Test
  public void stringWithCommentAtEndShouldRemoveComment() {
    String command = "start locator --name=loc1 /* comment at end */";
    assertThat(commentSkipHelper.skipComments(command))
        .isEqualTo("start locator --name=loc1 ");
  }

  @Test
  public void stringWithCommentAtBeginningShouldRemoveComment() {
    String command = "/* comment at begin */ start locator --name=loc1";
    assertThat(commentSkipHelper.skipComments(command))
        .isEqualTo(" start locator --name=loc1");
  }

  @Test
  public void stringWithInsideOutCommentShouldMisbehave() { // TODO: possible bug
    String command = "*/ this is a comment /* start locator --name=loc1";
    assertThat(commentSkipHelper.skipComments(command))
        .isEqualTo("*/ this is a comment  this is a comment /* start locator --name=loc1");
  }

}
