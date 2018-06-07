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

package org.apache.geode.test.junit.assertions;

import static org.assertj.core.api.Assertions.assertThat;

import org.assertj.core.api.AbstractCharSequenceAssert;
import org.assertj.core.api.ListAssert;

import org.apache.geode.management.internal.cli.result.model.InfoResultModel;

public class InfoResultModelAssert
    extends AbstractResultModelAssert<InfoResultModelAssert, InfoResultModel> {

  public InfoResultModelAssert(InfoResultModel infoResultModel) {
    super(infoResultModel, InfoResultModelAssert.class);
  }

  public AbstractCharSequenceAssert<?, String> hasOutput() {
    String output = String.join(System.lineSeparator(), actual.getContent());
    return assertThat(output);
  }

  public ListAssert<String> hasLines() {
    return assertThat(actual.getContent());
  }
}
