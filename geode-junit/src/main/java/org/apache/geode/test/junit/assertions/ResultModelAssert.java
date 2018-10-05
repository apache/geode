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

import org.assertj.core.api.AbstractAssert;

import org.apache.geode.management.internal.cli.result.model.DataResultModel;
import org.apache.geode.management.internal.cli.result.model.InfoResultModel;
import org.apache.geode.management.internal.cli.result.model.ResultModel;
import org.apache.geode.management.internal.cli.result.model.TabularResultModel;

public class ResultModelAssert extends AbstractAssert<ResultModelAssert, ResultModel> {

  public ResultModelAssert(ResultModel infoResultModel) {
    super(infoResultModel, ResultModelAssert.class);
  }

  public InfoResultModelAssert hasInfoResultModel(String name) {
    InfoResultModel infoSection = actual.getInfoSection(name);
    assertThat(infoSection).isNotNull();
    return new InfoResultModelAssert(infoSection);
  }

  public DataResultModelAssert hasDateResultModel(String name) {
    DataResultModel dataSection = actual.getDataSection(name);
    assertThat(dataSection).isNotNull();
    return new DataResultModelAssert(dataSection);
  }

  public TabularResultModelAssert hasTabularResultModel(String name) {
    TabularResultModel dataSection = actual.getTableSection(name);
    assertThat(dataSection).isNotNull();
    return new TabularResultModelAssert(dataSection);
  }
}
