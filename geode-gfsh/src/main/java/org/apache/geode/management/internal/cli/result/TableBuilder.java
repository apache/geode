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
package org.apache.geode.management.internal.cli.result;

/**
 * Helper class to build rows of columnized strings & build a table from those rows.
 *
 * Sample usage:
 *
 * <pre>
 * public Table createTable() {
 *   Table resultTable = TableBuilder.newTable();
 *   resultTable.setColumnSeparator(" | ");
 *
 *   resultTable.newBlankRow();
 *   resultTable.newRow().newLeftCol("Displaying all fields for member: " + memberName);
 *   resultTable.newBlankRow();
 *   RowGroup rowGroup = resultTable.newRowGroup();
 *   rowGroup.newRow().newCenterCol("FIELD1").newCenterCol("FIELD2");
 *   rowGroup.newRowSeparator('-');
 *   for (int i = 0; i < counter; i++) {
 *     rowGroup.newRow().newLeftCol(myFirstField[i]).newLeftCol(mySecondField[i]);
 *   }
 *   resultTable.newBlankRow();
 *
 *   return resultTable;
 * }
 * </pre>
 *
 * Will result in this:
 *
 * <pre>
 *
 * Displaying all fields for member: Member1
 *
 * FIELD1 | FIELD2 -------------- | --------------- My First Field | My Second Field Another Fld1 |
 * Another Fld2 Last Fld1 | Last Fld2
 *
 * </pre>
 *
 * @since GemFire 7.0
 */
class TableBuilder {

  Table newTable() {
    return newTable(new Screen());
  }

  Table newTable(Screen screen) {
    return new Table(screen);
  }
}
