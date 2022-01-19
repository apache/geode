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

package org.apache.geode.management.internal.cli.help;

import java.util.ArrayList;
import java.util.List;

public class Topic implements Comparable<Topic> {
  String topic;
  String desc;
  List<Command> relatedCommands = new ArrayList<>();

  public Topic(String topic, String desc) {
    this.topic = topic;
    this.desc = desc;
  }

  public void addRelatedCommand(String command, String desc) {
    relatedCommands.add(new Command(command, desc));
  }

  @Override
  public int compareTo(Topic o) {
    return topic.compareTo(o.topic);
  }

  public class Command implements Comparable<Command> {
    String command;
    String desc;

    public Command(String command, String desc) {
      this.command = command;
      this.desc = desc;
    }

    @Override
    public int compareTo(Command o) {
      return command.compareTo(o.command);
    }
  }


}
