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

package org.apache.geode.redis.internal.parameters;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.function.BiConsumer;

import org.apache.geode.redis.internal.RedisCommandType;
import org.apache.geode.redis.internal.netty.Command;
import org.apache.geode.redis.internal.netty.ExecutionHandlerContext;

public class Parameter {

  private int arity;
  private List<RedisCommandType.Flag> flags = new ArrayList<>();
  private int firstKey = 1;
  private int lastKey = 1;
  private int step = 1;
  private List<BiConsumer<Command, ExecutionHandlerContext>> predicates = new ArrayList<>();

  public void checkParameters(Command command, ExecutionHandlerContext context) {
    predicates.forEach(p -> p.accept(command, context));
  }

  public int getArity() {
    return arity;
  }

  public List<RedisCommandType.Flag> getFlags() {
    return flags;
  }

  public int getFirstKey() {
    return firstKey;
  }

  public int getLastKey() {
    return lastKey;
  }

  public int getStep() {
    return step;
  }

  public Parameter flags(RedisCommandType.Flag... flags) {
    this.flags = Arrays.asList(flags);
    return this;
  }

  public Parameter exact(int exact) {
    arity = exact;
    predicates.add((c, e) -> {
      if (c.getProcessedCommand().size() != exact) {
        throw new RedisParametersMismatchException(c.wrongNumberOfArgumentsErrorMessage());
      }
    });
    return this;
  }

  public Parameter min(int minumum) {
    arity = -minumum;
    predicates.add((c, e) -> {
      if (c.getProcessedCommand().size() < minumum) {
        throw new RedisParametersMismatchException(c.wrongNumberOfArgumentsErrorMessage());
      }
    });
    return this;
  }

  public Parameter max(int maximum) {
    predicates.add((c, e) -> {
      if (c.getProcessedCommand().size() > maximum) {
        throw new RedisParametersMismatchException(c.wrongNumberOfArgumentsErrorMessage());
      }
    });
    return this;
  }

  public Parameter max(int maximum, String customError) {
    predicates.add((c, e) -> {
      if (c.getProcessedCommand().size() > maximum) {
        throw new RedisParametersMismatchException(customError);
      }
    });
    return this;
  }

  public Parameter even() {
    predicates.add((c, e) -> {
      if (c.getProcessedCommand().size() % 2 != 0) {
        throw new RedisParametersMismatchException(c.wrongNumberOfArgumentsErrorMessage());
      }
    });
    return this;
  }

  public Parameter even(String customeError) {
    predicates.add((c, e) -> {
      if (c.getProcessedCommand().size() % 2 != 0) {
        throw new RedisParametersMismatchException(customeError);
      }
    });
    return this;
  }

  public Parameter odd() {
    predicates.add((c, e) -> {
      if (c.getProcessedCommand().size() % 2 == 0) {
        throw new RedisParametersMismatchException(c.wrongNumberOfArgumentsErrorMessage());
      }
    });
    return this;
  }

  public Parameter odd(String customeError) {
    predicates.add((c, e) -> {
      if (c.getProcessedCommand().size() % 2 == 0) {
        throw new RedisParametersMismatchException(customeError);
      }
    });
    return this;
  }

  public Parameter custom(BiConsumer<Command, ExecutionHandlerContext> customCheck) {
    predicates.add(customCheck);
    return this;
  }

  public Parameter firstKey(int firstKey) {
    this.firstKey = firstKey;
    if (firstKey == 0) {
      lastKey = 0;
      step = 0;
    }
    return this;
  }

  public Parameter lastKey(int lastKey) {
    this.lastKey = lastKey;
    return this;
  }

  public Parameter step(int step) {
    this.step = step;
    return this;
  }
}
