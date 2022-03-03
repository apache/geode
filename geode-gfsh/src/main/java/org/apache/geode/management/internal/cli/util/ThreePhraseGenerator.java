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

import java.security.SecureRandom;
import java.util.Random;

/**
 * Picks a random three word phrase based on simple word lists with over 80k possible combinations.
 */
public class ThreePhraseGenerator {
  private static final String[] adjectives =
      "agreeable, alive, better, brave, calm, careful, clever, drab, delightful, eager, elegant, faithful, famous, gentle, gifted, happy, helpful, important, inexpensive, jolly, kind, lively, nice, obedient, odd, polite, powerful, proud, relieved, rich, shy, silly, tender, thankful, uninterested, vast, victorious, witty, wrong, zealous"
          .split(", ");

  private static final String[] nouns =
      "alpha, bravo, ball, bat, bed, book, bun, can, cake, cap, car, cup, delta, echo, hotel, day, kilo, uniform, dust, fan, feet, xray, gun, hall, hat, gamma, jar, kite, beta, map, omega, zeta, pan, post, pie, iota, pot, sigma, theta, sun, toe, tub, van"
          .split(", ");

  private static final String[] verbs =
      "add, allow, bake, sing, call, chase, dance, dream, end, elevate, fasten, fix, gather, give, hold, hug, imagine, itch, jog, jump, kick, knit, land, lock, march, mix, name, notice, obey, open, pass, promise, question, reach, rinse, scatter, stay, talk, turn, untie, use, vanish, visit, walk, work, yawn, yell, zip, zoom"
          .split(", ");

  private final Random prng;

  public ThreePhraseGenerator() {
    prng = new SecureRandom();
  }

  public String generate(char separator) {
    return select(verbs) + separator + select(adjectives)
        + separator + select(nouns);
  }

  private String select(String[] dictionary) {
    return dictionary[prng.nextInt(dictionary.length)];
  }
}
