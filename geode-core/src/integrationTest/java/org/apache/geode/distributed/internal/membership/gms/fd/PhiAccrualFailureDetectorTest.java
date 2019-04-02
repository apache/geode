/*
 * Copyright 2018 Mitsunori Komatsu (komamitsu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.geode.distributed.internal.membership.gms.fd;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.experimental.categories.Category;

import org.apache.geode.test.junit.categories.MembershipTest;

@Category({MembershipTest.class})
public class PhiAccrualFailureDetectorTest {
  @Test
  public void test() {
    PhiAccrualFailureDetector failureDetector = new PhiAccrualFailureDetector.Builder().build();
    long now = 1420070400000L;
    for (int i = 0; i < 300; i++) {
      long timestampMillis = now + i * 1000;

      if (i > 290) {
        double phi = failureDetector.phi(timestampMillis);
        if (i == 291) {
          assertTrue(1 < phi && phi < 3);
          assertTrue(failureDetector.isAvailable(timestampMillis));
        } else if (i == 292) {
          assertTrue(3 < phi && phi < 8);
          assertTrue(failureDetector.isAvailable(timestampMillis));
        } else if (i == 293) {
          assertTrue(8 < phi && phi < 16);
          assertTrue(failureDetector.isAvailable(timestampMillis));
        } else if (i == 294) {
          assertTrue(16 < phi && phi < 30);
          assertFalse(failureDetector.isAvailable(timestampMillis));
        } else if (i == 295) {
          assertTrue(30 < phi && phi < 50);
          assertFalse(failureDetector.isAvailable(timestampMillis));
        } else if (i == 296) {
          assertTrue(50 < phi && phi < 70);
          assertFalse(failureDetector.isAvailable(timestampMillis));
        } else if (i == 297) {
          assertTrue(70 < phi && phi < 100);
          assertFalse(failureDetector.isAvailable(timestampMillis));
        } else {
          assertTrue(100 < phi);
          assertFalse(failureDetector.isAvailable(timestampMillis));
        }
        continue;
      } else if (i > 200) {
        if (i % 5 == 0) {
          double phi = failureDetector.phi(timestampMillis);
          assertTrue(0.1 < phi && phi < 0.5);
          assertTrue(failureDetector.isAvailable(timestampMillis));
          continue;
        }
      }
      failureDetector.heartbeat(timestampMillis);
      assertTrue(failureDetector.phi(timestampMillis) < 0.1);
      assertTrue(failureDetector.isAvailable(timestampMillis));
    }
  }
}
