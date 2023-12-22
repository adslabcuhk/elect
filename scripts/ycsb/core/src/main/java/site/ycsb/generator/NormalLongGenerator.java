/**
 * Copyright (c) 2010 Yahoo! Inc. Copyright (c) 2017 YCSB contributors. All rights reserved.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you
 * may not use this file except in compliance with the License. You
 * may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
 * implied. See the License for the specific language governing
 * permissions and limitations under the License. See accompanying
 * LICENSE file.
 */

package site.ycsb.generator;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Generates longs randomly uniform from an interval.
 */
public class NormalLongGenerator extends NumberGenerator {
  private final double stdev, mean, min, max;
  private final boolean varyValueFlag;

  private static final double EPSILON = 1e-9;

  public static boolean areDoublesEqual(double a, double b) {
    return Math.abs(a - b) < EPSILON;
  }

  /**
   * Creates a generator that will return longs uniformly randomly from the
   * interval [lb,ub] inclusive (that is, lb and ub are possible values)
   * (lb and ub are possible values).
   *
   * @param mean  the mean bound (inclusive) of generated values
   * @param stdev the stdev bound (inclusive) of generated values
   * @param min   the stdev bound (inclusive) of generated values
   * @param max   the stdev bound (inclusive) of generated values
   */
  public NormalLongGenerator(double mean, double stdev, double min, double max) {
    this.mean = mean;
    this.stdev = stdev;
    this.min = min;
    this.max = max;
    if (!areDoublesEqual(min, max)) {
      this.varyValueFlag = true;
    } else {
      this.varyValueFlag = false;
    }
  }

  @Override
  public Long nextValue() {
    // generate a random number from a normal distribution with mean and stdev
    if (varyValueFlag) {
      double result;
      do {
        result = ThreadLocalRandom.current().nextGaussian() * stdev + mean;
      } while (result < min || result > max);
      setLastValue((long) result);
      return (long) result;
    } else {
      setLastValue((long) mean);
      return (long) mean;
    }
  }

  @Override
  public double mean() {
    return mean;
  }
}
