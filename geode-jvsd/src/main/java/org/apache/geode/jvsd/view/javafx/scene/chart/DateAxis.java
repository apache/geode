/*
 * Portions of this source copied with love from https://bitbucket.org/sco0ter/extfx
 * under the following license:
 * 
 * The MIT License (MIT)
 *
 * Copyright (c) 2013, Christian Schudt
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package org.apache.geode.jvsd.view.javafx.scene.chart;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;

import javafx.animation.KeyFrame;
import javafx.animation.KeyValue;
import javafx.beans.property.ReadOnlyDoubleWrapper;
import javafx.scene.chart.ValueAxis;
import javafx.util.Duration;
import javafx.util.StringConverter;

import com.sun.javafx.charts.ChartLayoutAnimator;

/**
 * Implemenation of {@link ValueAxis} similar to {@link NumberAxis} but with
 * formatting for date and time.
 * 
 * @author jbarrett
 * 
 */
public final class DateAxis extends ValueAxis<Number> {

  private ChartLayoutAnimator animator = new ChartLayoutAnimator(this);

  private Object currentAnimationID;

  private Interval actualInterval = Interval.DECADE;

  /**
   * Default constructor. By default the lower and upper bound are calculated by
   * the data.
   */
  public DateAxis() {
  }

  /**
   * Constructs a date axis with fix lower and upper bounds.
   * 
   * @param lowerBound
   *          The lower bound.
   * @param upperBound
   *          The upper bound.
   */
  public DateAxis(Long lowerBound, Long upperBound) {
    this();
    setAutoRanging(false);
    setLowerBound(lowerBound);
    setUpperBound(upperBound);
  }

  /**
   * Constructs a date axis with a label and fix lower and upper bounds.
   * 
   * @param axisLabel
   *          The label for the axis.
   * @param lowerBound
   *          The lower bound.
   * @param upperBound
   *          The upper bound.
   */
  public DateAxis(String axisLabel, Long lowerBound, Long upperBound) {
    this(lowerBound, upperBound);
    setLabel(axisLabel);
  }

  @Override
  protected Object autoRange(double minValue, double maxValue, double length, double labelSize) {
    return new Object[] { (long) minValue, (long) maxValue, calculateNewScale(length, (long) minValue, (long) maxValue) };
  }

  @Override
  protected void setRange(Object range, boolean animating) {
    Object[] r = (Object[]) range;
    long oldLowerBound = (long) getLowerBound();
    long lower = (long) r[0];
    long upper = (long) r[1];
    double scale = (double) r[2];
    setLowerBound(lower);
    setUpperBound(upper);

    if (animating) {
      animator.stop(currentAnimationID);
      currentAnimationID = animator.animate(
          new KeyFrame(Duration.ZERO,
              new KeyValue(currentLowerBound, oldLowerBound),
              new KeyValue(scalePropertyImplProtected(), getScale())),
          new KeyFrame(Duration.millis(700),
              new KeyValue(currentLowerBound, lower), 
              new KeyValue(scalePropertyImplProtected(), scale)));

    } else {
      currentLowerBound.set(getLowerBound());
      setScale(scale);
    }
  }

  @Override
  protected Object getRange() {
    return new Object[] { (long) getLowerBound(), (long) getUpperBound() };
  }

  @Override
  protected List<Number> calculateTickValues(double v, Object range) {
    Object[] r = (Object[]) range;
    long lower = (long) r[0];
    long upper = (long) r[1];

    List<Date> dateList = new ArrayList<Date>();
    Calendar calendar = Calendar.getInstance();

    // The preferred gap which should be between two tick marks.
    double averageTickGap = 100;
    double averageTicks = v / averageTickGap;

    List<Date> previousDateList = new ArrayList<Date>();

    Interval previousInterval = Interval.values()[0];

    // Starting with the greatest interval, add one of each calendar unit.
    for (Interval interval : Interval.values()) {
      // Reset the calendar.
      calendar.setTime(new Date(lower));
      // Clear the list.
      dateList.clear();
      previousDateList.clear();
      actualInterval = interval;

      // Loop as long we exceeded the upper bound.
      while (calendar.getTime().getTime() <= upper) {
        dateList.add(calendar.getTime());
        calendar.add(interval.interval, interval.amount);
      }
      // Then check the size of the list. If it is greater than the amount of
      // ticks, take that list.
      if (dateList.size() > averageTicks) {
        calendar.setTime(new Date(lower));
        // Recheck if the previous interval is better suited.
        while (calendar.getTime().getTime() <= upper) {
          previousDateList.add(calendar.getTime());
          calendar.add(previousInterval.interval, previousInterval.amount);
        }
        break;
      }

      previousInterval = interval;
    }
    if (previousDateList.size() - averageTicks > averageTicks - dateList.size()) {
      dateList = previousDateList;
      actualInterval = previousInterval;
    }

    // At last add the upper bound.
    dateList.add(new Date(upper));

    List<Number> evenDateList = makeDatesEven(dateList, calendar);
    // If there are at least three dates, check if the gap between the lower
    // date and the second date is at least half the gap of the second and third
    // date.
    // Do the same for the upper bound.
    // If gaps between dates are to small, remove one of them.
    // This can occur, e.g. if the lower bound is 25.12.2013 and years are
    // shown. Then the next year shown would be 2014 (01.01.2014) which would be
    // too narrow to 25.12.2013.
    if (evenDateList.size() > 2) {

      long secondDate = evenDateList.get(1).longValue();
      long thirdDate = evenDateList.get(2).longValue();
      long lastDate = evenDateList.get(dateList.size() - 2).longValue();
      long previousLastDate = evenDateList.get(dateList.size() - 3).longValue();

      // If the second date is too near by the lower bound, remove it.
      if (secondDate - lower < (thirdDate - secondDate) / 2) {
        evenDateList.remove(secondDate);
      }

      // If difference from the upper bound to the last date is less than the
      // half of the difference of the previous two dates,
      // we better remove the last date, as it comes to close to the upper
      // bound.
      if (upper - lastDate < (lastDate - previousLastDate) / 2) {
        evenDateList.remove(lastDate);
      }
    }

    return evenDateList;
  }

  @Override
  protected String getTickMarkLabel(final Number ts) {
    final Date date = new Date(ts.longValue());

    StringConverter<Number> converter = getTickLabelFormatter();
    if (converter != null) {
      return converter.toString(date.getTime());
    }

    DateFormat dateFormat;
    Calendar calendar = Calendar.getInstance();
    calendar.setTime(date);

    if (actualInterval.interval == Calendar.YEAR && calendar.get(Calendar.MONTH) == 0 && calendar.get(Calendar.DATE) == 1) {
      dateFormat = new SimpleDateFormat("yyyy");
    } else if (actualInterval.interval == Calendar.MONTH && calendar.get(Calendar.DATE) == 1) {
      dateFormat = new SimpleDateFormat("MMM yy");
    } else {
      switch (actualInterval.interval) {
      case Calendar.DATE:
      case Calendar.WEEK_OF_YEAR:
      default:
        dateFormat = DateFormat.getDateInstance(DateFormat.MEDIUM);
        break;
      case Calendar.HOUR:
      case Calendar.MINUTE:
        dateFormat = DateFormat.getTimeInstance(DateFormat.SHORT);
        break;
      case Calendar.SECOND:
        dateFormat = DateFormat.getTimeInstance(DateFormat.MEDIUM);
        break;
      case Calendar.MILLISECOND:
        dateFormat = DateFormat.getTimeInstance(DateFormat.FULL);
        break;
      }
    }
    return dateFormat.format(date);
  }

  /**
   * Makes dates even, in the sense of that years always begin in January,
   * months always begin on the 1st and days always at midnight.
   * 
   * @param dates
   *          The list of dates.
   * @return The new list of dates.
   */
  private List<Number> makeDatesEven(List<Date> dates, Calendar calendar) {
    // If the dates contain more dates than just the lower and upper bounds,
    // make the dates in between even.
    // TODO if (dates.size() > 2) {
    List<Number> evenDates = new ArrayList<Number>();

    // For each interval, modify the date slightly by a few millis, to make sure
    // they are different days.
    // This is because Axis stores each value and won't update the tick labels,
    // if the value is already known.
    // This happens if you display days and then add a date many years in the
    // future the tick label will still be displayed as day.
    for (int i = 0; i < dates.size(); i++) {
      calendar.setTime(dates.get(i));
      switch (actualInterval.interval) {
      case Calendar.YEAR:
        // If its not the first or last date (lower and upper bound), make the
        // year begin with first month and let the months begin with first day.
        if (i != 0 && i != dates.size() - 1) {
          calendar.set(Calendar.MONTH, 0);
          calendar.set(Calendar.DATE, 1);
        }
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 6);
        break;
      case Calendar.MONTH:
        // If its not the first or last date (lower and upper bound), make the
        // months begin with first day.
        if (i != 0 && i != dates.size() - 1) {
          calendar.set(Calendar.DATE, 1);
        }
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 5);
        break;
      case Calendar.WEEK_OF_YEAR:
        // Make weeks begin with first day of week?
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 4);
        break;
      case Calendar.DATE:
        calendar.set(Calendar.HOUR_OF_DAY, 0);
        calendar.set(Calendar.MINUTE, 0);
        calendar.set(Calendar.SECOND, 0);
        calendar.set(Calendar.MILLISECOND, 3);
        break;
      case Calendar.HOUR:
        if (i != 0 && i != dates.size() - 1) {
          calendar.set(Calendar.MINUTE, 0);
          calendar.set(Calendar.SECOND, 0);
        }
        calendar.set(Calendar.MILLISECOND, 2);
        break;
      case Calendar.MINUTE:
        if (i != 0 && i != dates.size() - 1) {
          calendar.set(Calendar.SECOND, 0);
        }
        calendar.set(Calendar.MILLISECOND, 1);
        break;
      case Calendar.SECOND:
        calendar.set(Calendar.MILLISECOND, 0);
        break;

      }
      evenDates.add(calendar.getTime().getTime());
    }

    return evenDates;
    // } else {
    // return dates;
    // }
  }

  /**
   * The intervals, which are used for the tick labels. Beginning with the
   * largest interval, the axis tries to calculate the tick values for this
   * interval. If a smaller interval is better suited for, that one is taken.
   */
  private enum Interval {
    DECADE(Calendar.YEAR, 10), YEAR(Calendar.YEAR, 1), MONTH_6(Calendar.MONTH, 6), MONTH_3(Calendar.MONTH, 3), MONTH_1(Calendar.MONTH, 1), WEEK(
        Calendar.WEEK_OF_YEAR, 1), DAY(Calendar.DATE, 1), HOUR_12(Calendar.HOUR, 12), HOUR_6(Calendar.HOUR, 6), HOUR_3(Calendar.HOUR, 3), HOUR_1(Calendar.HOUR,
        1), MINUTE_15(Calendar.MINUTE, 15), MINUTE_5(Calendar.MINUTE, 5), MINUTE_1(Calendar.MINUTE, 1), SECOND_15(Calendar.SECOND, 15), SECOND_5(
        Calendar.SECOND, 5), SECOND_1(Calendar.SECOND, 1), MILLISECOND(Calendar.MILLISECOND, 1);

    private final int amount;

    private final int interval;

    private Interval(int interval, int amount) {
      this.interval = interval;
      this.amount = amount;
    }
  }

  @Override
  protected List<Number> calculateMinorTickMarks() {
    return Collections.<Number>emptyList();
  }

  /* Access to package protected method */
  private static final Method scalePropertyImplMethod;

  static {
    try {
      scalePropertyImplMethod = ValueAxis.class.getDeclaredMethod("scalePropertyImpl");
      scalePropertyImplMethod.setAccessible(true);
    } catch (SecurityException | NoSuchMethodException e) {
      throw new IllegalStateException(e);
    }
  }

  protected ReadOnlyDoubleWrapper scalePropertyImplProtected() {
    try {
      return (ReadOnlyDoubleWrapper) scalePropertyImplMethod.invoke(this);
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
      throw new IllegalStateException(e);
    }
  }

}