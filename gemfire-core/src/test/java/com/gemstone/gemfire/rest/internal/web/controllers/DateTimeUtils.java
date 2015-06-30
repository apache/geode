package com.gemstone.gemfire.rest.internal.web.controllers;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

public class DateTimeUtils {
  public static Calendar createCalendar(final int year, final int month, final int day) {
    final Calendar dateTime = Calendar.getInstance();
    dateTime.clear();
    dateTime.set(Calendar.YEAR, year);
    dateTime.set(Calendar.MONTH, month);
    dateTime.set(Calendar.DAY_OF_MONTH, day);
    return dateTime;
  }

  public static Date createDate(final int year, final int month, final int day) {
    return createCalendar(year, month, day).getTime();
  }

  public static String format(final Date dateTime, final String formatPattern) {
    return (dateTime != null ? new SimpleDateFormat(formatPattern).format(dateTime) : null);
  }
}
