package com.its.esales.framework.util.calendar

import java.time.{LocalDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter

class DateTimeInfo(currentDateTime: LocalDateTime) {
  // Formatter for the date and time
  val formatter: DateTimeFormatter = DateTimeFormatter.ofPattern("yyyy, MM, dd, HH")

  // Format the provided LocalDateTime
  val formattedDate: String = currentDateTime.format(formatter)

  // Extract information from the current date and time
  lazy val currentYear: Int = currentDateTime.getYear
  lazy val currentMonth: String = currentDateTime.format(DateTimeFormatter.ofPattern("MM"))
  lazy val currentDate: String = currentDateTime.format(DateTimeFormatter.ofPattern("dd"))
  lazy val currentHour: String = currentDateTime.format(DateTimeFormatter.ofPattern("HH"))

  // Calculate information for the last hour
  lazy val lastHourDateTime: LocalDateTime = currentDateTime.minusHours(1)
  lazy val lastHourYear: Int = lastHourDateTime.getYear
  lazy val lastHourMonth: String = lastHourDateTime.format(DateTimeFormatter.ofPattern("MM"))
  lazy val lastHourDate: String = lastHourDateTime.format(DateTimeFormatter.ofPattern("dd"))
  lazy val lastHour: String = lastHourDateTime.format(DateTimeFormatter.ofPattern("HH"))

  lazy val yesterdayDate: String = currentDateTime.minusDays(1).format(DateTimeFormatter.ofPattern("dd"))
  lazy val yesterdayMonth: String = currentDateTime.minusDays(1).format(DateTimeFormatter.ofPattern("MM"))
  lazy val yesterdayYear: String = currentDateTime.minusDays(1).format(DateTimeFormatter.ofPattern("yyyy"))


  lazy val previousMonth: String = currentDateTime.minusMonths(1).format(DateTimeFormatter.ofPattern("MM"))
  lazy val previousMonthYear: String = currentDateTime.minusMonths(1).format(DateTimeFormatter.ofPattern("yyyy"))


  lazy val previousYear: String =  currentDateTime.minusYears(1).format(DateTimeFormatter.ofPattern("yyyy"))


}