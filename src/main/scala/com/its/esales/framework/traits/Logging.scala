package com.its.esales.framework.traits

import org.apache.log4j.{Level, Logger}
import org.apache.log4j.PropertyConfigurator

trait Logging {
  PropertyConfigurator.configure(getClass.getClassLoader.getResource("log4j.properties"))
  Logger.getLogger("org").setLevel(Level.INFO)
  protected val logger: Logger = Logger.getLogger(getClass.getSimpleName)


}
