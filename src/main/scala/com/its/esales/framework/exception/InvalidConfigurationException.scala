package com.its.esales.framework.exception

final case class InvalidConfigurationException(private val message: String = "",
                                               private val cause: Throwable =
                                               None.orNull)
  extends Exception(message, cause)
