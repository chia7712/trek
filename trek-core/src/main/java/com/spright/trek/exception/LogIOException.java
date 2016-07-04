package com.spright.trek.exception;

import com.spright.trek.web.HttpStatusCode;

public class LogIOException extends IOExceptionWithErrorCode {

  public LogIOException(String message) {
    super(message);
  }

  public LogIOException(String message, final HttpStatusCode s) {
    super(message, s);
  }

  public LogIOException(String message, Throwable cause) {
    super(message, cause);
  }

  public LogIOException(String message, Throwable cause, final HttpStatusCode s) {
    super(message, cause, s);
  }
}
