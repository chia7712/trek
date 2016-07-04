package com.spright.trek.exception;

import com.spright.trek.web.HttpStatusCode;

public class TaskIOException extends IOExceptionWithErrorCode {

  public TaskIOException(String message) {
    super(message);
  }

  public TaskIOException(String message, final HttpStatusCode s) {
    super(message, s);
  }

  public TaskIOException(String message, Throwable cause) {
    super(message, cause);
  }

  public TaskIOException(String message, Throwable cause, final HttpStatusCode s) {
    super(message, cause, s);
  }
}
