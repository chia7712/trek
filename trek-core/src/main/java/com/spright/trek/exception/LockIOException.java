package com.spright.trek.exception;

import com.spright.trek.web.HttpStatusCode;

public class LockIOException extends IOExceptionWithErrorCode {

  public LockIOException(String message) {
    super(message);
  }

  public LockIOException(String message, final HttpStatusCode s) {
    super(message, s);
  }

  public LockIOException(String message, Throwable cause) {
    super(message, cause);
  }

  public LockIOException(String message, Throwable cause, final HttpStatusCode s) {
    super(message, cause, s);
  }
}
