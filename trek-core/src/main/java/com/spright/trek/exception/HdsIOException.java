package com.spright.trek.exception;

import com.spright.trek.web.HttpStatusCode;

public class HdsIOException extends IOExceptionWithErrorCode {

  public HdsIOException(String message) {
    super(message);
  }

  public HdsIOException(String message, final HttpStatusCode s) {
    super(message, s);
  }

  public HdsIOException(String message, Throwable cause) {
    super(message, cause);
  }

  public HdsIOException(String message, Throwable cause, final HttpStatusCode s) {
    super(message, cause, s);
  }
}
