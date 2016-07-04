package com.spright.trek.exception;

import com.spright.trek.web.HttpStatusCode;

public class HttpIOException extends IOExceptionWithErrorCode {

  public HttpIOException(String message) {
    super(message);
  }

  public HttpIOException(String message, final HttpStatusCode s) {
    super(message, s);
  }

  public HttpIOException(String message, Throwable cause) {
    super(message, cause);
  }

  public HttpIOException(String message, Throwable cause, final HttpStatusCode s) {
    super(message, cause, s);
  }
}
