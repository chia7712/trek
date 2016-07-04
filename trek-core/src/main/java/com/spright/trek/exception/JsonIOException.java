package com.spright.trek.exception;

import com.spright.trek.web.HttpStatusCode;

public class JsonIOException extends IOExceptionWithErrorCode {

  public JsonIOException(String message) {
    super(message);
  }

  public JsonIOException(String message, final HttpStatusCode s) {
    super(message, s);
  }

  public JsonIOException(String message, Throwable cause) {
    super(message, cause);
  }

  public JsonIOException(String message, Throwable cause, final HttpStatusCode s) {
    super(message, cause, s);
  }
}
