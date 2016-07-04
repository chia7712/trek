package com.spright.trek.exception;

import com.spright.trek.web.HttpStatusCode;

public class MappingIOException extends IOExceptionWithErrorCode {

  public MappingIOException(String message) {
    super(message);
  }

  public MappingIOException(String message, final HttpStatusCode s) {
    super(message, s);
  }

  public MappingIOException(String message, Throwable cause) {
    super(message, cause);
  }

  public MappingIOException(String message, Throwable cause, final HttpStatusCode s) {
    super(message, cause, s);
  }
}
