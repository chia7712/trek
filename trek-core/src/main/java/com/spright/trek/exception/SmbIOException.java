package com.spright.trek.exception;

import com.spright.trek.web.HttpStatusCode;

public class SmbIOException extends IOExceptionWithErrorCode {

  public SmbIOException(String message) {
    super(message);
  }

  public SmbIOException(String message, final HttpStatusCode s) {
    super(message, s);
  }

  public SmbIOException(String message, Throwable cause) {
    super(message, cause);
  }

  public SmbIOException(String message, Throwable cause, final HttpStatusCode s) {
    super(message, cause, s);
  }
}
