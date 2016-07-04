package com.spright.trek.exception;

import com.spright.trek.web.HttpStatusCode;

public class HdfsIOException extends IOExceptionWithErrorCode {

  public HdfsIOException(String message) {
    super(message);
  }

  public HdfsIOException(String message, final HttpStatusCode s) {
    super(message, s);
  }

  public HdfsIOException(String message, Throwable cause) {
    super(message, cause);
  }

  public HdfsIOException(String message, Throwable cause, final HttpStatusCode s) {
    super(message, cause, s);
  }
}
