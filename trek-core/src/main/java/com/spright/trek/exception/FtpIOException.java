package com.spright.trek.exception;

import com.spright.trek.web.HttpStatusCode;

public class FtpIOException extends IOExceptionWithErrorCode {

  public FtpIOException(String message) {
    super(message);
  }

  public FtpIOException(String message, final HttpStatusCode s) {
    super(message, s);
  }

  public FtpIOException(String message, Throwable cause) {
    super(message, cause);
  }

  public FtpIOException(String message, Throwable cause, final HttpStatusCode s) {
    super(message, cause, s);
  }
}
