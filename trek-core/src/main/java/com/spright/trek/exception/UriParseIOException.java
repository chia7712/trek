package com.spright.trek.exception;

import com.spright.trek.web.HttpStatusCode;

public class UriParseIOException extends IOExceptionWithErrorCode {

  public UriParseIOException(String message) {
    super(message, HttpStatusCode.BAD_REQUEST);
  }

  public UriParseIOException(Throwable cause) {
    super(cause, HttpStatusCode.BAD_REQUEST);
  }

  public UriParseIOException(String message, final HttpStatusCode s) {
    super(message, s);
  }

  public UriParseIOException(String message, Throwable cause) {
    super(message, cause, HttpStatusCode.BAD_REQUEST);
  }

  public UriParseIOException(String message, Throwable cause,
          final HttpStatusCode status) {
    super(message, cause, status);
  }
}
