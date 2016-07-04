package com.spright.trek.exception;

import com.spright.trek.web.HttpStatusCode;

public class FileIOException extends IOExceptionWithErrorCode {

  public FileIOException(String message) {
    super(message);
  }

  public FileIOException(String message, final HttpStatusCode s) {
    super(message, s);
  }

  public FileIOException(String message, Throwable cause) {
    super(message, cause);
  }

  public FileIOException(String message, Throwable cause, final HttpStatusCode s) {
    super(message, cause, s);
  }
}
