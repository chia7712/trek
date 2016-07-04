package com.spright.trek.exception;

import com.spright.trek.web.HttpStatusCode;

public class ResourceNotFoundException extends IOExceptionWithErrorCode {

  public ResourceNotFoundException(String message) {
    super(message, HttpStatusCode.RESOURCE_NOT_FOUND);
  }

  public ResourceNotFoundException(String message, Throwable cause) {
    super(message, cause, HttpStatusCode.RESOURCE_NOT_FOUND);
  }
}
