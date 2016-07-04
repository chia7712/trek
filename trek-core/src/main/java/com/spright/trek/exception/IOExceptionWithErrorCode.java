package com.spright.trek.exception;

import com.spright.trek.web.HttpStatusCode;
import java.io.IOException;

public class IOExceptionWithErrorCode extends IOException {

  private final HttpStatusCode status;

  /**
   * Constructs an {@code UriParseException} with the specified detail message.
   *
   * @param message The detail message (which is saved for later retrieval by
   * the {@link #getMessage()} method)
   */
  public IOExceptionWithErrorCode(String message) {
    this(message, HttpStatusCode.INTERNAL_SERVER_ERROR);
  }

  /**
   * Constructs an {@code IOException} with the specified cause and a detail
   * message of {@code (cause==null ? null : cause.toString())} (which typically
   * contains the class and detail message of {@code cause}). This constructor
   * is useful for IO exceptions that are little more than wrappers for other
   * throwables.
   *
   * @param cause The cause (which is saved for later retrieval by the
   * {@link #getCause()} method). (A null value is permitted, and indicates that
   * the cause is nonexistent or unknown.)
   *
   */
  public IOExceptionWithErrorCode(Throwable cause) {
    super(cause);
    status = HttpStatusCode.INTERNAL_SERVER_ERROR;
  }

  public IOExceptionWithErrorCode(Throwable cause, final HttpStatusCode pStatus) {
    super(cause);
    status = pStatus;
  }

  public IOExceptionWithErrorCode(String message, final HttpStatusCode pStatus) {
    super(message);
    status = pStatus;
  }

  /**
   * Constructs an {@code UriParseException} with the specified detail message
   * and cause.
   *
   * <p>
   * Note that the detail message associated with {@code cause} is
   * <i>not</i> automatically incorporated into this exception's detail message.
   *
   * @param message The detail message (which is saved for later retrieval by
   * the {@link #getMessage()} method)
   *
   * @param cause The cause (which is saved for later retrieval by the
   * {@link #getCause()} method). (A null value is permitted, and indicates that
   * the cause is nonexistent or unknown.)
   *
   * @since 1.6
   */
  public IOExceptionWithErrorCode(String message, Throwable cause) {
    this(message, cause, HttpStatusCode.BAD_REQUEST);
  }

  public IOExceptionWithErrorCode(String message, Throwable cause, final HttpStatusCode s) {
    super(message, cause);
    status = s;
  }

  public HttpStatusCode getHttpStatus() {
    return status;
  }
}
