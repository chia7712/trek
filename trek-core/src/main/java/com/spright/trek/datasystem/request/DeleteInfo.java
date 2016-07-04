package com.spright.trek.datasystem.request;

public final class DeleteInfo extends DataInfo {

  private final String errorMsg;

  public DeleteInfo(final DataInfo info, final String errorMsg) {
    super(info);
    this.errorMsg = errorMsg;
  }

  public String getErrorMessage() {
    return errorMsg;
  }
}
