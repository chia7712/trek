package com.spright.trek.task;

import com.spright.trek.datasystem.InputChannel;
import com.spright.trek.datasystem.OutputChannel;
import com.spright.trek.utils.BaseBuilder;

public final class AccessTaskRequest {

  public static Builder newBuilder() {
    return new AccessTaskRequest.Builder();
  }
  private final InputChannel input;
  private final OutputChannel output;
  private final String serverName;
  private final String clientName;
  private final String redirectFrom;

  public AccessTaskRequest(final InputChannel input, final OutputChannel output,
          final String serverName, final String clientName, final String redirectFrom) {
    this.input = input;
    this.output = output;
    this.serverName = serverName;
    this.clientName = clientName;
    this.redirectFrom = redirectFrom == null ? serverName : redirectFrom;
  }

  public InputChannel getInput() {
    return input;
  }

  public OutputChannel getOutput() {
    return output;
  }

  public String getServerName() {
    return serverName;
  }

  public String getClientName() {
    return clientName;
  }

  public String getRedirectFrom() {
    return redirectFrom;
  }

  public static class Builder extends BaseBuilder {

    private InputChannel input;
    private OutputChannel output;
    private String serverName;
    private String clientName;
    private String redirectFrom;

    private Builder() {
    }

    public Builder setInput(final InputChannel v) {
      if (isValid(v)) {
        input = v;
      }
      return this;
    }

    public Builder setOutput(final OutputChannel v) {
      if (isValid(v)) {
        output = v;
      }
      return this;
    }

    public Builder setServerName(final String v) {
      if (isValid(v)) {
        serverName = v;
      }
      return this;
    }

    public Builder setClientName(final String v) {
      if (isValid(v)) {
        clientName = v;
      }
      return this;
    }

    public Builder setRedirectFrom(final String v) {
      if (isValid(v)) {
        redirectFrom = v;
      }
      return this;
    }

    public AccessTaskRequest build() {
      return new AccessTaskRequest(input, output, serverName,
              clientName, redirectFrom);
    }
  }
}
