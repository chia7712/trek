package com.spright.trek.task;

public interface AccessTask {

  AccessTaskRequest getRequest();

  AccessStatus getStatus();

  void abort() throws Exception;

  void waitCompletion() throws Exception;
}
