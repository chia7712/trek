package com.spright.trek.datasystem;

import com.spright.trek.datasystem.request.WriteDataRequest;
import java.io.IOException;
import java.io.OutputStream;

public interface OutputChannel extends AutoCloseable {

  WriteDataRequest getRequest();

  OutputStream getOutputStream();

  void recover() throws IOException;
}
