package com.spright.trek.datasystem;

import com.spright.trek.datasystem.request.DataInfo;
import java.io.InputStream;

public interface InputChannel extends AutoCloseable {

  DataInfo getInfo();

  InputStream getInputStream();
}
