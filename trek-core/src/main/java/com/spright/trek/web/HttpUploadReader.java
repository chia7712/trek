package com.spright.trek.web;

import com.spright.trek.datasystem.InputChannel;
import java.util.List;
import java.util.Map;

public interface HttpUploadReader extends InputChannel {

  Map<String, List<String>> getHeader();
}
