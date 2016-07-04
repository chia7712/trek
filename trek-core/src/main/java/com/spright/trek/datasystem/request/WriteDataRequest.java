package com.spright.trek.datasystem.request;

import com.spright.trek.exception.MappingIOException;
import com.spright.trek.exception.UriParseIOException;
import com.spright.trek.mapping.Mapping;
import com.spright.trek.query.QueryUtils;
import com.spright.trek.DConstants;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Optional;

public class WriteDataRequest extends UriRequest {

  public static WriteDataRequest parse(final Map<String, String> rawQuery,
          final Mapping mapping, final DataInfo refInfo) throws UriParseIOException, MappingIOException {
    SimpleDateFormat timeSdf = new SimpleDateFormat(DConstants.DEFAULT_TIME_FORMAT);
    if (refInfo == null) {
      return new WriteDataRequest(
              UriRequest.parseTo(rawQuery, mapping, null),
              QueryUtils.parsePositiveTime(timeSdf, rawQuery.get(DConstants.URI_DATA_UPLOAD_TIME), -1),
              -1);
    } else {
      return new WriteDataRequest(
              UriRequest.parseTo(rawQuery, mapping, refInfo.getUriRequest().getPath().getName()),
              QueryUtils.parsePositiveTime(timeSdf, rawQuery.get(DConstants.URI_DATA_UPLOAD_TIME), -1),
              refInfo.getSize());
    }
  }
  private final long uploadTime;
  private final long size;

  public WriteDataRequest(final UriRequest request) {
    this(request, -1, -1);
  }

  public WriteDataRequest(final UriRequest request, final long uploadTime, final long size) {
    super(request);
    this.uploadTime = uploadTime;
    this.size = size;
  }

  public final Optional<Long> getExpectedSize() {
    return size <= 0 ? Optional.empty() : Optional.of(size);
  }

  public final Optional<Long> getExpectedTime() {
    return uploadTime <= 0 ? Optional.empty() : Optional.of(uploadTime);
  }

  @Override
  public String toString() {
    return super.toString();
  }
}
