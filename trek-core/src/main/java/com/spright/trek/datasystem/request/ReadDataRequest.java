package com.spright.trek.datasystem.request;

import com.spright.trek.exception.MappingIOException;
import com.spright.trek.exception.UriParseIOException;
import com.spright.trek.mapping.Mapping;
import com.spright.trek.query.QueryUtils;
import com.spright.trek.DConstants;
import java.util.Map;

public class ReadDataRequest extends UriRequest {

  public static ReadDataRequest parse(final Map<String, String> rawQuery,
          final Mapping mapping) throws UriParseIOException, MappingIOException {
    return new ReadDataRequest(UriRequest.parseFrom(rawQuery, mapping, null),
            QueryUtils.parseBoolean(rawQuery.get(DConstants.URI_DATA_KEEP),
                    DConstants.DEFAULT_URI_DATA_KEEP));
  }
  private final boolean keep;

  public ReadDataRequest(final UriRequest request, final boolean keep) {
    super(request);
    this.keep = keep;
  }

  public final boolean isKeep() {
    return keep;
  }

  @Override
  public String toString() {
    return super.toString();
  }
}
