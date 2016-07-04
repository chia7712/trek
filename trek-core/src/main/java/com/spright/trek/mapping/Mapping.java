package com.spright.trek.mapping;

import com.spright.trek.exception.MappingIOException;
import com.spright.trek.query.CloseableIterator;
import java.io.IOException;
import java.util.Optional;

/**
 * Saves the {@link AccountInfo}. It is used for replaceing the alias in the
 */
public interface Mapping extends AutoCloseable {

  CloseableIterator<AccountInfo> list() throws MappingIOException, IOException;

  CloseableIterator<AccountInfo> list(final AccountInfoQuery query) throws MappingIOException, IOException;

  boolean supportOrder(final AccountInfoQuery query);

  boolean supportFilter(final AccountInfoQuery query);

  Optional<AccountInfo> find(final String alias) throws MappingIOException;

  AccountInfo add(final AccountInfoUpdate update) throws IOException;
}
