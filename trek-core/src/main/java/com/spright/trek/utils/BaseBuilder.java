package com.spright.trek.utils;

import java.util.List;
import java.util.Map;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public abstract class BaseBuilder {

  protected static final Log LOG = LogFactory.getLog(BaseBuilder.class);

  protected static boolean isValid(final double v) {
    return isValid(new Double(v));
  }

  protected static boolean isValid(final double v, final double limit) {
    return isValid(new Double(v)) && v <= limit;
  }

  protected static boolean isValid(final long v) {
    return isValid(new Long(v));
  }

  protected static boolean isValid(final int v) {
    return isValid(new Integer(v));
  }

  protected static boolean isValid(final Object v) {
    if (v != null) {
      if (v instanceof String) {
        String s = (String) v;
        if (s.length() != 0) {
          return true;
        }
      } else if (v instanceof Integer) {
        Integer s = (Integer) v;
        if (s >= 0) {
          return true;
        }
      } else if (v instanceof Long) {
        Long s = (Long) v;
        if (s >= 0) {
          return true;
        }
      } else if (v instanceof Double) {
        Double s = (Double) v;
        if (s >= 0) {
          return true;
        }
      } else if (v instanceof List) {
        List s = (List) v;
        if (s.size() > 0) {
          return true;
        }
      } else if (v instanceof Map) {
        Map s = (Map) v;
        if (s.size() > 0) {
          return true;
        }
      } else {
        return true;
      }
    }
    LOG.warn("Add a invalid value : " + v + " to builder");
    return false;
  }

  private static void throwError(final Object obj, final String name) {
    String msg = "The " + name + " has invalid value:" + obj;
    LOG.error(msg);
    throw new IllegalArgumentException(msg);
  }

  protected static void checkNull(final double obj, final String name, final double limit) {
    if (!isValid(obj, limit)) {
      throwError(obj, name);
    }
  }

  protected static void checkNull(final Object obj, final String name) {
    if (!isValid(obj)) {
      throwError(obj, name);
    }
  }
}
