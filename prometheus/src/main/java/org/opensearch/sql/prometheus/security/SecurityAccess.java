/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.prometheus.security;

import java.io.IOException;
import java.security.AccessController;
import java.security.PrivilegedActionException;
import java.security.PrivilegedExceptionAction;
import org.opensearch.SpecialPermission;

/**
 * Ref:
 * https://www.elastic.co/guide/en/elasticsearch/plugins/current/plugin-authors.html#_java_security_permissions
 */
public class SecurityAccess {

  /**
   * Execute the operation in privileged mode.
   */
  public static <T> T doPrivileged(final PrivilegedExceptionAction<T> operation)
      throws IOException {
    SpecialPermission.check();
    try {
      return AccessController.doPrivileged(operation);
    } catch (final PrivilegedActionException e) {
      throw (IOException) e.getCause();
    }
  }
}
