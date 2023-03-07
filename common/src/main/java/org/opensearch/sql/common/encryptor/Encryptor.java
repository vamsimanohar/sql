/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */

package org.opensearch.sql.common.encryptor;

public interface Encryptor {

  String encrypt(String plainText);

  String decrypt(String encryptedText);

}