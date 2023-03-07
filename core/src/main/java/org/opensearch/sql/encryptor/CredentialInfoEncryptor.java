/*
 *
 *  * Copyright OpenSearch Contributors
 *  * SPDX-License-Identifier: Apache-2.0
 *
 */
package org.opensearch.sql.encryptor;

public interface CredentialInfoEncryptor {

  String encrypt(String plainText);

  String decrypt(String encryptedText);

}