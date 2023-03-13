package org.opensearch.sql.plugin.datasource;

import static org.opensearch.commons.ConfigConstants.OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT;
import static org.opensearch.sql.analysis.DataSourceSchemaIdentifierNameResolver.DEFAULT_DATASOURCE_NAME;

import lombok.AllArgsConstructor;
import org.opensearch.client.Client;
import org.opensearch.commons.authuser.User;
import org.opensearch.sql.datasource.DataSourceAuthorizer;
import org.opensearch.sql.datasource.model.DataSourceMetadata;


@AllArgsConstructor
public class DataSourceAuthorizerImpl implements DataSourceAuthorizer {

  private final Client client;

  @Override
  public void authorize(DataSourceMetadata dataSourceMetadata) {
    String userString = client.threadPool()
        .getThreadContext().getTransient(OPENSEARCH_SECURITY_USER_INFO_THREAD_CONTEXT);
    if (userString != null && !dataSourceMetadata.getName().equals(DEFAULT_DATASOURCE_NAME)) {
      User user = User.parse(userString);
      if (!isAdminUser(user)) {
        for (String role : user.getRoles()) {
          if (dataSourceMetadata.getAllowedRoles().contains(role)) {
            return;
          }
        }
      }
    }


  }

  private Boolean isAdminUser(User user) {
    return user.getRoles().contains("all_access");
  }

}
