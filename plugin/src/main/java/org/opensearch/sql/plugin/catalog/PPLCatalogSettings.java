package org.opensearch.sql.plugin.catalog;

import org.opensearch.common.settings.SecureSetting;
import org.opensearch.common.settings.Setting;

import java.io.InputStream;

public class PPLCatalogSettings {

    public static final Setting<Boolean> FEDERATION_ENABLED = Setting.boolSetting(
            "plugins.ppl.federation.enabled",
            false,
            Setting.Property.NodeScope,
            Setting.Property.Dynamic
    );

    public static final Setting<InputStream> CATALOG_CONFIG = SecureSetting.secureFile(
            "plugins.ppl.federation.catalog.config",
            null);
}
