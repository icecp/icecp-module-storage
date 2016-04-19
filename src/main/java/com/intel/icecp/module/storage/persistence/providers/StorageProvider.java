package com.intel.icecp.module.storage.persistence.providers;

/**
 * Interface for common storage provider which encapsulates specific versions of provider
 *
 */
public interface StorageProvider extends LegacyStorageProvider, TaggedStorageProvider {

}
