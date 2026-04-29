use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::constants::{EMBEDDED_CATALOG_PATH, EMBEDDED_CATALOG_VERSION};
use crate::s3_uri::normalize_etag;
use crate::zip_manifest::normalize_zip_file_path;

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct EmbeddedCatalog {
    pub(crate) version: u32,
    pub(crate) entries: Vec<EmbeddedCatalogEntry>,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub(crate) struct EmbeddedCatalogEntry {
    pub(crate) path: String,
    pub(crate) md5: String,
}

pub(crate) fn catalog_md5_by_path(catalog: EmbeddedCatalog) -> HashMap<String, String> {
    if catalog.version != EMBEDDED_CATALOG_VERSION {
        return HashMap::new();
    }

    let mut result = HashMap::new();
    for entry in catalog.entries {
        let Ok(Some(path)) = normalize_zip_file_path(&entry.path) else {
            continue;
        };
        if path == EMBEDDED_CATALOG_PATH {
            continue;
        }
        let Some(md5) = normalize_etag(&entry.md5) else {
            continue;
        };
        result.insert(path, md5);
    }
    result
}
