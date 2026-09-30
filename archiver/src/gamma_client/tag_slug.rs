use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TagSlug {
    pub id: String,
    pub label: Option<String>,
    pub slug: Option<String>,
    pub force_show: Option<bool>,
    pub published_at: Option<String>,
    pub created_by: Option<i64>,
    pub updated_by: Option<i64>,
    pub created_at: Option<DateTime<Utc>>,
    pub updated_at: Option<DateTime<Utc>>,
    pub force_hide: Option<bool>,
    pub is_carousel: Option<bool>,
}
