//! Build an `aws_sdk_s3::Client` from our [`S3Config`].
//!
//! Credential precedence (handled here + by the SDK):
//! 1. Explicit `access_key_id` / `secret_access_key` in config / env vars
//! 2. Standard AWS SDK chain (~/.aws/credentials, instance metadata, etc.)

use aws_config::BehaviorVersion;
use aws_credential_types::Credentials;
use aws_sdk_s3::config::Builder as S3ConfigBuilder;
use aws_sdk_s3::config::{
    RequestChecksumCalculation, ResponseChecksumValidation, StalledStreamProtectionConfig,
};
use aws_sdk_s3::Client;
use warc_search_config::S3Config;

/// Build an S3 client from our config struct.
///
/// If explicit credentials are present they are loaded as static credentials.
/// Otherwise the standard AWS SDK credential chain is used.
pub async fn build_client(cfg: &S3Config) -> Client {
    let sdk_config = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_config::Region::new(cfg.region.clone()))
        .load()
        .await;

    // Since SDK 1.72 the default is `WhenSupported`, which wraps uploads in
    // `aws-chunked` framing with a trailing checksum.  Garage rejects those
    // requests with `InvalidRequest: Invalid payload signature`, so only send a
    // checksum where the API actually requires one.
    // Stalled-stream protection aborts a transfer whose throughput drops to
    // 0 B/s for a few seconds. Against a single-node Garage doing several
    // hundred-MB transfers at once, brief stalls are normal and this kills
    // otherwise healthy uploads and downloads
    // ("dispatch failure: timeout: minimum throughput was specified at 1 B/s").
    let mut builder = S3ConfigBuilder::from(&sdk_config)
        .request_checksum_calculation(RequestChecksumCalculation::WhenRequired)
        .response_checksum_validation(ResponseChecksumValidation::WhenRequired)
        .stalled_stream_protection(StalledStreamProtectionConfig::disabled());

    // Override credentials if explicitly provided
    if let (Some(key), Some(secret)) = (&cfg.access_key_id, &cfg.secret_access_key) {
        if !key.is_empty() && !secret.is_empty() {
            let creds = Credentials::new(
                key.clone(),
                secret.clone(),
                None, // session token
                None, // expiry
                "warc-search-config",
            );
            builder = builder.credentials_provider(creds);
        }
    }

    // Custom endpoint (MinIO, R2, B2, etc.)
    if let Some(endpoint) = &cfg.endpoint_url {
        builder = builder.endpoint_url(endpoint.clone());
    }

    if cfg.force_path_style {
        builder = builder.force_path_style(true);
    }

    Client::from_conf(builder.build())
}
