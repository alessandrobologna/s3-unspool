use aws_config::BehaviorVersion;
use aws_sdk_s3::config::StalledStreamProtectionConfig;

pub(crate) async fn s3_client() -> aws_sdk_s3::Client {
    let config = aws_config::load_defaults(BehaviorVersion::latest()).await;
    aws_sdk_s3::Client::from_conf(
        aws_sdk_s3::config::Builder::from(&config)
            .stalled_stream_protection(
                StalledStreamProtectionConfig::enabled()
                    .upload_enabled(false)
                    .download_enabled(true)
                    .build(),
            )
            .build(),
    )
}
