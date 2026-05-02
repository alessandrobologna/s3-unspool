use std::time::Duration;

pub(crate) fn plural(count: usize, singular: &str, plural: &str) -> String {
    if count == 1 {
        format!("1 {singular}")
    } else {
        format!("{count} {plural}")
    }
}

pub(crate) fn truncate_text(value: &str, max_chars: usize) -> String {
    if value.chars().count() <= max_chars {
        return value.to_string();
    }

    let keep = max_chars.saturating_sub(3);
    format!("{}...", value.chars().take(keep).collect::<String>())
}

pub(crate) fn format_elapsed(elapsed: Duration) -> String {
    let total_seconds = elapsed.as_secs();
    let minutes = total_seconds / 60;
    let seconds = total_seconds % 60;
    format!("{minutes:02}:{seconds:02}")
}

pub(crate) fn format_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KiB", "MiB", "GiB", "TiB"];
    if bytes < 1024 {
        return format!("{bytes} B");
    }

    let mut value = bytes as f64;
    let mut unit = 0;
    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }

    if value >= 10.0 {
        format!("{value:.0} {}", UNITS[unit])
    } else {
        format!("{value:.1} {}", UNITS[unit])
    }
}

pub(crate) fn format_upload_speed(bytes: u64, elapsed: Duration) -> String {
    let seconds = elapsed.as_secs_f64();
    let mib_per_second = if seconds > 0.0 {
        bytes as f64 / 1024.0 / 1024.0 / seconds
    } else {
        0.0
    };
    format!("{mib_per_second:.2} MiB/s")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn formats_bytes_for_summary_rows() {
        assert_eq!(format_bytes(0), "0 B");
        assert_eq!(format_bytes(768), "768 B");
        assert_eq!(format_bytes(1536), "1.5 KiB");
        assert_eq!(format_bytes(10 * 1024), "10 KiB");
        assert_eq!(format_bytes(2 * 1024 * 1024), "2.0 MiB");
    }

    #[test]
    fn formats_upload_speed_with_two_decimal_places() {
        assert_eq!(
            format_upload_speed(3 * 1024 * 1024, Duration::from_secs(2)),
            "1.50 MiB/s"
        );
        assert_eq!(
            format_upload_speed(1024 * 1024, Duration::from_secs(0)),
            "0.00 MiB/s"
        );
    }

    #[test]
    fn formats_elapsed_as_stable_minutes_and_seconds() {
        assert_eq!(format_elapsed(Duration::from_secs(0)), "00:00");
        assert_eq!(format_elapsed(Duration::from_secs(9)), "00:09");
        assert_eq!(format_elapsed(Duration::from_secs(75)), "01:15");
    }
}
