use s3_unspool as unspool;

use super::activity::ActivityProgress;
use super::{Theme, format_bytes};

pub(crate) fn upload_progress_state(progress: &unspool::UploadProgress) -> ActivityProgress {
    match progress {
        unspool::UploadProgress::Planned {
            total_files,
            total_bytes,
        } => ActivityProgress {
            processed_files: 0,
            total_files: *total_files,
            current_file: None,
            processed_bytes: 0,
            total_bytes: *total_bytes,
        },
        unspool::UploadProgress::FileStarted {
            current_file,
            total_files,
            processed_files,
            processed_bytes,
            total_bytes,
            ..
        } => ActivityProgress {
            processed_files: *processed_files,
            total_files: *total_files,
            current_file: Some(*current_file),
            processed_bytes: *processed_bytes,
            total_bytes: *total_bytes,
        },
        unspool::UploadProgress::FileProgress {
            current_file,
            total_files,
            processed_files,
            processed_bytes,
            total_bytes,
            ..
        } => ActivityProgress {
            processed_files: *processed_files,
            total_files: *total_files,
            current_file: Some(*current_file),
            processed_bytes: *processed_bytes,
            total_bytes: *total_bytes,
        },
        unspool::UploadProgress::FileFinished {
            processed_files,
            total_files,
            processed_bytes,
            total_bytes,
            ..
        } => ActivityProgress {
            processed_files: *processed_files,
            total_files: *total_files,
            current_file: None,
            processed_bytes: *processed_bytes,
            total_bytes: *total_bytes,
        },
        unspool::UploadProgress::Finished {
            total_files,
            total_bytes,
        } => ActivityProgress {
            processed_files: *total_files,
            total_files: *total_files,
            current_file: None,
            processed_bytes: *total_bytes,
            total_bytes: *total_bytes,
        },
    }
}

pub(super) fn render_progress(
    theme: Theme,
    progress: &ActivityProgress,
    available_width: usize,
    animation_frame: usize,
) -> String {
    let percent = progress_percent(progress);
    let bytes = byte_progress(progress.processed_bytes, progress.total_bytes);
    let files = progress_file_label(progress);
    let metadata = format!("{percent} {bytes} {files}");

    let max_bar_inner_width = available_width.saturating_sub(metadata.len() + 3);
    if max_bar_inner_width < 6 {
        let compact = format!("{percent} {files}");
        if available_width < compact.len() {
            return percent;
        }
        return compact;
    }

    let bar_inner_width = max_bar_inner_width.min(18);
    format!(
        "{} {metadata}",
        progress_bar(theme, progress, bar_inner_width, animation_frame)
    )
}

fn progress_bar(
    theme: Theme,
    progress: &ActivityProgress,
    inner_width: usize,
    animation_frame: usize,
) -> String {
    format!(
        "[{}]",
        progress_bar_cells(
            theme,
            progress_filled_eighths(progress, inner_width),
            inner_width,
            animation_frame,
        )
    )
}

fn progress_bar_cells(
    theme: Theme,
    filled_eighths: usize,
    inner_width: usize,
    animation_frame: usize,
) -> String {
    let mut cells = String::new();
    for index in 0..inner_width {
        let cell_eighths = filled_eighths.saturating_sub(index * 8).min(8);
        let block = progress_block(cell_eighths).to_string();
        if cell_eighths == 0 {
            cells.push_str(&theme.muted_hint(&block));
        } else {
            cells.push_str(&theme.brightness(shimmer_brightness(index, animation_frame), &block));
        }
    }
    cells
}

fn progress_block(eighths: usize) -> char {
    match eighths.min(8) {
        0 => ' ',
        1 => '▏',
        2 => '▎',
        3 => '▍',
        4 => '▌',
        5 => '▋',
        6 => '▊',
        7 => '▉',
        _ => '█',
    }
}

fn shimmer_brightness(index: usize, animation_frame: usize) -> f64 {
    let phase = ((index as f64 - animation_frame as f64 * 0.5) / 6.0) * std::f64::consts::TAU;
    0.5 + 0.5 * phase.cos()
}

fn progress_filled_eighths(progress: &ActivityProgress, width: usize) -> usize {
    if width == 0 {
        return 0;
    }
    if progress.total_bytes == 0 {
        return if progress.processed_files >= progress.total_files {
            width * 8
        } else {
            0
        };
    }

    let processed = progress.processed_bytes.min(progress.total_bytes) as u128;
    let total = progress.total_bytes as u128;
    ((processed * (width * 8) as u128) / total) as usize
}

fn progress_percent(progress: &ActivityProgress) -> String {
    let percent = if progress.total_bytes == 0 {
        if progress.processed_files >= progress.total_files {
            100
        } else {
            0
        }
    } else {
        let processed = progress.processed_bytes.min(progress.total_bytes) as u128;
        let total = progress.total_bytes as u128;
        ((processed * 100) / total) as u64
    };
    format!("{percent}%")
}

fn progress_file_label(progress: &ActivityProgress) -> String {
    if progress.total_files == 0 {
        return "0 files".to_string();
    }
    if let Some(current_file) = progress.current_file {
        return format!("file {current_file}/{}", progress.total_files);
    }
    file_progress(progress.processed_files, progress.total_files)
}

fn file_progress(processed_files: usize, total_files: usize) -> String {
    if total_files == 0 {
        "0 files".to_string()
    } else {
        format!("{processed_files}/{total_files} files")
    }
}

fn byte_progress(processed_bytes: u64, total_bytes: u64) -> String {
    if total_bytes == 0 {
        return format_bytes(processed_bytes);
    }

    format!(
        "{}/{}",
        format_bytes(processed_bytes),
        format_bytes(total_bytes)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn renders_upload_progress_bar() {
        assert_eq!(
            render_progress(
                Theme { color: false },
                &upload_progress_state(&unspool::UploadProgress::Planned {
                    total_files: 10,
                    total_bytes: 100,
                }),
                80,
                0,
            ),
            "[                  ] 0% 0 B/100 B 0/10 files"
        );
        assert_eq!(
            render_progress(
                Theme { color: false },
                &upload_progress_state(&unspool::UploadProgress::FileFinished {
                    processed_files: 3,
                    total_files: 10,
                    processed_bytes: 30,
                    total_bytes: 100,
                    path: "a.txt".to_string(),
                }),
                80,
                0,
            ),
            "[█████▍            ] 30% 30 B/100 B 3/10 files"
        );
        assert_eq!(
            render_progress(
                Theme { color: false },
                &upload_progress_state(&unspool::UploadProgress::FileStarted {
                    current_file: 4,
                    total_files: 10,
                    processed_files: 3,
                    processed_bytes: 30,
                    total_bytes: 100,
                    path: "b.txt".to_string(),
                }),
                80,
                0,
            ),
            "[█████▍            ] 30% 30 B/100 B file 4/10"
        );
        assert_eq!(
            render_progress(
                Theme { color: false },
                &upload_progress_state(&unspool::UploadProgress::FileProgress {
                    current_file: 4,
                    total_files: 10,
                    processed_files: 3,
                    processed_bytes: 40,
                    total_bytes: 100,
                    path: "b.txt".to_string(),
                }),
                80,
                0,
            ),
            "[███████▏          ] 40% 40 B/100 B file 4/10"
        );
        assert_eq!(
            render_progress(
                Theme { color: false },
                &upload_progress_state(&unspool::UploadProgress::Finished {
                    total_files: 10,
                    total_bytes: 100,
                }),
                80,
                0,
            ),
            "[██████████████████] 100% 100 B/100 B 10/10 files"
        );
    }

    #[test]
    fn maps_upload_progress_events_to_activity_state() {
        assert_eq!(
            upload_progress_state(&unspool::UploadProgress::Planned {
                total_files: 10,
                total_bytes: 100,
            }),
            ActivityProgress {
                processed_files: 0,
                total_files: 10,
                current_file: None,
                processed_bytes: 0,
                total_bytes: 100,
            }
        );
        assert_eq!(
            upload_progress_state(&unspool::UploadProgress::FileFinished {
                processed_files: 3,
                total_files: 10,
                processed_bytes: 30,
                total_bytes: 100,
                path: "a.txt".to_string(),
            }),
            ActivityProgress {
                processed_files: 3,
                total_files: 10,
                current_file: None,
                processed_bytes: 30,
                total_bytes: 100,
            }
        );
        assert_eq!(
            upload_progress_state(&unspool::UploadProgress::FileStarted {
                current_file: 4,
                total_files: 10,
                processed_files: 3,
                processed_bytes: 30,
                total_bytes: 100,
                path: "b.txt".to_string(),
            }),
            ActivityProgress {
                processed_files: 3,
                total_files: 10,
                current_file: Some(4),
                processed_bytes: 30,
                total_bytes: 100,
            }
        );
        assert_eq!(
            upload_progress_state(&unspool::UploadProgress::Finished {
                total_files: 10,
                total_bytes: 100,
            }),
            ActivityProgress {
                processed_files: 10,
                total_files: 10,
                current_file: None,
                processed_bytes: 100,
                total_bytes: 100,
            }
        );
    }

    #[test]
    fn calculates_progress_bar_fill_from_bytes() {
        let progress = ActivityProgress {
            processed_files: 3,
            total_files: 10,
            current_file: Some(4),
            processed_bytes: 40,
            total_bytes: 100,
        };

        assert_eq!(progress_filled_eighths(&progress, 18), 57);
        assert_eq!(
            progress_bar_cells(Theme { color: false }, 57, 18, 0),
            "███████▏          "
        );
        assert_eq!(progress_block(1), '▏');
        assert_eq!(progress_block(4), '▌');
        assert_eq!(progress_block(7), '▉');
        assert_eq!(progress_block(8), '█');
    }

    #[test]
    fn calculates_cosine_shimmer_brightness() {
        assert!((shimmer_brightness(0, 0) - 1.0).abs() < f64::EPSILON);
        assert!((shimmer_brightness(0, 6) - 0.0).abs() < f64::EPSILON);
    }

    #[test]
    fn renders_compact_progress_when_width_is_tight() {
        assert_eq!(
            render_progress(
                Theme { color: false },
                &upload_progress_state(&unspool::UploadProgress::FileProgress {
                    current_file: 4,
                    total_files: 10,
                    processed_files: 3,
                    processed_bytes: 40,
                    total_bytes: 100,
                    path: "b.txt".to_string(),
                }),
                18,
                0,
            ),
            "40% file 4/10"
        );
        assert_eq!(
            render_progress(
                Theme { color: false },
                &upload_progress_state(&unspool::UploadProgress::FileProgress {
                    current_file: 4,
                    total_files: 10,
                    processed_files: 3,
                    processed_bytes: 40,
                    total_bytes: 100,
                    path: "b.txt".to_string(),
                }),
                4,
                0,
            ),
            "40%"
        );
    }
}
