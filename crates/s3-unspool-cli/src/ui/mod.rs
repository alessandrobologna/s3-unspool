mod activity;
mod progress;
mod text;
mod theme;
mod transcript;

use std::io::{self, IsTerminal, Write};

use clap::ArgMatches;

pub(crate) use activity::{Activity, ActivityDetail};
pub(crate) use progress::upload_progress_state;
pub(crate) use text::{format_bytes, format_elapsed, format_upload_speed, plural, truncate_text};
pub(crate) use theme::{ColorMode, Theme};
pub(crate) use transcript::{Transcript, TranscriptKind};

#[derive(Debug)]
pub(crate) struct Output {
    quiet: bool,
    theme: Theme,
    interactive: bool,
}

impl Output {
    pub(crate) fn from_matches(matches: &ArgMatches) -> Self {
        let stderr_is_terminal = io::stderr().is_terminal();
        let quiet = matches.get_flag("quiet");
        let color = match matches
            .get_one::<String>("color")
            .map(String::as_str)
            .unwrap_or("auto")
        {
            "always" => ColorMode::Always,
            "never" => ColorMode::Never,
            _ => ColorMode::Auto,
        };
        Self {
            quiet,
            theme: Theme::from_mode(color, stderr_is_terminal),
            interactive: stderr_is_terminal,
        }
    }

    pub(crate) fn write(&self, transcript: &Transcript) -> io::Result<()> {
        if self.quiet && transcript.kind != TranscriptKind::Error {
            return Ok(());
        }
        let rendered = transcript.render(self.theme);
        writeln!(io::stderr().lock(), "{rendered}")
    }

    pub(crate) fn start_activity(
        &self,
        verb: &'static str,
        detail: Option<ActivityDetail>,
    ) -> Activity {
        if self.quiet || !self.interactive {
            return Activity::disabled();
        }

        Activity::start(verb, self.theme, detail)
    }
}
