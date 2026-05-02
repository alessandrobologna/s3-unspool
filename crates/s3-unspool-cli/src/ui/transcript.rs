use super::Theme;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum TranscriptKind {
    Running,
    Success,
    Notice,
    Error,
}

#[derive(Debug, Eq, PartialEq)]
pub(crate) struct Transcript {
    pub(crate) kind: TranscriptKind,
    title: String,
    details: Vec<String>,
}

impl Transcript {
    pub(crate) fn running(title: impl Into<String>, details: Vec<String>) -> Self {
        Self {
            kind: TranscriptKind::Running,
            title: title.into(),
            details,
        }
    }

    pub(crate) fn success(title: impl Into<String>, details: Vec<String>) -> Self {
        Self {
            kind: TranscriptKind::Success,
            title: title.into(),
            details,
        }
    }

    pub(crate) fn notice(title: impl Into<String>, details: Vec<String>) -> Self {
        Self {
            kind: TranscriptKind::Notice,
            title: title.into(),
            details,
        }
    }

    pub(crate) fn error(title: impl Into<String>, details: Vec<String>) -> Self {
        Self {
            kind: TranscriptKind::Error,
            title: title.into(),
            details,
        }
    }

    pub(crate) fn render(&self, theme: Theme) -> String {
        let symbol = match self.kind {
            TranscriptKind::Running => theme.accent("•"),
            TranscriptKind::Success => theme.success("✓"),
            TranscriptKind::Notice => theme.accent("!"),
            TranscriptKind::Error => theme.error("×"),
        };
        let mut lines = vec![format!("{symbol} {}", self.title)];

        for (index, detail) in self.details.iter().enumerate() {
            let prefix = if index == 0 { "  └ " } else { "    " };
            lines.push(format!("{}{}", theme.muted_hint(prefix), detail));
        }

        lines.join("\n")
    }
}
