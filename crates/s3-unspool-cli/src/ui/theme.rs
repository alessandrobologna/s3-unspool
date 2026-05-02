use std::env;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum ColorMode {
    Auto,
    Always,
    Never,
}

#[derive(Clone, Copy, Debug)]
pub(crate) struct Theme {
    pub(crate) color: bool,
}

impl Theme {
    pub(crate) fn from_mode(mode: ColorMode, stderr_is_terminal: bool) -> Self {
        let color = match mode {
            ColorMode::Auto => stderr_is_terminal && env::var_os("NO_COLOR").is_none(),
            ColorMode::Always => true,
            ColorMode::Never => false,
        };
        Self { color }
    }

    pub(crate) fn dim(self, value: &str) -> String {
        self.wrap("2", value)
    }

    pub(crate) fn muted_hint(self, value: &str) -> String {
        self.wrap("90", value)
    }

    pub(crate) fn brightness(self, level: f64, value: &str) -> String {
        if !self.color {
            return value.to_string();
        }

        let channel = (150.0 + 85.0 * level.clamp(0.0, 1.0)).round() as u8;
        format!("\x1b[38;2;{channel};{channel};{channel}m{value}\x1b[0m")
    }

    pub(crate) fn accent(self, value: &str) -> String {
        self.wrap("36", value)
    }

    pub(crate) fn success(self, value: &str) -> String {
        self.wrap("32", value)
    }

    pub(crate) fn error(self, value: &str) -> String {
        self.wrap("31", value)
    }

    fn wrap(self, code: &str, value: &str) -> String {
        if self.color {
            format!("\x1b[{code}m{value}\x1b[0m")
        } else {
            value.to_string()
        }
    }
}
