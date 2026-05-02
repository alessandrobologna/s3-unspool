#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum ReportDestination {
    None,
    Human,
    JsonFile(String),
}

impl ReportDestination {
    pub(crate) fn from_cli_value(value: Option<&String>) -> Self {
        match value.map(String::as_str) {
            None => Self::None,
            Some("-") => Self::Human,
            Some(path) => Self::JsonFile(path.to_string()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn maps_report_destination_cli_values() {
        assert_eq!(
            ReportDestination::from_cli_value(None),
            ReportDestination::None
        );
        assert_eq!(
            ReportDestination::from_cli_value(Some(&"-".to_string())),
            ReportDestination::Human
        );
        assert_eq!(
            ReportDestination::from_cli_value(Some(&"report.json".to_string())),
            ReportDestination::JsonFile("report.json".to_string())
        );
    }
}
