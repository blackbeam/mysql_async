/// Compile-time tracing level.
pub trait TracingLevel: Send + Sync + 'static {
    #[cfg(feature = "tracing")]
    const LEVEL: tracing::Level;
}

/// INFO tracing level.
pub struct LevelInfo;

impl TracingLevel for LevelInfo {
    #[cfg(feature = "tracing")]
    const LEVEL: tracing::Level = tracing::Level::INFO;
}

/// TRACE tracing level.
pub struct LevelTrace;

impl TracingLevel for LevelTrace {
    #[cfg(feature = "tracing")]
    const LEVEL: tracing::Level = tracing::Level::TRACE;
}

#[cfg(feature = "tracing")]
macro_rules! create_span {
    ($s:expr, $($field:tt)*) => {
        if $s == tracing::Level::TRACE {
            tracing::trace_span!($($field)*)
        } else if $s == tracing::Level::DEBUG {
            tracing::debug_span!($($field)*)
        } else if $s == tracing::Level::INFO {
            tracing::info_span!($($field)*)
        } else if $s == tracing::Level::WARN {
            tracing::warn_span!($($field)*)
        } else if $s == tracing::Level::ERROR {
            tracing::error_span!($($field)*)
        } else {
            unreachable!();
        }
    }
}

#[cfg(feature = "tracing")]
macro_rules! instrument_result {
    ($fut:expr, $span:expr) => {{
        let fut = async {
            $fut.await.or_else(|e| {
                tracing::error!(error = %e);
                Err(e)
            })
        };
        <_ as tracing::Instrument>::instrument(fut, $span)
    }};
}
