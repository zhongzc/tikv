use crate::Either;
use crossbeam::channel::TrySendError;
use std::fmt;

pub struct Contextual<T> {
    // TODO: place trace context here
    pub _ctx: (),
    pub msg: T,
}

impl<T: fmt::Debug> fmt::Debug for Contextual<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.msg.fmt(f)
    }
}

impl<T: fmt::Display> fmt::Display for Contextual<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.msg.fmt(f)
    }
}

impl<T> From<T> for Contextual<T> {
    fn from(msg: T) -> Self {
        Self { _ctx: (), msg }
    }
}

pub trait UnwrapContext {
    type Target;
    fn unwrap_context(self) -> Self::Target;
}

impl<T> UnwrapContext for Contextual<T> {
    type Target = T;

    #[inline]
    fn unwrap_context(self) -> Self::Target {
        self.msg
    }
}

impl<T: UnwrapContext> UnwrapContext for TrySendError<T> {
    type Target = TrySendError<T::Target>;

    #[inline]
    fn unwrap_context(self) -> Self::Target {
        match self {
            TrySendError::Full(t) => TrySendError::Full(t.unwrap_context()),
            TrySendError::Disconnected(t) => TrySendError::Disconnected(t.unwrap_context()),
        }
    }
}

impl<T: UnwrapContext, U> UnwrapContext for Result<T, U> {
    type Target = Result<T::Target, U>;

    #[inline]
    fn unwrap_context(self) -> Result<T::Target, U> {
        self.map(|r| r.unwrap_context())
    }
}

impl<T: UnwrapContext> UnwrapContext for Result<(), T> {
    type Target = Result<(), T::Target>;

    #[inline]
    fn unwrap_context(self) -> Result<(), T::Target> {
        self.map_err(|e| e.unwrap_context())
    }
}

impl<T: UnwrapContext, U: UnwrapContext> UnwrapContext for Either<T, U> {
    type Target = Either<T::Target, U::Target>;

    #[inline]
    fn unwrap_context(self) -> Self::Target {
        match self {
            Either::Left(l) => Either::Left(l.unwrap_context()),
            Either::Right(r) => Either::Right(r.unwrap_context()),
        }
    }
}
