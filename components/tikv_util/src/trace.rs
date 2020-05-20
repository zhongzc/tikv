use kvproto::span as spanpb;
use num_enum::{IntoPrimitive, TryFromPrimitive};
use std::convert::TryFrom;

#[derive(Display, Debug, Eq, PartialEq, TryFromPrimitive, IntoPrimitive)]
#[repr(u32)]
pub enum TraceEvent {
    #[allow(dead_code)]
    Unknown = 0,
    CoprRequest,
    HandleUnaryRequest,
    Snapshot,
    HandleChecksum,
    HandleDag,
    HandleBatchDag,
    HandleAnalyze,
    HandleCached,
    BatchHandle,
    BatchHandleLoop,
    TopN,
    TableScan,
    StreamAgg,
    SlowHashAgg,
    SimpleAgg,
    Selection,
    FastHashAgg,
    Limit,
    IndexScan,
}

impl Into<spanpb::Event> for TraceEvent {
    fn into(self) -> spanpb::Event {
        match self {
            TraceEvent::Snapshot => spanpb::Event::Snapshot,
            TraceEvent::HandleChecksum => spanpb::Event::HandleChecksum,
            TraceEvent::HandleDag => spanpb::Event::HandleDag,
            TraceEvent::HandleBatchDag => spanpb::Event::HandleBatchDag,
            TraceEvent::HandleAnalyze => spanpb::Event::HandleAnalyze,
            TraceEvent::HandleCached => spanpb::Event::HandleCached,
            _ => spanpb::Event::Unknown,
        }
    }
}

pub fn encode_spans(finished_spans: Vec<minitrace::Span>) -> impl Iterator<Item = spanpb::Span> {
    let spans = finished_spans.into_iter().map(|span| {
        let mut s = spanpb::Span::default();

        s.set_id(span.id.into());
        s.set_start(span.elapsed_start);
        s.set_end(span.elapsed_end);
        s.set_event(
            TraceEvent::try_from(span.tag)
                .unwrap_or(TraceEvent::Unknown)
                .into(),
        );

        #[cfg(feature = "prost-codec")]
        {
            if let Some(p) = span.parent {
                s.parent = Some(spanpb::Parent::ParentValue(p.into()));
            } else {
                s.parent = Some(spanpb::Parent::ParentNone(true));
            }
        }

        #[cfg(feature = "protobuf-codec")]
        {
            if let Some(p) = span.parent {
                s.set_parent_value(p.into());
            }
        }

        s
    });
    spans.into_iter()
}
