use kvproto::span as spanpb;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
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

impl Into<u32> for TraceEvent {
    fn into(self) -> u32 {
        self as u32
    }
}

impl From<u32> for TraceEvent {
    fn from(x: u32) -> Self {
        match x {
            _ if x == TraceEvent::CoprRequest as u32 => TraceEvent::CoprRequest,
            _ if x == TraceEvent::HandleUnaryRequest as u32 => TraceEvent::HandleUnaryRequest,
            _ if x == TraceEvent::Snapshot as u32 => TraceEvent::Snapshot,
            _ if x == TraceEvent::HandleChecksum as u32 => TraceEvent::HandleChecksum,
            _ if x == TraceEvent::HandleDag as u32 => TraceEvent::HandleDag,
            _ if x == TraceEvent::HandleBatchDag as u32 => TraceEvent::HandleBatchDag,
            _ if x == TraceEvent::HandleAnalyze as u32 => TraceEvent::HandleAnalyze,
            _ if x == TraceEvent::HandleCached as u32 => TraceEvent::HandleCached,
            _ if x == TraceEvent::BatchHandle as u32 => TraceEvent::BatchHandle,
            _ if x == TraceEvent::BatchHandleLoop as u32 => TraceEvent::BatchHandleLoop,
            _ if x == TraceEvent::TopN as u32 => TraceEvent::TopN,
            _ if x == TraceEvent::TableScan as u32 => TraceEvent::TableScan,
            _ if x == TraceEvent::StreamAgg as u32 => TraceEvent::StreamAgg,
            _ if x == TraceEvent::SlowHashAgg as u32 => TraceEvent::SlowHashAgg,
            _ if x == TraceEvent::SimpleAgg as u32 => TraceEvent::SimpleAgg,
            _ if x == TraceEvent::Selection as u32 => TraceEvent::Selection,
            _ if x == TraceEvent::FastHashAgg as u32 => TraceEvent::FastHashAgg,
            _ if x == TraceEvent::Limit as u32 => TraceEvent::Limit,
            _ if x == TraceEvent::IndexScan as u32 => TraceEvent::IndexScan,
            _  => TraceEvent::Unknown,
        }
    }
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
        s.set_event(TraceEvent::from(span.tag).into());

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
