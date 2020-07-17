use kvproto::span as spanpb;
use minitrace::{Link, Span, SpanSet};

pub fn encode_spans(span_sets: Vec<SpanSet>) -> impl Iterator<Item = spanpb::SpanSet> {
    span_sets
        .into_iter()
        .map(|span_set| {
            let mut pb_set = spanpb::SpanSet::default();
            pb_set.set_create_time_ns(span_set.create_time_ns);
            pb_set.set_start_time_ns(span_set.start_time_ns);

            let spans = span_set.spans.into_iter().map(|span| {
                let mut s = spanpb::Span::default();
                s.set_id(span.id);
                s.set_begin_cycles(span.begin_cycles);
                s.set_end_cycles(span.elapsed_cycles);
                s.set_event(span.event);

                #[cfg(feature = "prost-codec")]
                {
                    s.link = Some(spanpb::Link {
                        link: Some(match span.link {
                            Link::Root => spanpb::link::Link::Root(spanpb::Root {}),
                            Link::Parent { id } => {
                                spanpb::link::Link::Parent(spanpb::Parent { id })
                            }
                            Link::Continue { id } => {
                                spanpb::link::Link::Continue(spanpb::Continue { id })
                            }
                        }),
                    });
                }

                #[cfg(feature = "protobuf-codec")]
                {
                    let mut link = spanpb::Link::new();
                    match span.link {
                        Link::Root => link.set_root(spanpb::Root::new()),
                        Link::Parent { id } => {
                            let mut parent = spanpb::Parent::new();
                            parent.set_id(id);
                            link.set_parent(parent);
                        }
                        Link::Continue { id } => {
                            let mut cont = spanpb::Continue::new();
                            cont.set_id(id);
                            link.set_continue(cont);
                        }
                    };
                    s.set_link(link);
                }
                s
            });

            pb_set.set_spans(spans.collect());

            pb_set
        })
        .into_iter()
}

pub fn decode_spans(span_sets: Vec<spanpb::SpanSet>) -> impl Iterator<Item = SpanSet> {
    span_sets.into_iter().map(|span_set| {
        let spans = span_set
            .spans
            .into_iter()
            .map(|span| {
                #[cfg(feature = "prost-codec")]
                {
                    if let Some(link) = span.link {
                        let link = match link.link {
                            Some(spanpb::link::Link::Root(spanpb::Root {})) => Link::Root,
                            Some(spanpb::link::Link::Parent(spanpb::Parent { id })) => {
                                Link::Parent { id }
                            }
                            Some(spanpb::link::Link::Continue(spanpb::Continue { id })) => {
                                Link::Continue { id }
                            }
                            _ => panic!("Link should not be none from spanpb"),
                        };
                        Span {
                            id: span.id,
                            begin_cycles: span.begin_cycles,
                            elapsed_cycles: span.end_cycles,
                            event: span.event,
                            link,
                        }
                    } else {
                        panic!("Link should not be none from spanpb")
                    }
                }
                #[cfg(feature = "protobuf-codec")]
                {
                    let link = if span.get_link().has_root() {
                        Link::Root
                    } else if span.get_link().has_parent() {
                        Link::Parent {
                            id: span.get_link().get_parent().id,
                        }
                    } else if span.get_link().has_continue() {
                        Link::Continue {
                            id: span.get_link().get_continue().id,
                        }
                    } else {
                        panic!("Link must be one of root, parent or continue")
                    };
                    Span {
                        id: span.id,
                        begin_cycles: span.begin_cycles,
                        elapsed_cycles: span.end_cycles,
                        event: span.event,
                        link,
                    }
                }
            })
            .collect();
        SpanSet {
            create_time_ns: span_set.create_time_ns,
            start_time_ns: span_set.start_time_ns,
            spans,
        }
    })
}

pub fn memcopy(span_sets: Vec<SpanSet>) -> Vec<u8> {
    let size: usize = span_sets
        .iter()
        .map(|span_set| {
            span_set.spans.len() * std::mem::size_of::<minitrace::Span>()
                + std::mem::size_of::<u64>() * 2
        })
        .sum();

    let mut buf = Vec::<u8>::with_capacity(size);
    unsafe { buf.set_len(size) };
    let mut cur_ptr = buf.as_mut_ptr();
    for span_set in span_sets {
        unsafe {
            std::ptr::copy_nonoverlapping(
                &span_set.create_time_ns as *const _ as *const u8,
                cur_ptr,
                8,
            );
            cur_ptr = cur_ptr.offset(8);
            std::ptr::copy_nonoverlapping(
                &span_set.start_time_ns as *const _ as *const u8,
                cur_ptr,
                8,
            );
            cur_ptr = cur_ptr.offset(8);
            let span_size = span_set.spans.len() * std::mem::size_of::<minitrace::Span>();
            std::ptr::copy_nonoverlapping(span_set.spans.as_ptr() as *const u8, cur_ptr, span_size);
            cur_ptr = cur_ptr.offset(span_size as isize);
        }
    }

    buf
}

#[cfg(test)]
mod tests {
    use minitrace::{Link, Span, SpanSet};
    use std::{u32, u64};

    #[test]
    fn test_encode_spans() {
        let raw_span_sets = vec![
            vec![
                SpanSet {
                    create_time_ns: 0,
                    start_time_ns: 1,
                    spans: vec![
                        Span {
                            id: 0,
                            link: Link::Root,
                            begin_cycles: 0,
                            elapsed_cycles: 10,
                            event: 0,
                        },
                        Span {
                            id: 1,
                            link: Link::Parent { id: 0 },
                            begin_cycles: 0,
                            elapsed_cycles: 9,
                            event: 1,
                        },
                    ],
                },
                SpanSet {
                    create_time_ns: 3,
                    start_time_ns: 2,
                    spans: vec![
                        Span {
                            id: 2,
                            link: Link::Continue { id: 0 },
                            begin_cycles: 10,
                            elapsed_cycles: 20,
                            event: 2,
                        },
                        Span {
                            id: 3,
                            link: Link::Parent { id: 2 },
                            begin_cycles: 20,
                            elapsed_cycles: 30,
                            event: 3,
                        },
                    ],
                },
            ],
            vec![],
            vec![
                SpanSet {
                    create_time_ns: u64::MAX,
                    start_time_ns: u64::MAX,
                    spans: vec![
                        Span {
                            id: u64::MAX,
                            link: Link::Root,
                            begin_cycles: u64::MAX,
                            elapsed_cycles: u64::MAX,
                            event: u32::MAX,
                        },
                        Span {
                            id: u64::MAX,
                            link: Link::Parent { id: u64::MAX },
                            begin_cycles: u64::MAX,
                            elapsed_cycles: u64::MAX,
                            event: u32::MAX,
                        },
                    ],
                },
                SpanSet {
                    create_time_ns: u64::MAX,
                    start_time_ns: u64::MAX,
                    spans: vec![Span {
                        id: u64::MAX,
                        link: Link::Continue { id: u64::MAX },
                        begin_cycles: u64::MAX,
                        elapsed_cycles: u64::MAX,
                        event: u32::MAX,
                    }],
                },
            ],
        ];
        for raw_span_set in raw_span_sets {
            let spanpb_set_vec =
                crate::trace::encode_spans(raw_span_set.clone()).collect::<Vec<_>>();
            let encode_and_decode: Vec<_> = crate::trace::decode_spans(spanpb_set_vec).collect();
            assert_eq!(raw_span_set, encode_and_decode)
        }
    }
}
