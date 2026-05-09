//! End-to-end pipeline tests for operators.
//!
//! These tests verify that operators can be composed into pipelines
//! that correctly process data end-to-end.

#[cfg(test)]
mod tests {
    use std::sync::{Arc, Mutex};

    use crate::dataflow::operators::inspect::InspectOperator;
    use crate::dataflow::operators::probe::ProbeOperator;
    use crate::dataflow::operators::unary::UnaryOperator;
    use crate::dataflow::stage::StageId;
    use crate::progress::frontier::Antichain;

    /// Simulate a pipeline: input → unary(double) → inspect(collect) → probe
    /// by manually wiring operator handles together.
    #[test]
    fn pipeline_unary_inspect_probe() {
        let stage = StageId::new(0);

        // Create operators
        let mut unary = UnaryOperator::<u64, i32, i32>::new("double", 0, stage, |input, output| {
            while let Some((time, data)) = input.next() {
                let mut session = output.session(time);
                for item in data {
                    session.give(item * 2);
                }
            }
            Ok(())
        });

        let collected = Arc::new(Mutex::new(Vec::new()));
        let collected_clone = Arc::clone(&collected);

        let mut inspect = InspectOperator::<u64, i32>::new(
            "collector",
            1,
            stage,
            move |time: &u64, data: &[i32]| {
                let mut guard = collected_clone.lock().unwrap();
                for item in data {
                    guard.push((*time, *item));
                }
            },
        );

        let (mut probe_op, probe_handle) = ProbeOperator::<u64, i32>::new("end_probe", 2, stage);

        // Feed input data
        unary.input_mut().push_vec(1, vec![10, 20, 30]);
        unary.input_mut().push_vec(2, vec![5, 15]);

        // Activate unary → produces doubled output
        unary.activate().unwrap();

        // Wire unary output → inspect input
        for (time, data) in unary.drain_output() {
            inspect.input_mut().push_vec(time, data);
        }

        // Activate inspect → observes and passes through
        inspect.activate().unwrap();

        // Wire inspect output → probe input
        for (time, data) in inspect.drain_output() {
            probe_op.input_mut().push_vec(time, data);
        }

        // Activate probe → drains input
        probe_op.activate();

        // Verify collected data
        let result = collected.lock().unwrap();
        assert_eq!(
            *result,
            vec![
                (1, 20),
                (1, 40),
                (1, 60), // 10*2, 20*2, 30*2
                (2, 10),
                (2, 30), // 5*2, 15*2
            ]
        );

        // Simulate frontier advance
        probe_handle.update_frontier(Antichain::from_elem(2));
        assert!(probe_handle.less_than(&3));
        assert!(!probe_handle.less_than(&2));
    }

    /// Pipeline with multiple unary stages chained.
    #[test]
    fn pipeline_chained_unary() {
        let stage = StageId::new(0);

        // Stage 1: add 1
        let mut add_one =
            UnaryOperator::<u64, i32, i32>::new("add_one", 0, stage, |input, output| {
                while let Some((time, data)) = input.next() {
                    let mut session = output.session(time);
                    for item in data {
                        session.give(item + 1);
                    }
                }
                Ok(())
            });

        // Stage 2: multiply by 3
        let mut mul_three =
            UnaryOperator::<u64, i32, i32>::new("mul_three", 1, stage, |input, output| {
                while let Some((time, data)) = input.next() {
                    let mut session = output.session(time);
                    for item in data {
                        session.give(item * 3);
                    }
                }
                Ok(())
            });

        // Stage 3: to_string
        let mut to_str =
            UnaryOperator::<u64, i32, String>::new("to_string", 2, stage, |input, output| {
                while let Some((time, data)) = input.next() {
                    let mut session = output.session(time);
                    for item in data {
                        session.give(format!("result={item}"));
                    }
                }
                Ok(())
            });

        // Feed: [1, 2, 3] at time 0
        add_one.input_mut().push_vec(0, vec![1, 2, 3]);
        add_one.activate().unwrap();

        // Wire stage 1 → stage 2
        for (t, d) in add_one.drain_output() {
            mul_three.input_mut().push_vec(t, d);
        }
        mul_three.activate().unwrap();

        // Wire stage 2 → stage 3
        for (t, d) in mul_three.drain_output() {
            to_str.input_mut().push_vec(t, d);
        }
        to_str.activate().unwrap();

        let results: Vec<_> = to_str.drain_output().collect();
        // (1+1)*3=6, (2+1)*3=9, (3+1)*3=12
        assert_eq!(
            results,
            vec![(
                0,
                vec![
                    "result=6".to_string(),
                    "result=9".to_string(),
                    "result=12".to_string()
                ]
            )]
        );
    }

    /// Pipeline with stateful unary (running sum) + inspect.
    #[test]
    fn pipeline_stateful_unary_inspect() {
        let stage = StageId::new(0);
        let mut running_sum = 0i64;

        let mut sum_op =
            UnaryOperator::<u64, i32, i64>::new("running_sum", 0, stage, move |input, output| {
                while let Some((time, data)) = input.next() {
                    for item in data {
                        running_sum += item as i64;
                    }
                    let mut session = output.session(time);
                    session.give(running_sum);
                }
                Ok(())
            });

        let sums = Arc::new(Mutex::new(Vec::new()));
        let sums_clone = Arc::clone(&sums);

        let mut inspect = InspectOperator::<u64, i64>::new(
            "sum_observer",
            1,
            stage,
            move |time: &u64, data: &[i64]| {
                let mut guard = sums_clone.lock().unwrap();
                for item in data {
                    guard.push((*time, *item));
                }
            },
        );

        // Batch 1
        sum_op.input_mut().push_vec(1, vec![10, 20]);
        sum_op.activate().unwrap();
        for (t, d) in sum_op.drain_output() {
            inspect.input_mut().push_vec(t, d);
        }
        inspect.activate().unwrap();
        let _ = inspect.drain_output().count(); // drain

        // Batch 2
        sum_op.input_mut().push_vec(2, vec![5, 15]);
        sum_op.activate().unwrap();
        for (t, d) in sum_op.drain_output() {
            inspect.input_mut().push_vec(t, d);
        }
        inspect.activate().unwrap();
        let _ = inspect.drain_output().count();

        let results = sums.lock().unwrap();
        assert_eq!(*results, vec![(1, 30), (2, 50)]);
    }

    /// Pipeline with filter unary — some data is dropped.
    #[test]
    fn pipeline_filter_inspect_probe() {
        let stage = StageId::new(0);

        let mut filter =
            UnaryOperator::<u64, i32, i32>::new("keep_positive", 0, stage, |input, output| {
                while let Some((time, data)) = input.next() {
                    let mut session = output.session(time);
                    for item in data {
                        if item > 0 {
                            session.give(item);
                        }
                    }
                }
                Ok(())
            });

        let seen = Arc::new(Mutex::new(Vec::new()));
        let seen_clone = Arc::clone(&seen);

        let mut inspect = InspectOperator::<u64, i32>::new(
            "observe",
            1,
            stage,
            move |time: &u64, data: &[i32]| {
                let mut guard = seen_clone.lock().unwrap();
                for item in data {
                    guard.push((*time, *item));
                }
            },
        );

        let (mut probe_op, handle) = ProbeOperator::<u64, i32>::new("probe", 2, stage);

        // Feed mixed positive/negative data
        filter.input_mut().push_vec(1, vec![-5, 3, -1, 7, 0, 2]);
        filter.activate().unwrap();

        for (t, d) in filter.drain_output() {
            inspect.input_mut().push_vec(t, d);
        }
        inspect.activate().unwrap();

        for (t, d) in inspect.drain_output() {
            probe_op.input_mut().push_vec(t, d);
        }
        probe_op.activate();

        let results = seen.lock().unwrap();
        assert_eq!(*results, vec![(1, 3), (1, 7), (1, 2)]);

        // Mark done
        filter.input_mut().mark_exhausted();
        inspect.input_mut().mark_exhausted();
        probe_op.input_mut().mark_exhausted();
        probe_op.activate();
        assert!(handle.done());
    }

    /// Pipeline completion: mark_exhausted propagates through.
    #[test]
    fn pipeline_completion() {
        let stage = StageId::new(0);

        let mut pass = UnaryOperator::<u64, i32, i32>::new("pass", 0, stage, |input, output| {
            while let Some((time, data)) = input.next() {
                let mut session = output.session(time);
                for item in data {
                    session.give(item);
                }
            }
            Ok(())
        });

        let (mut probe_op, handle) = ProbeOperator::<u64, i32>::new("probe", 1, stage);

        // Process some data
        pass.input_mut().push_vec(1, vec![42]);
        pass.activate().unwrap();
        for (t, d) in pass.drain_output() {
            probe_op.input_mut().push_vec(t, d);
        }
        probe_op.activate();
        assert!(!handle.done());

        // Signal exhaustion
        pass.input_mut().mark_exhausted();
        assert!(pass.is_done());

        probe_op.input_mut().mark_exhausted();
        probe_op.activate();
        assert!(handle.done());
    }

    /// Verify probe frontier tracking through a pipeline.
    #[test]
    fn pipeline_probe_frontier_tracking() {
        let stage = StageId::new(0);

        let mut op = UnaryOperator::<u64, i32, i32>::new("identity", 0, stage, |input, output| {
            while let Some((time, data)) = input.next() {
                let mut session = output.session(time);
                for item in data {
                    session.give(item);
                }
            }
            Ok(())
        });

        let (mut probe_op, handle) = ProbeOperator::<u64, i32>::new("probe", 1, stage);

        // Initial frontier at minimum (0)
        assert!(handle.less_equal(&0));

        // Process time=1 data
        op.input_mut().push_vec(1, vec![10]);
        op.activate().unwrap();
        for (t, d) in op.drain_output() {
            probe_op.input_mut().push_vec(t, d);
        }
        probe_op.activate();

        // Simulate frontier advance to {2}
        handle.update_frontier(Antichain::from_elem(2));
        assert!(handle.less_than(&3));
        assert!(!handle.less_than(&2));
        assert!(handle.less_equal(&2));

        // Advance further to {5}
        handle.update_frontier(Antichain::from_elem(5));
        assert!(!handle.less_than(&4));
        assert!(handle.less_than(&6));

        // Done
        handle.mark_done();
        assert!(handle.done());
        assert!(handle.frontier().elements().is_empty());
    }

    /// StreamEdge extension chaining: unary → inspect → probe
    #[test]
    fn stream_ext_chaining() {
        use crate::dataflow::operators::inspect::InspectExt;
        use crate::dataflow::operators::probe::ProbeExt;
        use crate::dataflow::operators::unary::UnaryExt;
        use crate::dataflow::scope::{RootScope, Scope};
        use crate::dataflow::stream::{Slot, StreamEdge};

        let mut scope = RootScope::<u64>::new("pipeline", 4);
        let stage_id = scope.current_stage_id();
        let src_idx = scope.allocate_operator_index();
        let source = Slot::new(src_idx, 0);
        let stream: StreamEdge<RootScope<u64>, i32> = StreamEdge::new(scope, source, stage_id);

        // Chain: unary(double) → inspect → probe
        let handle = stream
            .unary("double", |input, output| {
                while let Some((time, data)) = input.next() {
                    let mut session = output.session(time);
                    for item in data {
                        session.give(item * 2);
                    }
                }
                Ok(())
            })
            .inspect("observe", |_time, _data| {})
            .probe("end_probe");

        // Probe handle is ready
        assert!(!handle.done());
        // The scope allocated: source(0), double(1), inspect(2), probe(3)
        assert_eq!(stream.scope().operator_count(), 4);
    }

    /// End-to-end: two inputs → binary → inspect → probe
    #[test]
    fn pipeline_binary_inspect_probe() {
        use crate::dataflow::operators::binary::BinaryOperator;

        let stage = StageId::new(0);
        let collected = Arc::new(Mutex::new(Vec::new()));
        let collected_clone = Arc::clone(&collected);

        let mut binary = BinaryOperator::<u64, i32, i32, i32>::new(
            "sum_pairs",
            0,
            stage,
            |input1, input2, output| {
                // Merge both inputs to output
                while let Some((t, d)) = input1.next() {
                    let mut s = output.session(t);
                    for item in d {
                        s.give(item);
                    }
                }
                while let Some((t, d)) = input2.next() {
                    let mut s = output.session(t);
                    for item in d {
                        s.give(item);
                    }
                }
                Ok(())
            },
        );

        let mut inspect = InspectOperator::<u64, i32>::new(
            "collector",
            1,
            stage,
            move |time: &u64, data: &[i32]| {
                let mut guard = collected_clone.lock().unwrap();
                for item in data {
                    guard.push((*time, *item));
                }
            },
        );

        let (mut probe_op, handle) = ProbeOperator::<u64, i32>::new("probe", 2, stage);

        // Feed data to both inputs
        binary.input1_mut().push_vec(1, vec![10, 20]);
        binary.input2_mut().push_vec(1, vec![100]);
        binary.input2_mut().push_vec(2, vec![200]);

        binary.activate().unwrap();
        for (t, d) in binary.drain_output() {
            inspect.input_mut().push_vec(t, d);
        }
        inspect.activate().unwrap();
        for (t, d) in inspect.drain_output() {
            probe_op.input_mut().push_vec(t, d);
        }
        probe_op.activate();

        let results = collected.lock().unwrap();
        assert_eq!(*results, vec![(1, 10), (1, 20), (1, 100), (2, 200)]);

        handle.update_frontier(Antichain::from_elem(3));
        assert!(handle.less_than(&4));
    }

    /// End-to-end: input → delay_batch → inspect — verify output order
    #[test]
    fn pipeline_delay_batch_inspect() {
        use crate::dataflow::operators::delay::DelayBatchOperator;

        let stage = StageId::new(0);

        let mut delay =
            DelayBatchOperator::<u64, i32, _>::new("delay_by_10", 0, stage, |time: &u64| time + 10);

        let collected = Arc::new(Mutex::new(Vec::new()));
        let collected_clone = Arc::clone(&collected);

        let mut inspect = InspectOperator::<u64, i32>::new(
            "observer",
            1,
            stage,
            move |time: &u64, data: &[i32]| {
                let mut guard = collected_clone.lock().unwrap();
                for item in data {
                    guard.push((*time, *item));
                }
            },
        );

        // Feed data at times 1 and 2
        delay.input_mut().push_vec(1, vec![10, 20]);
        delay.input_mut().push_vec(2, vec![30]);
        delay.activate().unwrap();

        // Nothing released yet (frontier at 0)
        assert_eq!(delay.buffered_timestamps(), 2);

        // Advance frontier past time 11 (releases time 11) but not 12
        delay.update_frontier(Antichain::from_elem(12));
        delay.activate().unwrap();

        for (t, d) in delay.drain_output() {
            inspect.input_mut().push_vec(t, d);
        }
        inspect.activate().unwrap();
        let _ = inspect.drain_output().count();

        {
            let results = collected.lock().unwrap();
            assert_eq!(*results, vec![(11, 10), (11, 20)]);
        }

        // Advance past 12
        delay.update_frontier(Antichain::from_elem(13));
        delay.activate().unwrap();

        for (t, d) in delay.drain_output() {
            inspect.input_mut().push_vec(t, d);
        }
        inspect.activate().unwrap();
        let _ = inspect.drain_output().count();

        let results = collected.lock().unwrap();
        assert_eq!(*results, vec![(11, 10), (11, 20), (12, 30)]);
    }

    /// End-to-end: concat + unary pipeline
    #[test]
    fn pipeline_concat_unary() {
        use crate::dataflow::operators::concat::ConcatOperator;

        let stage = StageId::new(0);

        let mut concat = ConcatOperator::<u64, i32>::new("merge", 0, stage, 2);
        let mut double =
            UnaryOperator::<u64, i32, i32>::new("double", 1, stage, |input, output| {
                while let Some((time, data)) = input.next() {
                    let mut session = output.session(time);
                    for item in data {
                        session.give(item * 2);
                    }
                }
                Ok(())
            });

        concat.input_mut(0).push_vec(1, vec![5]);
        concat.input_mut(1).push_vec(1, vec![10]);
        concat.input_mut(1).push_vec(2, vec![15]);

        concat.activate().unwrap();
        for (t, d) in concat.drain_output() {
            double.input_mut().push_vec(t, d);
        }
        double.activate().unwrap();

        let results: Vec<_> = double.drain_output().collect();
        assert_eq!(results[0], (1, vec![10])); // 5*2
        assert_eq!(results[1], (1, vec![20])); // 10*2
        assert_eq!(results[2], (2, vec![30])); // 15*2
    }

    /// End-to-end: chaining exchange → unary → gather → probe at StreamEdge level.
    #[test]
    fn pipeline_exchange_gather_probe() {
        use crate::dataflow::operators::exchange::ExchangeExt;
        use crate::dataflow::operators::gather::GatherExt;
        use crate::dataflow::operators::probe::ProbeExt;
        use crate::dataflow::operators::unary::UnaryExt;
        use crate::dataflow::scope::{RootScope, Scope};
        use crate::dataflow::stream::{Slot, StreamEdge};

        let mut scope = RootScope::<u64>::new("pipeline", 4);
        let stage_id = scope.current_stage_id();
        let src_idx = scope.allocate_operator_index();
        let source = Slot::new(src_idx, 0);
        let stream: StreamEdge<RootScope<u64>, (u64, i32)> =
            StreamEdge::new(scope, source, stage_id);

        // Chain: exchange(by key) → exchange_to(16) → unary → gather → probe
        let handle = stream
            .exchange(|r: &(u64, i32)| r.0)
            .exchange_to(16, |r: &(u64, i32)| r.0)
            .unwrap()
            .unary("process", |input, output| {
                while let Some((time, data)) = input.next() {
                    let mut session = output.session(time);
                    for item in data {
                        session.give(item);
                    }
                }
                Ok(())
            })
            .gather()
            .probe("final");

        assert!(!handle.done());
    }

    /// Pipeline with rebalance → broadcast → inspect.
    #[test]
    fn pipeline_rebalance_broadcast_chain() {
        use crate::dataflow::operators::broadcast::BroadcastExt;
        use crate::dataflow::operators::inspect::InspectExt;
        use crate::dataflow::operators::rebalance::RebalanceExt;
        use crate::dataflow::scope::{RootScope, Scope};
        use crate::dataflow::stream::{Slot, StreamEdge};

        let mut scope = RootScope::<u64>::new("pipeline", 4);
        let stage_id = scope.current_stage_id();
        let src_idx = scope.allocate_operator_index();
        let source = Slot::new(src_idx, 0);
        let stream: StreamEdge<RootScope<u64>, i32> = StreamEdge::new(scope, source, stage_id);

        // Chain: rebalance_to(8) → broadcast → broadcast_local → inspect
        let _output = stream
            .rebalance_to(8)
            .unwrap()
            .broadcast()
            .broadcast_local()
            .inspect("observe", |_time, _data| {});

        // Verify stages were created correctly
        assert_ne!(_output.stage_id(), stage_id);
    }

    /// Verify stage transitions through a multi-repartition pipeline.
    #[test]
    fn pipeline_stage_transitions() {
        use crate::dataflow::operators::exchange::ExchangeExt;
        use crate::dataflow::operators::gather::GatherExt;
        use crate::dataflow::operators::rebalance::RebalanceExt;
        use crate::dataflow::scope::{RootScope, Scope};
        use crate::dataflow::stream::{Slot, StreamEdge};

        let mut scope = RootScope::<u64>::new("test", 4);
        let r0 = scope.current_stage_id();
        let src_idx = scope.allocate_operator_index();
        let source = Slot::new(src_idx, 0);
        let stream: StreamEdge<RootScope<u64>, i32> = StreamEdge::new(scope.clone(), source, r0);

        // exchange same parallelism → same stage
        let s1 = stream.exchange(|x: &i32| *x);
        assert_eq!(s1.stage_id(), r0);

        // exchange_to different parallelism → new stage
        let s2 = s1.exchange_to(8, |x: &i32| *x).unwrap();
        let r1 = s2.stage_id();
        assert_ne!(r1, r0);
        assert_eq!(scope.stage_parallelism(r1), Some(8));

        // rebalance same parallelism → same stage
        let s3 = s2.rebalance();
        assert_eq!(s3.stage_id(), r1);

        // gather → new stage parallelism=1
        let s4 = s3.gather();
        let r2 = s4.stage_id();
        assert_ne!(r2, r1);
        assert_eq!(scope.stage_parallelism(r2), Some(1));

        // rebalance_to(4) from gather → new stage
        let s5 = s4.rebalance_to(4).unwrap();
        let r3 = s5.stage_id();
        assert_ne!(r3, r2);
        assert_eq!(scope.stage_parallelism(r3), Some(4));
    }

    // ===================================================================
    // Graph construction integration tests
    // ===================================================================

    /// Verify that a simple pipeline registers operators and edges in the graph.
    #[test]
    fn graph_simple_pipeline() {
        use crate::dataflow::operators::inspect::InspectExt;
        use crate::dataflow::operators::probe::ProbeExt;
        use crate::dataflow::operators::unary::UnaryExt;
        use crate::dataflow::scope::{RootScope, Scope};
        use crate::dataflow::stream::{Slot, StreamEdge};

        let mut scope = RootScope::<u64>::new("test", 4);
        let stage_id = scope.current_stage_id();

        // Simulate a source operator (index 0).
        let src_idx = scope.allocate_operator_index();
        scope
            .register_operator(crate::dataflow::graph::OperatorInfo::new(
                src_idx, "source", stage_id, 0, 1,
            ))
            .unwrap();

        let source = Slot::new(src_idx, 0);
        let stream: StreamEdge<RootScope<u64>, i32> =
            StreamEdge::new(scope.clone(), source, stage_id);

        // Build: source → unary(double) → inspect → probe
        let _handle = stream
            .unary("double", |input, output| {
                while let Some((t, d)) = input.next() {
                    let mut s = output.session(t);
                    for x in d {
                        s.give(x * 2);
                    }
                }
                Ok(())
            })
            .inspect("observe", |_t, _d| {})
            .probe("end");

        let graph = scope.graph();

        // 4 operators: source(1), double(2), observe(3), end(4)
        assert_eq!(graph.operator_count(), 4);
        assert_eq!(graph.operator(1).unwrap().name, "source");
        assert_eq!(graph.operator(2).unwrap().name, "double");
        assert_eq!(graph.operator(3).unwrap().name, "observe");
        assert_eq!(graph.operator(4).unwrap().name, "end");

        // 3 edges: source→double, double→observe, observe→end
        assert_eq!(graph.edge_count(), 3);

        // Topological order
        let order = graph.topological_order().unwrap();
        assert_eq!(order, vec![1, 2, 3, 4]);

        // Validation passes
        assert!(graph.validate().is_ok());
    }

    /// Verify graph construction with binary operators.
    #[test]
    fn graph_binary_pipeline() {
        use crate::dataflow::operators::binary::BinaryExt;
        use crate::dataflow::operators::probe::ProbeExt;
        use crate::dataflow::scope::{RootScope, Scope};
        use crate::dataflow::stream::{Slot, StreamEdge};

        let mut scope = RootScope::<u64>::new("test", 4);
        let stage_id = scope.current_stage_id();

        // Two source operators.
        let src0 = scope.allocate_operator_index();
        scope
            .register_operator(crate::dataflow::graph::OperatorInfo::new(
                src0,
                "left_source",
                stage_id,
                0,
                1,
            ))
            .unwrap();
        let src1 = scope.allocate_operator_index();
        scope
            .register_operator(crate::dataflow::graph::OperatorInfo::new(
                src1,
                "right_source",
                stage_id,
                0,
                1,
            ))
            .unwrap();

        let left: StreamEdge<RootScope<u64>, i32> =
            StreamEdge::new(scope.clone(), Slot::new(src0, 0), stage_id);
        let right: StreamEdge<RootScope<u64>, i32> =
            StreamEdge::new(scope.clone(), Slot::new(src1, 0), stage_id);

        let _handle = left
            .binary(&right, "join", |l, r, out| {
                while let Some((t, d)) = l.next() {
                    out.push_vec(t, d);
                }
                while let Some((t, d)) = r.next() {
                    out.push_vec(t, d);
                }
                Ok(())
            })
            .probe("end");

        let graph = scope.graph();
        assert_eq!(graph.operator_count(), 4); // 2 sources + join + probe
        assert_eq!(graph.edge_count(), 3); // src0→join, src1→join, join→probe

        // Binary op has 2 inputs
        assert_eq!(graph.operator(3).unwrap().input_count, 2);

        let order = graph.topological_order().unwrap();
        // Both sources before join, join before probe
        let join_pos = order.iter().position(|&x| x == 3).unwrap();
        let probe_pos = order.iter().position(|&x| x == 4).unwrap();
        assert!(join_pos < probe_pos);

        assert!(graph.validate().is_ok());
    }

    /// Verify graph construction with branch (1 input, 2 outputs).
    #[test]
    fn graph_branch_pipeline() {
        use crate::dataflow::operators::branch::BranchExt;
        use crate::dataflow::operators::probe::ProbeExt;
        use crate::dataflow::scope::{RootScope, Scope};
        use crate::dataflow::stream::{Slot, StreamEdge};

        let mut scope = RootScope::<u64>::new("test", 4);
        let stage_id = scope.current_stage_id();

        let src_idx = scope.allocate_operator_index();
        scope
            .register_operator(crate::dataflow::graph::OperatorInfo::new(
                src_idx, "source", stage_id, 0, 1,
            ))
            .unwrap();

        let stream: StreamEdge<RootScope<u64>, i32> =
            StreamEdge::new(scope.clone(), Slot::new(src_idx, 0), stage_id);

        let (true_stream, false_stream) = stream.branch(|x| *x > 0);
        let _p1 = true_stream.probe("pos");
        let _p2 = false_stream.probe("neg");

        let graph = scope.graph();
        // source(1), branch(2), pos(3), neg(4)
        assert_eq!(graph.operator_count(), 4);
        assert_eq!(graph.operator(2).unwrap().output_count, 2);
        assert!(graph.validate().is_ok());
    }
}
