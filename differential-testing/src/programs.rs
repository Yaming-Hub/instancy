//! Random dataflow programs: a small typed stage list that each backend
//! interprets with its own native operators. Records are
//! `(key, (value, hops))`; `hops` packs one 8-bit iteration counter per loop
//! depth, so loop exits are data-driven and identical on both engines.

use crate::Rng;

/// A program record: `(key, (value, hops))`.
pub type PRec = (u64, (i64, u64));

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Stage {
    /// `value += c`.
    AddValue(i64),
    /// `key = (key + c) % 8`.
    RotateKey(u64),
    /// Keep records with `value >= c`.
    KeepValueAtLeast(i64),
    /// Exchange by key.
    Exchange,
    /// Each record and a copy with `value + 1`.
    Duplicate,
    /// Branch on `value >= 0`, negate the negative side's value, merge.
    BranchNegate,
    /// Delay records with an odd key by one epoch (root level only).
    DelayOddKeys,
    /// Copy every record to every worker (root level only).
    Broadcast,
    /// Apply `body` exactly `iterations` times inside an `iterate` scope.
    Loop { iterations: u64, body: Vec<Stage> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Program {
    pub stages: Vec<Stage>,
}

/// The iteration counter of loop depth `depth` in `hops`.
pub fn hops_at(hops: u64, depth: u32) -> u64 {
    (hops >> (8 * depth)) & 0xff
}

/// `hops` with loop depth `depth`'s counter set to `n`.
pub fn with_hops(hops: u64, depth: u32, n: u64) -> u64 {
    (hops & !(0xff << (8 * depth))) | ((n & 0xff) << (8 * depth))
}

pub fn add_value(r: PRec, c: i64) -> PRec {
    (r.0, (r.1.0 + c, r.1.1))
}

pub fn rotate_key(r: PRec, c: u64) -> PRec {
    ((r.0 + c) % 8, r.1)
}

pub fn duplicate(r: PRec) -> Vec<PRec> {
    vec![r, (r.0, (r.1.0 + 1, r.1.1))]
}

pub fn negate(r: PRec) -> PRec {
    (r.0, (-r.1.0, r.1.1))
}

pub fn route_program(r: &PRec) -> u64 {
    r.0
}

/// A bounded random program: 2-5 root stages, at most one broadcast, one
/// delay and one root loop; loops nest at most two deep.
pub fn random_program(seed: u64) -> Program {
    let mut rng = Rng::new(seed ^ 0x0070_726f_6772_616d);
    let mut broadcast = false;
    let mut delay = false;
    let mut root_loop = false;
    let n = 2 + rng.below(4);
    let mut stages = Vec::new();
    for _ in 0..n {
        let stage = match rng.below(10) {
            0 if !broadcast => {
                broadcast = true;
                Stage::Broadcast
            }
            1 if !delay => {
                delay = true;
                Stage::DelayOddKeys
            }
            2 | 3 if !root_loop => {
                root_loop = true;
                loop_stage(&mut rng, 0)
            }
            _ => simple_stage(&mut rng, true),
        };
        stages.push(stage);
    }
    if !stages.iter().any(|s| matches!(s, Stage::Loop { .. })) && rng.below(2) == 0 {
        stages.push(loop_stage(&mut rng, 0));
    }
    Program { stages }
}

fn simple_stage(rng: &mut Rng, allow_duplicate: bool) -> Stage {
    match rng.below(if allow_duplicate { 6 } else { 5 }) {
        0 => Stage::AddValue(rng.below(5) as i64 - 2),
        1 => Stage::RotateKey(1 + rng.below(7)),
        2 => Stage::KeepValueAtLeast(-(rng.below(9) as i64)),
        3 => Stage::Exchange,
        4 => Stage::BranchNegate,
        _ => Stage::Duplicate,
    }
}

fn loop_stage(rng: &mut Rng, depth: u32) -> Stage {
    let iterations = 1 + rng.below(if depth == 0 { 3 } else { 2 });
    let mut body = Vec::new();
    let mut duplicated = false;
    for _ in 0..1 + rng.below(3) {
        let stage = if depth == 0 && rng.below(4) == 0 {
            loop_stage(rng, depth + 1)
        } else {
            simple_stage(rng, !duplicated)
        };
        duplicated |= stage == Stage::Duplicate;
        body.push(stage);
    }
    if !body.contains(&Stage::Exchange) && rng.below(2) == 0 {
        body.push(Stage::Exchange);
    }
    Stage::Loop { iterations, body }
}

/// The deepest loop nesting of a program.
pub fn depth(stages: &[Stage]) -> u32 {
    stages
        .iter()
        .map(|s| match s {
            Stage::Loop { body, .. } => 1 + depth(body),
            _ => 0,
        })
        .max()
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn programs_are_bounded_and_varied() {
        let mut loops = 0;
        let mut nested = 0;
        let mut delays = 0;
        let mut broadcasts = 0;
        for seed in 1..=64 {
            let p = random_program(seed);
            assert!(depth(&p.stages) <= 2, "{p:?}");
            let count = |s: &Stage| p.stages.iter().filter(|x| *x == s).count();
            assert!(count(&Stage::Broadcast) <= 1 && count(&Stage::DelayOddKeys) <= 1);
            loops += usize::from(depth(&p.stages) >= 1);
            nested += usize::from(depth(&p.stages) == 2);
            delays += count(&Stage::DelayOddKeys);
            broadcasts += count(&Stage::Broadcast);
        }
        assert!(loops >= 16 && nested >= 2 && delays >= 4 && broadcasts >= 4);
    }

    #[test]
    fn hop_counters_are_independent_per_depth() {
        let h = with_hops(with_hops(0, 0, 3), 1, 2);
        assert_eq!((hops_at(h, 0), hops_at(h, 1)), (3, 2));
        assert_eq!(hops_at(with_hops(h, 0, 0), 1), 2);
    }
}
