use itertools::Itertools as _;
use std::collections::BTreeMap;
use std::fmt::{self, Display, Formatter};

#[derive(Debug, Copy, Clone, PartialEq, Eq)]
struct Data(&'static str);

#[derive(Debug, Copy, Clone, PartialEq, Eq, PartialOrd, Ord)]
struct TxId(&'static str);

#[derive(Debug, Copy, Clone)]
enum OpKind {
    Read(Data),
    Write(Data),
    Commit,
    Abort,
}

#[derive(Debug, Copy, Clone)]
struct Op {
    kind: OpKind,
    tx: TxId,
}

impl Display for Op {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        match self.kind {
            OpKind::Read(Data(dat)) => write!(f, "R_{}({})", self.tx.0, dat),
            OpKind::Write(Data(dat)) => write!(f, "W_{}({})", self.tx.0, dat),
            OpKind::Commit => write!(f, "C_{}", self.tx.0),
            OpKind::Abort => write!(f, "A_{}", self.tx.0),
        }
    }
}

macro_rules! op {
    (r, $dat:expr, $tx_id:expr) => {
        Op {
            kind: OpKind::Read(Data(stringify!($dat))),
            tx: TxId(stringify!($tx_id)),
        }
    };

    (w, $dat:expr, $tx_id:expr) => {
        Op {
            kind: OpKind::Write(Data(stringify!($dat))),
            tx: TxId(stringify!($tx_id)),
        }
    };

    (c, $tx_id:expr) => {
        Op {
            kind: OpKind::Commit,
            tx: TxId(stringify!($tx_id)),
        }
    };

    (a, $tx_id:expr) => {
        Op {
            kind: OpKind::Abort,
            tx: TxId(stringify!($tx_id)),
        }
    };
}

macro_rules! sched {
    ( $( ( $( $parts:tt ),* ) ),* ) => {{
        Schedule::new(&[
            $(
                op!(
                    $(
                        $parts
                    ),*
                )
            ),*
        ])
    }};
}

#[derive(Debug, Copy, Clone)]
struct OpPair((Op, Op));

#[derive(Debug)]
struct OpPairView<'v>(&'v [OpPair]);

trait DisplayOpPairs {
    fn display(&self) -> OpPairView;
}

impl DisplayOpPairs for [OpPair] {
    fn display(&self) -> OpPairView {
        OpPairView(self)
    }
}

impl<'v> Display for OpPairView<'v> {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let parts = self
            .0
            .iter()
            .map(|p| format!("({}, {})", p.0 .0, p.0 .1))
            .collect::<Vec<_>>();

        write!(f, "[{}]", parts.join(", "))
    }
}

impl OpPair {
    fn is_conflicting(&self) -> bool {
        self.are_different_transactions() && self.on_same_data() && self.one_is_a_write()
    }

    fn are_different_transactions(&self) -> bool {
        self.0 .0.tx != self.0 .1.tx
    }

    fn on_same_data(&self) -> bool {
        let d1 = match self.0 .0.kind {
            OpKind::Read(d) | OpKind::Write(d) => d,
            _ => return false,
        };

        let d2 = match self.0 .1.kind {
            OpKind::Read(d) | OpKind::Write(d) => d,
            _ => return false,
        };

        d1 == d2
    }

    fn one_is_a_write(&self) -> bool {
        matches!(
            (self.0 .0.kind, self.0 .1.kind),
            (OpKind::Write(..), _) | (_, OpKind::Write(..))
        )
    }
}

#[derive(Debug)]
struct Transaction {
    id: TxId,
    ops: Vec<Op>,
}

impl Display for Transaction {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let parts = self.ops.iter().map(ToString::to_string).collect::<Vec<_>>();
        write!(f, "Transaction '{}': [{}]", self.id.0, parts.join(", "))
    }
}

#[derive(Debug)]
struct Schedule {
    ops: Vec<Op>,
}

impl Schedule {
    fn new(ops: &[Op]) -> Schedule {
        Schedule { ops: ops.to_vec() }
    }

    fn conflicting_pairs(&self) -> Vec<OpPair> {
        self.ops
            .iter()
            .combinations(2)
            .map(|p| OpPair((*p[0], *p[1])))
            .filter(|p| p.is_conflicting())
            .collect()
    }

    fn transactions(&self) -> Vec<Transaction> {
        let mut result: BTreeMap<TxId, Vec<Op>> = BTreeMap::new();

        for op in &self.ops {
            result
                .entry(op.tx)
                .and_modify(|e| e.push(op.clone()))
                .or_insert(vec![op.clone()]);
        }

        result
            .into_iter()
            .map(|(id, ops)| Transaction { id, ops })
            .collect()
    }
}

impl Display for Schedule {
    fn fmt(&self, f: &mut Formatter) -> fmt::Result {
        let parts = self.ops.iter().map(ToString::to_string).collect::<Vec<_>>();
        write!(f, "[{}]", parts.join(", "))
    }
}

fn schedule_report(schedule: &Schedule) {
    println!("Schedule:");
    println!("\t{}", schedule);
    println!();

    println!("Transactions:");
    for tx in &schedule.transactions() {
        println!("\t{}", tx);
    }
    println!();

    println!("Conflicting Pairs:");
    println!("\t{}", schedule.conflicting_pairs().display());
}

fn main() {
    let schedule = sched!((r, a, 1), (w, a, 2), (a, 2), (c, 1));
    schedule_report(&schedule);
}
