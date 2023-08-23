pub struct Sharder {
    n_shards: u32,
    pos: u32,
}

impl Sharder {
    pub fn new(pos: u32, n_shards: u32) -> Self {
        assert!(n_shards > 0);
        assert!(pos < n_shards, "out of range");

        Self { n_shards, pos }
    }

    pub fn matching(&self, hash: u64) -> bool {
        (hash % self.n_shards as u64) as u32 == self.pos
    }
}

#[cfg(test)]
mod tests {
    use crate::outbound::sharder::Sharder;

    #[test]
    fn shard() {
        let sharder = Sharder::new(3, 10);

        assert!(sharder.matching(13));
        assert!(sharder.matching(23));
        assert!(sharder.matching(33));
        assert!(!sharder.matching(31));
    }
}
