pub enum Uniqueness {
    ExactlyOne,
    Multiple(usize),
    Empty,
}

impl Uniqueness {
    pub fn inspect_len(len: usize) -> Uniqueness {
        if len == 1 {
            Uniqueness::ExactlyOne
        } else if len == 0 {
            Uniqueness::Empty
        } else {
            Uniqueness::Multiple(len)
        }
    }
}
