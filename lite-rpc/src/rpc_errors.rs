#[derive(Copy, Clone, Debug, Eq, PartialEq)]
#[repr(i32)]
pub enum RpcErrors {
    // Account does not satisfy any account filters or account does not exists.
    AccountNotFound = 0,
}
