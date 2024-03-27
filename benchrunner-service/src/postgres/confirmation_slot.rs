use std::time::SystemTime;

#[derive(Debug)]
pub struct PostgresConfirmationSlot {
    pub signature: String,
    pub bench_datetime: SystemTime,
    pub slot_sent: u64,
    pub slot_confirmed: u64,
    pub endpoint: String,
    pub confirmed: bool,
    pub confirmation_time_ms: f32,
}

// impl PostgresConfirmationSlot {
//     pub fn to_values() -> &[&(dyn ToSql + Sync)] {
//         let values: &[&(dyn ToSql + Sync)] = &[
//             &self.signature,
//             &self.bench_datetime,
//             &(self.slot_sent as i64),
//             &(self.slot_confirmed as i64),
//             &self.endpoint,
//             &self.confirmed,
//             &self.confirmation_time_ms,
//         ];
//         values
//     }
// }
