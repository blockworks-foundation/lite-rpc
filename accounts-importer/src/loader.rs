// use std::io::Load;
// use std::path::Path;
//
// pub trait LoadProgressTracking {
//     fn new_load_progress_tracker(
//         &self,
//         path: &Path,
//         rd: Box<dyn Load>,
//         file_len: u64,
//     ) -> SnapshotResult<Box<dyn Load>>;
// }
//
// struct NoopLoadProgressTracking {}
//
// impl LoadProgressTracking for NoopLoadProgressTracking {
//     fn new_load_progress_tracker(
//         &self,
//         _path: &Path,
//         rd: Box<dyn Load>,
//         _file_len: u64,
//     ) -> SnapshotResult<Box<dyn Load>> {
//         Ok(rd)
//     }
// }
