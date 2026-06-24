//! Identifier newtypes.

/// Internal, monotonically-assigned object id. Used as the needle key, so the
/// (possibly long) S3 object key never enters the compact in-RAM index.
pub type ObjectId = u64;

/// Identifies one volume (append-only container) file within a node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct VolumeId(pub u32);

impl VolumeId {
    /// The numeric value.
    #[inline]
    pub fn get(self) -> u32 {
        self.0
    }
}

impl std::fmt::Display for VolumeId {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        // Zero-padded so volume files sort lexicographically by id.
        write!(f, "{:010}", self.0)
    }
}
