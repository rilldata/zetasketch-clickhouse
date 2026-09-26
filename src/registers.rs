//! The HyperLogLog++ register array shared by both formats, plus precision
//! folding.

use anyhow::{bail, ensure, Result};

/// Largest precision any format in scope uses: ZetaSketch caps its normal
/// precision here (`NormalRepresentation.MAXIMUM_PRECISION`), and ClickHouse's
/// `uniqCombined64` is only instantiated up to `K = 20`. Bounding construction
/// also keeps a corrupt precision field from requesting an absurd allocation.
pub const MAX_PRECISION: u8 = 24;

/// A dense HyperLogLog++ register array.
///
/// `data[i]` holds the *rhoW* of bucket `i` — the number of leading zero bits
/// plus one in the hashed value that landed there, capped at
/// `64 - precision + 1` — or `0` if nothing ever landed in that bucket.
/// Both ZetaSketch's `NORMAL` representation and ClickHouse's `LARGE`
/// container are encodings of exactly this array, which is why a sketch can be
/// moved between them without access to the original values.
///
/// Invariant: `data.len() == 1 << precision`. The fields are public for
/// convenience; [`Registers::new`] checks the invariant, and every method and
/// encoder in this crate re-checks it rather than trusting it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Registers {
    pub precision: u8,
    pub data: Vec<u8>,
}

impl Registers {
    /// Builds a register array, validating the length/precision invariant.
    pub fn new(precision: u8, data: Vec<u8>) -> Result<Self> {
        check_precision(precision)?;
        let expected = 1usize << precision;
        ensure!(
            data.len() == expected,
            "register array has {} entries, expected 2^{precision} = {expected}",
            data.len()
        );
        Ok(Self { precision, data })
    }

    /// An all-zero (never-updated) register array at `precision`.
    pub fn empty(precision: u8) -> Result<Self> {
        check_precision(precision)?;
        Ok(Self {
            precision,
            data: vec![0u8; 1usize << precision],
        })
    }

    /// Number of buckets, `2^precision`.
    pub fn bucket_count(&self) -> usize {
        1usize << self.precision
    }

    /// Buckets that were never updated. This is the `zeros` counter ClickHouse
    /// stores alongside the registers, and what linear counting corrects with.
    pub fn zero_count(&self) -> usize {
        self.data.iter().filter(|&&r| r == 0).count()
    }

    /// The largest rhoW representable at this precision: a 64-bit hash spends
    /// `precision` bits on the bucket index, leaving `64 - precision` bits
    /// whose all-zero case yields `64 - precision + 1`.
    pub fn max_rank(&self) -> u32 {
        64 - u32::from(self.precision) + 1
    }

    pub(crate) fn check_invariant(&self) -> Result<()> {
        check_precision(self.precision)?;
        let expected = 1usize << self.precision;
        ensure!(
            self.data.len() == expected,
            "register array has {} entries, expected 2^{} = {expected}",
            self.data.len(),
            self.precision
        );
        Ok(())
    }

    /// Folds down to a coarser precision, keeping the maximum rhoW of each
    /// group of `2^(self.precision - target_precision)` source buckets.
    ///
    /// This is the standard HLL++ precision downgrade: the target bucket index
    /// is the source index with its low bits dropped, and the rhoW is adjusted
    /// for those dropped bits (see [`downgrade_rho_w`]). Folding *up* is not
    /// possible — the information was never in the sketch.
    ///
    /// Ports `NormalRepresentation.mergeNormalDataMaybeDowngrading`
    /// (`NormalRepresentation.java:206-244`).
    pub fn fold_to(&self, target_precision: u8) -> Result<Registers> {
        self.check_invariant()?;

        if target_precision == self.precision {
            return Ok(self.clone());
        }
        if target_precision > self.precision {
            bail!(
                "cannot fold precision {} up to {target_precision}: the extra buckets were never in the sketch",
                self.precision
            );
        }
        ensure!(target_precision >= 1, "target precision must be >= 1");

        let source_p = u32::from(self.precision);
        let target_p = u32::from(target_precision);
        let shift = source_p - target_p;
        let mut out = vec![0u8; 1usize << target_precision];

        for (old_index, &old_rho_w) in self.data.iter().enumerate() {
            let new_index = old_index >> shift;
            let new_rho_w = downgrade_rho_w(old_index as u32, old_rho_w, source_p, target_p);
            if new_rho_w > out[new_index] {
                out[new_index] = new_rho_w;
            }
        }

        Ok(Registers {
            precision: target_precision,
            data: out,
        })
    }
}

pub(crate) fn check_precision(precision: u8) -> Result<()> {
    ensure!(
        (1..=MAX_PRECISION).contains(&precision),
        "precision {precision} out of range [1, {MAX_PRECISION}]"
    );
    Ok(())
}

/// Recomputes a rhoW for a coarser precision.
///
/// Dropping `source_p - target_p` index bits moves those bits to the front of
/// the region rhoW measures. If they contain a set bit, that bit determines the
/// new rhoW outright and the old value is irrelevant; if they are all zero, the
/// leading-zero run simply grew by that many bits.
///
/// Ports `Encoding.downgradeRhoW` (`Encoding.java:485-508`) plus the
/// "0 means unset, keep it unset" guard from `Encoding.Normal.downgradeRhoW`
/// (`Encoding.java:100-107`).
pub fn downgrade_rho_w(index: u32, rho_w: u8, source_p: u32, target_p: u32) -> u8 {
    if rho_w == 0 {
        return 0;
    }
    if source_p == target_p {
        return rho_w;
    }
    // `wrapping_shl` masks the shift to 5 bits exactly like Java's `int <<`.
    let suffix = index.wrapping_shl(32 - source_p + target_p);
    if suffix == 0 {
        (u32::from(rho_w) + source_p - target_p) as u8
    } else {
        (1 + suffix.leading_zeros()) as u8
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn new_rejects_length_precision_mismatch() {
        assert!(Registers::new(4, vec![0; 16]).is_ok());
        assert!(Registers::new(4, vec![0; 15]).is_err());
    }

    #[test]
    fn empty_is_all_zero_and_sized_by_precision() {
        let regs = Registers::empty(15).unwrap();
        assert_eq!(regs.bucket_count(), 32768);
        assert_eq!(regs.zero_count(), 32768);
        assert_eq!(regs.max_rank(), 50);
    }

    #[test]
    fn fold_to_same_precision_is_identity() {
        let regs = Registers::new(12, vec![3u8; 1 << 12]).unwrap();
        assert_eq!(regs.fold_to(12).unwrap(), regs);
    }

    #[test]
    fn fold_to_rejects_upsampling() {
        let regs = Registers::new(12, vec![0; 1 << 12]).unwrap();
        assert!(regs.fold_to(15).is_err());
    }

    #[test]
    fn fold_to_rejects_malformed_registers() {
        // Public fields let callers build an inconsistent value; folding it
        // must report the problem rather than index out of bounds.
        let bad = Registers {
            precision: 15,
            data: vec![0; 999],
        };
        assert!(bad.fold_to(12).is_err());
    }

    #[test]
    fn fold_to_keeps_group_max_and_preserves_unset_buckets() {
        // precision 16 -> 15 folds buckets {2i, 2i+1} into bucket i.
        let mut data = vec![0u8; 1 << 16];
        data[0] = 5; // index 0: dropped bit is 0, so rhoW grows by 1 -> 6
        data[1] = 9; // index 1: dropped bit is 1, so rhoW becomes 1 regardless
        // buckets 2 and 3 stay unset.
        let regs = Registers::new(16, data).unwrap();

        let folded = regs.fold_to(15).unwrap();
        assert_eq!(folded.precision, 15);
        assert_eq!(folded.data.len(), 1 << 15);
        assert_eq!(folded.data[0], 6, "max(downgrade(0,5), downgrade(1,9)) = max(6,1)");
        assert_eq!(folded.data[1], 0, "unset source buckets must stay unset");
    }

    #[test]
    fn downgrade_rho_w_matches_hand_computed_cases() {
        assert_eq!(downgrade_rho_w(0, 0, 16, 15), 0, "unset stays unset");
        assert_eq!(downgrade_rho_w(7, 4, 15, 15), 4, "same precision is identity");
        assert_eq!(downgrade_rho_w(0, 5, 16, 15), 6, "all-zero dropped bits extend the run");
        assert_eq!(downgrade_rho_w(1, 9, 16, 15), 1, "set dropped bit resets the run");
        assert_eq!(downgrade_rho_w(0b10, 9, 17, 15), 1, "highest dropped bit set -> 1");
        assert_eq!(downgrade_rho_w(0b01, 9, 17, 15), 2, "second dropped bit set -> 2");
        // Worst case must stay inside the target precision's max_rank.
        assert_eq!(downgrade_rho_w(0, 41, 24, 15), 50);
    }
}
