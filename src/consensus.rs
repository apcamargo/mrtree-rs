use crate::model::{EffectiveTable, LabelMatrix, Path};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ConsensusOptions {
    pub sample_weighting: bool,
    pub seed: u64,
}

#[derive(Debug, Clone)]
pub struct ConsensusReduction {
    inner: crate::algorithm::consensus::ConsensusState,
}

impl ConsensusReduction {
    #[must_use]
    pub fn reduced_labels(&self) -> &LabelMatrix {
        self.inner.labels()
    }

    #[must_use]
    pub fn group_mapping(&self) -> &[usize] {
        self.inner.group_mapping()
    }

    pub fn expand_paths(&self, reduced_paths: &[Path]) -> crate::Result<Vec<Path>> {
        self.inner.expand_paths(reduced_paths)
    }
}

pub fn reduce_same_k_groups(
    effective: &EffectiveTable,
    options: &ConsensusOptions,
) -> crate::Result<ConsensusReduction> {
    Ok(ConsensusReduction {
        inner: crate::algorithm::consensus::reduce_same_k_groups(
            effective,
            options.sample_weighting,
            options.seed,
        )?,
    })
}
