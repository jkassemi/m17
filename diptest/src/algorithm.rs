use std::cmp::Ordering;

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum EnvelopeKind {
    Minorant,
    Majorant,
}

#[derive(Clone, Copy)]
struct ConvexEnvelopeState {
    kind: EnvelopeKind,
    rel_length: usize,
    x: usize,
    y: usize,
}

impl ConvexEnvelopeState {
    fn new(kind: EnvelopeKind) -> Self {
        Self {
            kind,
            rel_length: 0,
            x: 0,
            y: 0,
        }
    }
}

#[derive(Clone, Copy)]
struct DipCandidate {
    value: f64,
    index: usize,
}

impl DipCandidate {
    fn new(value: f64, index: usize) -> Self {
        Self { value, index }
    }

    fn maybe_update(&mut self, other: DipCandidate) {
        if other.value > self.value {
            *self = other;
        }
    }

    fn maybe_update_value(&mut self, value: f64, index: usize) {
        if value > self.value {
            self.value = value;
            self.index = index;
        }
    }
}

#[derive(Debug, Clone)]
pub struct DipComputation {
    pub dip: f64,
    pub lo: usize,
    pub hi: usize,
    pub gcm_len: usize,
    pub lcm_len: usize,
    pub dip_index: Option<usize>,
    pub gcm: Vec<usize>,
    pub lcm: Vec<usize>,
}

pub fn is_sorted(data: &[f64]) -> bool {
    data.windows(2).all(|w| match w[0].partial_cmp(&w[1]) {
        Some(Ordering::Greater) => false,
        _ => true,
    })
}

pub fn dip_statistic_internal(data: &[f64], allow_zero: bool) -> DipComputation {
    debug_assert!(
        !data.is_empty(),
        "dip_statistic_internal expects a non-empty slice"
    );
    let n = data.len();
    let mut x = Vec::with_capacity(n + 1);
    x.push(0.0);
    x.extend_from_slice(data);

    let mut gcm = vec![0usize; n + 3];
    let mut lcm = vec![0usize; n + 3];
    let mut mn = vec![0usize; n + 3];
    let mut mj = vec![0usize; n + 3];

    let mut low = 1usize;
    let mut high = n;
    let mut dip = if allow_zero { 0.0 } else { 1.0 };
    let mut dip_idx: Option<usize> = None;

    if n < 2 || x[n] == x[1] {
        let denom = (2.0 * n as f64).max(1.0);
        return DipComputation {
            dip: dip / denom,
            lo: low,
            hi: high,
            gcm_len: 0,
            lcm_len: 0,
            dip_index: dip_idx,
            gcm,
            lcm,
        };
    }

    build_indices(&x, &mut mn, EnvelopeKind::Minorant);
    build_indices(&x, &mut mj, EnvelopeKind::Majorant);

    let mut gcm_state = ConvexEnvelopeState::new(EnvelopeKind::Minorant);
    let mut lcm_state = ConvexEnvelopeState::new(EnvelopeKind::Majorant);

    loop {
        gcm[1] = high;
        let mut i = 1usize;
        while gcm[i] > low {
            gcm[i + 1] = mn[gcm[i]];
            i += 1;
        }
        gcm_state.rel_length = i;
        gcm_state.x = i;
        gcm_state.y = gcm_state.x.saturating_sub(1);
        if gcm_state.y < 1 {
            gcm_state.y = 1;
        }

        lcm[1] = low;
        let mut j = 1usize;
        while lcm[j] < high {
            lcm[j + 1] = mj[lcm[j]];
            j += 1;
        }
        lcm_state.rel_length = j;
        lcm_state.x = j;
        lcm_state.y = if j >= 2 { 2 } else { 1 };

        let d = if gcm_state.rel_length != 2 || lcm_state.rel_length != 2 {
            max_distance(&x, &gcm, &lcm, &mut gcm_state, &mut lcm_state)
        } else if allow_zero {
            0.0
        } else {
            1.0
        };

        if d < dip {
            break;
        }

        let dip_l = compute_envelope_dip(&x, &gcm, &gcm_state);
        let dip_u = compute_envelope_dip(&x, &lcm, &lcm_state);
        let chosen = if dip_l.value < dip_u.value {
            dip_u
        } else {
            dip_l
        };

        if dip < chosen.value {
            dip = chosen.value;
            if chosen.index != usize::MAX {
                dip_idx = Some(chosen.index);
            }
        }

        let flag = low == gcm[gcm_state.x] && high == lcm[lcm_state.x];
        low = gcm[gcm_state.x];
        high = lcm[lcm_state.x];
        if flag {
            break;
        }
    }

    let final_dip = dip / (2.0 * n as f64);
    DipComputation {
        dip: final_dip,
        lo: low,
        hi: high,
        gcm_len: gcm_state.rel_length,
        lcm_len: lcm_state.rel_length,
        dip_index: dip_idx,
        gcm,
        lcm,
    }
}

fn build_indices(arr: &[f64], indices: &mut [usize], kind: EnvelopeKind) {
    let size = arr.len() - 1;
    if size == 0 {
        return;
    }
    let offset: isize = match kind {
        EnvelopeKind::Minorant => 1,
        EnvelopeKind::Majorant => -1,
    };
    let start: isize = match kind {
        EnvelopeKind::Minorant => 1,
        EnvelopeKind::Majorant => size as isize,
    };
    let end: isize = size as isize + 1 - start;
    indices[start as usize] = start as usize;

    let mut i = start + offset;
    while (offset > 0 && i <= end) || (offset < 0 && i >= end) {
        let idx = i as usize;
        let prev = (i - offset) as usize;
        indices[idx] = prev;

        loop {
            let ind_at_i = indices[idx];
            let ind_prev = indices[ind_at_i];

            let lhs = (arr[idx] - arr[ind_at_i]) * ((ind_at_i as isize - ind_prev as isize) as f64);
            let rhs = (arr[ind_at_i] - arr[ind_prev]) * ((idx as isize - ind_at_i as isize) as f64);

            if ind_at_i == start as usize || lhs < rhs {
                break;
            }
            indices[idx] = ind_prev;
        }

        i += offset;
    }
}

fn compute_envelope_dip(
    arr: &[f64],
    optimum: &[usize],
    state: &ConvexEnvelopeState,
) -> DipCandidate {
    let offset = match state.kind {
        EnvelopeKind::Minorant => 0usize,
        EnvelopeKind::Majorant => 1usize,
    };
    let sign = if matches!(state.kind, EnvelopeKind::Minorant) {
        1.0
    } else {
        -1.0
    };

    let mut ret = DipCandidate::new(0.0, usize::MAX);
    let mut tmp = DipCandidate::new(1.0, usize::MAX);

    if state.rel_length <= 1 {
        return ret;
    }

    for j in state.x..state.rel_length {
        let j_start = optimum[j + 1 - offset];
        let j_end = optimum[j + offset];
        if j_end > j_start + 1 && arr[j_end] != arr[j_start] {
            let numerator = (j_end - j_start) as f64;
            let denom = arr[j_end] - arr[j_start];
            let coeff = numerator / denom;

            for jj in j_start..=j_end {
                let delta = (jj as isize - j_start as isize) as f64 + sign;
                let d = sign * (delta - (arr[jj] - arr[j_start]) * coeff);
                tmp.maybe_update_value(d, jj);
            }
        }
        ret.maybe_update(tmp);
        tmp = DipCandidate::new(1.0, usize::MAX);
    }
    ret
}

fn max_distance(
    arr: &[f64],
    gcm_optimum: &[usize],
    lcm_optimum: &[usize],
    gcm_state: &mut ConvexEnvelopeState,
    lcm_state: &mut ConvexEnvelopeState,
) -> f64 {
    debug_assert!(matches!(gcm_state.kind, EnvelopeKind::Minorant));
    debug_assert!(matches!(lcm_state.kind, EnvelopeKind::Majorant));

    let mut ret = 0.0f64;

    loop {
        let gcm_y = gcm_optimum[gcm_state.y];
        let lcm_y = lcm_optimum[lcm_state.y];
        let is_maj = gcm_y > lcm_y;

        let (i, j) = if is_maj {
            (gcm_y, lcm_y)
        } else {
            (lcm_y, gcm_y)
        };
        let i1 = if is_maj {
            gcm_optimum[gcm_state.y + 1]
        } else {
            lcm_optimum[lcm_state.y - 1]
        };
        let sign = if is_maj { 1.0 } else { -1.0 };

        let numerator = j as f64 - i1 as f64 + sign;
        let denom = arr[i] - arr[i1];
        let dx = sign * (numerator - ((arr[j] - arr[i1]) * (i as f64 - i1 as f64) / denom));

        if is_maj {
            lcm_state.y += 1;
        } else {
            gcm_state.y = gcm_state.y.saturating_sub(1);
        }

        if dx >= ret {
            ret = dx;
            gcm_state.x = gcm_state.y + 1;
            lcm_state.x = lcm_state.y - if is_maj { 1 } else { 0 };
        }

        if gcm_state.y < 1 {
            gcm_state.y = 1;
        }
        if lcm_state.y > lcm_state.rel_length {
            lcm_state.y = lcm_state.rel_length;
        }

        if gcm_optimum[gcm_state.y] == lcm_optimum[lcm_state.y] {
            break;
        }
    }

    ret
}
