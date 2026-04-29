# Minimum-Viable Organist Parity Feature Set

Feature roadmap for bringing rusty-pipes to a level where an organist can perform
standard repertoire on a real MIDI console without feeling crippled. Grounded in
the current `organ.rs` model (flat `stops`/`ranks` with `division_id` on ranks
only).

This is the work to do on **desktop first**, before porting to mobile (Flutter).
The engine-extraction refactor (see `PLAN_OPTION_A_RUST_OWNED_AUDIO.md`) should
happen alongside or just before these features so they land in the shared
engine crate from day one.

---

## 1. Manuals & Divisions (foundation for everything else) — ✅ DONE

**Status:** Shipped 2026-04-19.

**Delivered:**
- `Division { id, name, midi_channel }` promoted to first-class struct in `src/organ.rs`
  (the `coupled_to` field is deferred with Feature #2).
- `Stop.division_id` populated by:
  - Hauptwerk: majority vote across the stop's ranks (`src/organ_hauptwerk.rs`).
  - GrandOrgue: `[Manual###]` section parsing with manual→stop ownership
    (`src/organ_grandorgue.rs`); falls back to synthetic "default" division on
    older ODFs.
- Per-division MIDI channel routing in `src/app_state.rs`:
  `active_stops: BTreeSet<usize>` + `division_channels: HashMap<String, u8>`
  replaced the flat `stop_channels` map for key dispatch. A legacy shim keeps
  the per-stop channel UI readable until it's retired.
- Persistence: `MidiControlMap.divisions` field with one-shot majority-vote
  migration of existing `*.midi_map.json` on load (`src/midi_control.rs`).
- REST: `GET /divisions`, `PUT /divisions/{id}/channel`, plus populated
  `division` field on `StopStatusResponse` (`src/api_rest.rs`).
- Held-note handoff when rebinding a division's channel (no stuck notes).

**Follow-ups (not blocking next feature):**
- UI: per-stop channel-number grid is now legacy; replace with a per-division
  channel picker.
- Consider filtering Hauptwerk "Key action *" noise-switch stops from the main
  stop grid (move to a dedicated "Mechanical noises" section).

---

## 2. Couplers

**MVP (the 6 that cover ~90% of repertoire):**
- Swell → Great (8′)
- Great → Pedal (8′)
- Swell → Pedal (8′)
- Swell → Great 16′ (sub-octave)
- Swell → Great 4′ (super-octave)
- Swell unison off

**Implementation:** coupler = a function from `(division, note, velocity)` →
list of additional `(division, note, velocity)` to trigger. Applied in the MIDI
dispatch layer *before* voice allocation. Octave couplers transpose ±12. Handle
note-off symmetrically. Watch for feedback loops (A→B and B→A both on).

**Data model:**
```rust
struct Coupler {
    from: DivisionId,
    to: DivisionId,
    interval_semitones: i8,
    enabled: bool,
}
```

---

## 3. Combinations & Pistons

**MVP:**
- **8 general combinations** (store/recall full stop + coupler state across all divisions)
- **Set/Memory mode**: hold SET piston + press combination piston → saves current state
- **Next/Previous piston**: steps through generals 1→8
- **General Cancel**: all stops off, all couplers off
- **Tutti** (optional but cheap): one saved "all stops" state
- Persist combinations to disk per organ (same pattern as existing per-organ MIDI mappings)

**Skip for MVP:** divisional pistons (per-division combinations), reversibles,
crescendo pedal, combination sequencer/frames. Organists can work without these.

**Data model:**
```rust
struct Combination {
    active_stops: HashSet<StopId>,
    active_couplers: HashSet<CouplerId>,
    active_tremulants: HashSet<TremulantId>,
}
struct CombinationBank {
    generals: [Combination; 8],
    tutti: Option<Combination>,
}
```

MIDI-learnable piston triggers (reuse existing MIDI-learn infrastructure).

---

## 4. Expression / Swell Pedal

**MVP:**
- Per-division `expression` level 0.0–1.0 (unenclosed divisions like Great/Pedal: fixed 1.0)
- MIDI CC input (CC 11 default, learnable) → per-division expression
- Apply as simple amplitude scalar in the mixer (multiply voice output by expression level of the voice's division)

**Skip:** shutter acoustic filtering (low-pass when shutters close) — nice but
not minimum. Simple gain is acceptable and matches GrandOrgue's default
behavior on many sets.

---

## 5. Transpose

**MVP:** global semitone transpose (-11 to +11). Applied at MIDI input by
shifting note numbers before voice lookup. One line of code; high value for
accompanists.

**Skip:** per-division transpose, pitch-bend, fine tuning UI. Global only.

---

## 6. All-Notes-Off / Panic

**MVP:** dedicated panic button + MIDI CC 123 handling. Already partially
present; ensure it cuts *all* active voices across all divisions cleanly
including coupler-generated ones.

---

## Explicitly Deferred (post-MVP)

| Feature | Why deferred |
|---|---|
| Temperaments / historical tuning | Needs per-pipe cent table + UI; real organists mostly play equal-tempered on digital |
| Crescendo pedal | Combination-sequencer logic on top of CC; complex UI |
| Divisional pistons | 3–5× the storage and UI of generals; generals cover most use |
| Reversible pistons | Toggle-specific, mostly for couplers — couplers being directly toggleable covers it |
| Split manuals | Edge-case (some organ literature, hymn reharmonization) |
| Combination sequencer / frames | Pro feature; advanced organists only |
| Shutter acoustic modeling | Gain-based expression is "good enough" |
| Second-touch / general cancel variations | Niche |
| Pipe-level voicing edits | Hauptwerk Pro territory |

---

## Suggested Implementation Order

1. ~~**Division promotion**~~ ✅ done
2. ~~**Per-division MIDI routing**~~ ✅ done
3. **Expression** (trivial once divisions exist; high user visibility) ← next
4. **Transpose** (trivial; high user value)
5. **Couplers** (the big one — touches MIDI dispatch + voice allocation)
6. **Combinations** (builds on everything above; mostly UI + state persistence)

Each step ships independently and is testable on desktop. Steps 1–2 are also
exactly what the engine-extraction refactor wants — killing two birds.

**Rough effort:** 2–4 weeks for a focused developer familiar with the codebase.
Couplers and combinations are ~60% of that.

---

## Relationship to Mobile Port

This feature work precedes the Flutter mobile port (see
`PLAN_OPTION_A_RUST_OWNED_AUDIO.md`). Rationale:

- These are *engine* features, not UI chrome — they belong in the core crate
  that both desktop and mobile will share.
- Faster iteration on desktop (no cross-compile, no device deploy).
- Existing desktop users benefit and shake out bugs before mobile lands.
- The engine-extraction refactor naturally absorbs the division model change.

Mobile-specific UI (touch-optimized pistons, on-screen swell slider, MIDI
routing UI) is deferred to the port phase.
