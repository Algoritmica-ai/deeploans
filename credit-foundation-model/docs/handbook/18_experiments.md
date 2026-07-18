# Part 18 — Experiments: Recording, Reproducibility, Registry

> The E-numbers you'll see in reports (E8, E10, E11…) index the project's experiment ledger;
> the durable record is `reports/` + each artifact's embedded lineage.

## 18.1 The recording philosophy: artifacts are the record

This repo doesn't rely on a separate experiment tracker to remember what happened — **every
artifact carries its own birth certificate**:

```
splits.meta.json      seed, source sha256, counts, origination ranges, git commit, full config
manifest.json         tokenizer path, vocab size, shard list, loan/token counts, full config
<ckpt>.pt             model config + FULL resolved run config + loss history
<ft>.pt / _meta.json  mode, LoRA params, task spec, metrics, base checkpoint, tokenizer
scores_manifest.json  checkpoint, cutoff, gate, score summary, finetune meta
calibrator.json       scores path, cutoff, method, n, base rate, Brier before/after
```

Given any file in the bucket, you can answer "what produced this, from what, with what
settings?" without leaving the file. That's the property to preserve in anything you add: **new
stage ⇒ new manifest with `cfg.to_dict()` inside.**

## 18.2 Naming conventions

```
run_name       run_<data-era>[_<sample>]        run_2000_2022_10pct
               └ names a DATA RUN (split+encode lineage); every path derives from it via common.yaml
checkpoints    m_<size>[_<variant>].pt          m_100m.pt, m_100m_ft.pt, m5_full.pt
experiments    E<N> in the ledger; branch/PR per brick: feat/<thing>, exp/<thing>, docs/<thing>
reports        reports/<model>_<protocol>_<mode>.md      reports/m_100m_oot_ft_full.md
scratch        ALWAYS a distinct suffix: *_rr (rerun), *_tmp — never a production name
```

The `_rr` convention exists because of a real incident: a parity test without an explicit
`--checkpoint.out` overwrote a production backbone. Rule since then: **test runs always pass a
scratch output path.** Names are cheap; retraining a backbone is a GPU-day.

## 18.3 Versioning — three lineages that must move together

```
DATA lineage        run_name: which ingest sample + split + encode produced the shards
TOKENIZER lineage   tokenizer.json: FROZEN; ids are embedding indices — refit = new world
MODEL lineage       architecture config inside the checkpoint; shape changes = new pretrain
```

A checkpoint is only meaningful against *its* tokenizer and *its* data run — which is why all
three are recorded inside it. The package version (`credit_fm.__version__`, single-sourced with
`pyproject.toml`, enforced by test) versions the *code*; the three lineages version the
*artifacts*. Compatibility questions ("can I fine-tune ckpt X on split Y?") reduce to: same
tokenizer file? same contract? — both answerable from the embedded lineage.

## 18.4 Reproducibility: what's exact, what's statistical

Exact (bit-identical across reruns): hash sampling (which loans), the split assignment, the
tokenizer fit, the encoded shards, deterministic val masking, checkpoint resume (RNG states
restored).

Statistical (same distribution, not same bits): GPU training itself — bf16 atomics, dataloader
timing, DDP reduction order. Hence the honest claim format used in this repo: *"ROC in the
0.84x band, comfortably above 0.8257"*, not "0.8468 to four decimals," when re-running E11.
Seeds are set everywhere (`set_seed(cfg.seed)`, per-rank offsets under DDP) to narrow the noise,
not to deny it.

**The reproduction protocol** (how a rerun of the headline experiment is done): fresh `_rr` paths for every
artifact, same recipe/seed/effective batch, per-stage validators must pass, judge the endpoint as
a band. Any framework change in between is regression-tested *by* the rerun.

## 18.5 Checkpoint management

```
during training   <out>.step<N>.pt every 1000 steps, keep:2 rotation (crash insurance, ~complete state)
end of training   best-val weights RESTORED, single <out>.pt written by rank 0
fine-tune         separate file (m_100m_ft.pt) — NEVER overwrite the backbone (many tasks share it)
hygiene           step files are disposable after the final ckpt lands; backbones + ft ckpts with
                  reported numbers are permanent; anything named *_rr/_tmp is deletable on sight
```

## 18.6 Model registry (current practice and its edges)

There is deliberately **no registry service** — the bucket layout + embedded metadata *is* the
registry: `gs://…/runs/` holds checkpoints whose identity is self-describing; `reports/` holds
the certified numbers; `models/` + `publish_model.py` package release candidates; model/data
cards in `docs/` document them. `serve.py`'s `/health` endpoint closes the loop — a deployed
instance reports exactly which checkpoint+calibrator it runs. When the count of live models
grows past a handful, the upgrade path is an index file over the same metadata (the manifests
already contain everything a registry row needs) — not a new source of truth.

## 18.7 The experiment ladder as a worked example of method

E8→E12 is the house style for answering a question with minimum GPU spend: establish a champion
(E8: 26M/4%); change ONE axis (E10: params only → flat 0.8223 — a cheap, decisive negative);
change the other (E12: data only → 0.8406, ~71% of the eventual gain); then both (E11: 0.8468)
— and *decompose the attribution* in the report. One-variable-at-a-time isn't bureaucracy; it's
what let "feed the model before you grow it" become a defensible engineering rule (DL-015).

### Things to remember

1. Artifacts are the experiment record — every file carries its full config, inputs, and commit.
2. Three lineages move together: data run (run_name), tokenizer (frozen), model shape (in the checkpoint).
3. GPU training reproduces as a band, not bits — claim bands; everything upstream reproduces exactly.
4. Scratch names (`_rr`, `_tmp`) always; never overwrite a backbone (many tasks share it).
5. Change one variable at a time — the E8→E12 ladder is the house method.

---
*Next: [Part 19 — Developer Guide](19_developer_guide.md): hands on the keyboard.*
