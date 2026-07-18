# Part 20 — Glossary

Format per entry: **Simple** (plain English) · **Technical** · **Example** (from this repo where
possible) · **⚠** (common mistake).

## Machine-learning terms

**Tensor** — Simple: a grid of numbers (list, table, or cube of them). Technical: n-dimensional
array with dtype/device; PyTorch's core type, supporting autograd. Example: a batch here is
`input_ids (B, L)` of int64; embeddings are `(B, L, 768)` bfloat16. ⚠ Shape bugs are 90% of
beginner errors — print `.shape` early and often.

**Parameter (weight)** — Simple: one adjustable number inside the model; training = adjusting
them all. Technical: `nn.Parameter` tensors updated by the optimizer. Example: 26M vs 100.9M
parameters are our two configs. ⚠ Confusing with *hyper*parameters (below).

**Hyperparameter** — Simple: a setting *you* choose (the model doesn't learn it). Technical:
architecture/optimization knobs outside gradient descent. Example: `dim: 768`, `lr: 3e-4`,
`n_bins: 16`. ⚠ Tuning them on the test set — that's selection leakage.

**Feature** — Simple: one input fact about an example. Technical: a column (tabular) or derived
input dimension. Example: `original_ltv` is a feature; `current_loan_delinquency_status` is a
*banned* one (leakage). ⚠ Features that encode the outcome.

**Embedding** — Simple: the model's numeric summary of a thing; similar things get nearby
summaries. Technical: dense learned vector. Example: each of 552 tokens has a 768-dim row; each
loan gets one `[USR]` vector. ⚠ Reading individual coordinates as meaningful — only the whole
vector is.

**Hidden state / hidden features** — Simple: the model's intermediate "thoughts" between input
and output. Technical: activations at intermediate layers, `(B, L, dim)` here. Example: the
Event encoder's token states become the MLM head's "local" context. ⚠ Expecting them to be
individually interpretable.

**Token** — Simple: one symbol from the model's alphabet. Technical: an integer id into a fixed
vocabulary. Example: `original_ltv=5`, `cal=2008Q4`, `[MASK]` — 552 total here. ⚠ Assuming
tokens = words; here they're `field=value` atoms.

**Vocabulary** — Simple: the complete alphabet. Technical: bijection token-string ↔ id;
serialized in `tokenizer.json`; frozen after fit. ⚠ Refitting it invalidates every checkpoint.

**Batch** — Simple: how many examples the model chews at once. Technical: leading tensor dim B;
here loans padded to the batch max length. Example: micro-batch 64 × grad-accum 4 × 8 GPUs =
effective 2048. ⚠ Thinking bigger batch = always better — LR and batch interact.

**Epoch vs step** — Simple: epoch = one pass over all data; step = one weight update. Example:
pretraining thinks in steps (20,000 over a cycled corpus); fine-tuning in epochs (~4–8). ⚠
Comparing runs by epochs when batch sizes differ — compare tokens/steps.

**Gradient** — Simple: for each parameter, which direction (and how strongly) to nudge it to
reduce error. Technical: ∂loss/∂θ via backprop; `loss.backward()` fills `.grad`. ⚠ Forgetting
`zero_grad` → gradients silently accumulate (we *use* that deliberately in grad-accum).

**Backpropagation** — Simple: the bookkeeping that assigns blame for the error backward through
every layer. Technical: reverse-mode autodiff over the recorded compute graph. ⚠ It's ~2× the
forward cost and needs the forward activations — that's where OOMs come from.

**Loss** — Simple: one number saying "how wrong was that?" Technical: differentiable objective;
here cross-entropy (MLM over 552 classes; 2-class weighted in fine-tune). Example: random ≈
ln 552 ≈ 6.3; our train end ≈ 0.14. ⚠ Judging a rebalanced fine-tune by its fit loss.

**Cross-entropy** — Simple: penalty = −log(probability the model gave the right answer).
Technical: `F.cross_entropy(logits, labels, ignore_index=-100)`. ⚠ Forgetting the ignore index
grades padding and unmasked positions.

**Softmax** — Simple: turns raw scores into positive numbers summing to 1. Technical:
`exp(zᵢ)/Σexp(zⱼ)`. Example: `model.classify(batch).softmax(-1)[:, 1]` = the default score. ⚠
Treating its output as a *calibrated* probability (see Calibration).

**Optimizer / AdamW** — Simple: the rule that turns gradients into weight changes. Technical:
Adam = per-parameter steps scaled by running gradient moments (β 0.9/0.95 here); W = decoupled
weight decay. ⚠ Applying weight decay to norms/biases/embeddings (we exempt them).

**Learning rate / warmup / cosine schedule** — Simple: step size; start gentle; glide down.
Example: 3e-4 pretrain, 2e-5 full fine-tune, 1000-step warmup, decay to 10%. ⚠ Full-FT at
pretrain LR = catastrophic forgetting.

**Gradient clipping** — Simple: cap on any single update's violence. Technical: rescale grads
when global norm > 1.0. ⚠ Clipping *every* step hard masks a too-high LR.

**Gradient accumulation** — Simple: save up several small batches' gradients, apply once — big-
batch math at small-batch memory. Example: 64×4 = effective 256; how the 100M model fit one
H100. ⚠ Forgetting to divide the loss by the accumulation count.

**Mixed precision / bf16** — Simple: compute in half-size numbers for speed, keep masters
precise. Technical: bfloat16 autocast (fp32 range, no loss scaling — unlike fp16). ⚠ Comparing
losses across precision modes to the 4th decimal.

**Dropout** — Simple: randomly silence parts of the network in training so it can't over-rely on
any one path. Technical: zero activations with prob p (0.1 pretrain); auto-off in `eval()`. ⚠
Scoring with a model left in train mode → nondeterministic outputs.

**Overfitting / regularization** — Simple: memorizing the textbook vs learning the subject;
regularization = habits that prevent it (dropout, weight decay, more data). Example: val loss
rising while train falls; the monitoring split makes it visible epoch 1. ⚠ "More epochs is
free" — it isn't; we restore the *best* epoch, not the last.

**Attention / Multi-head attention** — see Part 10. Simple: every token votes on which other
tokens matter to it. ⚠ Forgetting attention is order-blind without positional encoding.

**Transformer / encoder-only / decoder-only** — Simple: the attention-based architecture;
encoder reads everything at once (BERT, us), decoder reads left-to-right and generates (GPT). ⚠
Calling this model "a GPT for loans" — wrong half of the family.

**RoPE** — Simple: encodes *where* a token is by rotating its vectors; distance survives as
angle. ⚠ Requires even head_dim (the constructor enforces it).

**RMSNorm / residual connection / SwiGLU** — see Part 10.4. ⚠ Removing norms/residuals "to
simplify" — deep stacks stop training.

**MLM (masked-language modelling)** — Simple: fill-in-the-blanks as a training goal. Example:
our three-source masking (15% tokens / 10% whole months / 10% whole fields), BERT 80/10/10
corruption. ⚠ Masking specials or letting "[MASK] present" be a learnable shortcut.

**Foundation model / pretraining / fine-tuning / transfer learning / representation learning**
— see Part 1.4. ⚠ The classic confusion: pretraining has no task labels; fine-tuning has.

**LoRA** — Simple: instead of re-carving the statue, add small clay patches. Technical: frozen W
plus trainable low-rank B·A (rank 4–8 here), B zero-init; ~1–2% params. Example: 0.8068 OOT vs
full's 0.8257. ⚠ Loading a LoRA checkpoint without re-inserting adapters first.

**Checkpoint** — Simple: a saved snapshot of the model (and here, of the whole training state).
Example: `m_100m.pt`; step files carry optimizer+RNG for exact resume. ⚠ Loading a checkpoint
into a differently-shaped model — always rebuild from `ckpt["config"]`.

**Inference** — Simple: using the trained model (no learning). Technical: `eval()` +
`torch.no_grad()`. ⚠ Leaving grad tracking on — 2× memory for nothing.

**ROC-AUC / PR-AUC / precision / recall / F1 / confusion matrix / Gini / lift** — see Part 14.
⚠ The cardinal one: quoting ROC alone at a 0.14% base rate.

**Calibration / Brier / reliability / isotonic / Platt** — see Parts 14–15. Simple: making
"2%" mean 2%. ⚠ Calibrating on the test window (our `calibrate.py` refuses).

**Class imbalance / negative sampling / pos_weight** — see Part 8.4. ⚠ Downsampling the *test*
set too — metrics must see the true base rate.

**Leakage** — Simple: the model peeking at the future or the answer. The four channels and four
counters: Part 8.1. ⚠ Believing a great backtest before checking for it.

**Loan-disjoint / out-of-time (OOT) / embargo / cutoff / horizon / gate / observation** — the
evaluation vocabulary: Parts 5.6, 8.2–8.3, 14.5.

## Financial terms

**Mortgage / UPB** — Simple: property-secured loan; UPB = unpaid principal balance (what's still
owed). Example: `current_actual_upb` is a core event field. ⚠ UPB ≠ property value (that's the
V in LTV).

**LTV (loan-to-value)** — Simple: loan ÷ property value; the borrower's skin in the game
inverted. Example: Ohio loan 87 → anchored bin (85.2, 90]. ⚠ Blurring the 80/90/95/97 cliffs
(why anchors exist).

**DTI (debt-to-income)** — Simple: monthly debt payments ÷ income. Anchors at 36/43/45
(qualified-mortgage cliffs).

**FICO / credit score** — Simple: a 300–850 summary of past borrowing behavior.

**Origination / vintage / seasoning** — Simple: the loan's birth / its birth-year cohort / its
age. Example: split key = origination; `t=` token = seasoning bin. ⚠ Confusing the two clocks
(origination vs reporting) — §5.3.

**Delinquency (DPD ladder) / cure** — Simple: payments behind (30/60/90…); cure = catching back
up. Example: `dlq_num`; D180 = default threshold; Ohio loan cured after 60. ⚠ `"XX"` = unknown
→ NA → every flag consumer needs `.fillna(False)`.

**Default (here)** — Technical: `dlq_num ≥ 6` (180 days) OR a credit-event zero-balance code
{02,03,09,15}. Base rate ~0.65% of loan-months pooled; ~0.14% at a 2022 observation. ⚠ "Default"
has many industry definitions — always state yours (we do, in the contract).

**Prepayment** — Simple: the loan ends early because it's paid off (refinance/sale). Technical:
ZBC 01; a *good* exit for credit risk, a cost for the investor's interest stream. Example: the
`prepay_12m` task (honest negative: ROC 0.626 — macro-rate-driven, weakly loan-specific). ⚠
Treating prepay as a failure event like default.

**Zero-balance code (ZBC)** — Simple: the "cause of death" code when a balance hits zero. Table
in §5.5. ⚠ Forgetting `str.zfill(2)` — "1" and "01" are the same code.

**Foreclosure / REO / short sale / deed-in-lieu** — Simple: the bank's recovery paths after
default (repossess-and-hold = REO; sell short = accept less than owed). All are credit events
here; their date/cost columns are leakage features.

**Servicer / GSE / Fannie Mae** — Simple: who collects payments / government-sponsored
enterprises that buy+securitize mortgages / the GSE whose public performance data we train on.

**PD / LGD / exposure** — Simple: probability of default / loss-given-default (share lost when
it happens) / amount at stake. Example: threshold math in §15.4: flag when `pd × LGD × exposure
> review_cost`. This repo models PD; LGD is future work.

**Panel data** — Simple: same entities, repeated monthly observations. ⚠ Treating rows as
independent samples — the root of split-by-row leakage.

**RMBS / ESMA Annex 2** — Simple: bonds backed by mortgage pools / the EU's standardized
loan-level disclosure schema (the Dutch validation panel's 71 columns).

## Engineering terms

**Parquet** — Simple: a compressed, columnar table file. Technical: column projection + row
groups = read only what you need. Example: every pipeline artifact. ⚠ Appending is impossible —
hence one-file-per-shard designs.

**Hive partitioning** — Simple: directory names carry column values (`reporting_year=2016/…`) so
readers skip irrelevant data. ⚠ Believing it groups *loans* — it groups time; loans scatter.

**Shard / manifest / sidecar** — Simple: one piece of a big dataset / the packing list / a tiny
marker file next to a shard. Example: `part-2016Q1.parquet` + `_meta-2016Q1.json`
(completion marker — written strictly after the shard = the resume mechanism). ⚠ Writing the
marker *before* the payload — you've built a lying resume system.

**Idempotent / resumable** — Simple: safe to run twice / continues where it stopped. Example:
rerunning ingest skips finished quarters; `--resume auto` continues pretraining. ⚠ Scripts that
append instead of overwrite are neither.

**Atomicity (here: sidecar-after-write)** — Simple: an operation either fully happened or
didn't, as judged by the marker. ⚠ GCS uploads are atomic; local writes are NOT — that's what
the sidecar ordering fixes.

**fsspec / gcsfs** — Simple: one file API for local disk and cloud buckets. Example:
`storage.py` — swap `gs://` for `s3://` and nothing else changes. ⚠ This container's pyarrow
lacks native GCS — always go through the storage helpers.

**DuckDB** — Simple: SQL engine that queries parquet in place (used in the baseline builder).

**Hash sampling / hash bucketing** — Simple: use a stable fingerprint of the id instead of a
random draw — reproducible, consistent everywhere. Example: `hash(loan_id)%100<10`;
streaming buckets `hash%K`. ⚠ Hashing a *row* instead of the entity shreds histories.

**Thread pool vs process pool** — Simple: many hands in one kitchen (share memory; good for
waiting-on-network) vs many kitchens (true parallel CPU). Example: ingest = threads; encode =
`spawn` processes. ⚠ `fork` after gRPC/gcsfs initialization deadlocks — this repo uses `spawn`.

**DDP / all-reduce / rank / world size / barrier** — Simple: N GPUs each train on 1/N of the
data and average their gradients every step; rank = which process you are; barrier = "everyone
waits here." Example: G4b, §12.9. ⚠ Bare `torchrun` on this box (venv loss); forgetting
`find_unused_parameters` with an idle head.

**DistributedSampler / set_epoch** — Simple: deals each GPU a different, reshuffled hand every
epoch. ⚠ Skipping `set_epoch` → identical shuffle every epoch.

**Collator / DataLoader / worker** — Simple: the batch assembler / the conveyor feeding batches
/ background processes that keep it full. Example: `MLMCollator` pads+masks; `num_workers: 4`.

**YAML / config engine / interpolation** — Part 16. ⚠ Unquoted `no`/`on` parse as booleans in
YAML — quote weird strings.

**CI (continuous integration)** — Simple: a robot that runs lint+tests (and here, builds+smokes
the wheel) on every PR. Example: `.github/workflows/ci.yml`'s `lint-and-test` + `package` jobs.

**Wheel / extras / editable install** — Simple: Python's installable package format / optional
dependency groups / "install as a link to my working copy." Example: `pip install -e
".[dev,gcs]"`; `[serving]` for FastAPI. ⚠ Heavy deps creeping into core (a test forbids it).

**SPDX header / Apache 2.0** — Simple: the one-line license stamp every file carries / the
permissive license the project ships under.

**Negative control** — Simple: prove your alarm rings by setting a fire on purpose. Example:
poison a split → validator must FAIL; calibrate on a test cutoff → must be REFUSED. ⚠ Skipping
these — an un-failable check is decoration. (If this handbook leaves you with one engineering
habit, make it this one.)

**Lineage / manifest-driven registry** — Simple: every artifact carries its full birth record
(config, inputs, git commit). Example: `torch.load(ckpt)["run_config"]`. ⚠ Any new artifact
without an embedded config breaks the property.

**FastAPI / endpoint / pydantic** — Simple: the web-service toolkit of `serve.py`; endpoints =
URLs that answer requests; pydantic validates request shapes. ⚠ `from __future__ import
annotations` breaks closure-local pydantic models (documented in serve.py — it silently turns
the request body into a query parameter).

---
*The core handbook ends here — welcome aboard: you now know the project better than most
people know their own. Three supplements follow for the questions that come next:*

- *[Part 21 — Engineering Notes](21_engineering_notes.md): "why didn't you just use BERT/GPT/LSTM/TFT/XGBoost/TabNet?"*
- *[Part 22 — Debugging the Pipeline](22_debugging_the_pipeline.md): symptom → causes → diagnosis, for 11pm-you.*
- *[Part 23 — Credit FM vs LLM](23_credit_fm_vs_llm.md): the side-by-side that ties everything together.*
