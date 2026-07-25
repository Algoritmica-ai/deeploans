# Payment behaviours (B2B invoices) — reference implementation

Onboards an anonymized invoice payment-behaviour dataset onto the framework: one CSV row per
customer, `payment_sequence` = pipe-separated days-past-due per invoice (`0` = paid on time or
early). Tasks: **will a currently-current customer go >30 dpd within the next 3 invoices**
(`late30_3m`) and the >90-dpd / 6-invoice variant (`late90_6m`).

This is the framework's smallest onboarding: **one adapter file + one contract + recipes — zero
core-code changes.** Corpus scale: ~595k customers, ~25M invoices (median history 8 invoices,
p95 ~243), ~35% paid on time, 13% of invoices >30 dpd, 4% >90 dpd.

## Data placement

Copy the CSV to the box (it is small, ~300 MB):

```bash
# from your laptop:
scp ~/Downloads/Payment_Behaviours_Anon.csv <box>:/workspace/data/payment_behaviours/raw/
```

## Runbook (all from the repo root, venv active)

```bash
# 1. ingest: explode sequences -> per-invoice panel (cleans junk, caps dpd at 3650)
python scripts/ingest.py -c configs/payment_behaviours/ingest.yaml

# 2. customer-disjoint split (+ audit)
python scripts/prepare_data.py -c configs/payment_behaviours/prepare.yaml
python scripts/validate_splits.py --dir /workspace/data/payment_behaviours/processed/run_pb_v1

# 3. generate the field schema, then add the documented review layer: anchored dpd bins
python scripts/classify_schema.py -c configs/payment_behaviours/classify.yaml \
    --out configs/payment_behaviours/tokenizer.yaml
#    -> edit configs/payment_behaviours/tokenizer.yaml, add:   anchors: { dpd: [1, 8, 31, 61, 91] }

# 4. fit the KVT tokenizer on TRAIN only (DL-008) + QA report
python scripts/train_tokenizer.py -c configs/payment_behaviours/tokenizer_fit.yaml

# 5. encode-once shards
for s in train val test; do
  python scripts/encode_dataset.py -c configs/payment_behaviours/encode.yaml --split $s
done

# 6. MLM pretraining (~5M params; single GPU is plenty at this scale)
python scripts/pretrain.py -c configs/payment_behaviours/pretrain.yaml

# 7. fine-tune + evaluate the late30_3m task
python scripts/finetune.py -c configs/payment_behaviours/finetune_late30.yaml --mode full
```

## Honest-evaluation notes (read before quoting numbers)

* **There is no calendar.** Time is synthesized from sequence position (invoice *i* → month-end
  of `2000-01 + i`), so `cal=` tokens are pseudo-calendar and carry no macro signal, and
  "out-of-time" does not exist on this asset. The honest split is **customer-disjoint**: the
  prepare split is entity-disjoint by construction (constant origination → id-ordered
  positional partition over sha-256 ids), and `finetune.py`'s loan-disjoint hash split governs
  train-vs-test customers across cutoffs. If real invoice dates exist upstream of this
  anonymized extract, obtaining them would materially upgrade the evaluation.
* **Right-censoring:** a customer observed at a cutoff whose sequence ends inside the label
  window can never fire — labels are biased low at deep cutoffs. Same caveat as any survival
  setting; keep cutoffs well inside the length distribution.
* **`dpd` history is a feature by design** (it is the only behavioural signal in the data);
  honesty is preserved by strictly-forward labels plus gates (`under30`/`under90`) that exclude
  customers already in the event state at observation. The label event/gate columns themselves
  are machine-enforced leakage. See the comment block in
  [`configs/payment_behaviours/dataset.yaml`](../../configs/payment_behaviours/dataset.yaml).
* **No baseline yet:** `features_bar` is null. Before claiming an FM advantage, build the
  honest bar — XGBoost on rolling statistics (last/mean/max dpd over trailing k invoices,
  trend, on-time streak) over the same gated observations and customer-disjoint split.
* One element of `payment_sequence` is assumed to be one invoice; the long constant runs in
  the raw data suggest some sequences may be monthly restatements of the same receivable —
  confirm with the data owner before interpreting `horizon_months` as "next N invoices".
