# The Credit Foundation Model Handbook

> The definitive teaching reference for the `credit-foundation-model` repository.
> Written for someone who knows basic Python and simple SQL — and **nothing else**: no ML, no
> PyTorch, no finance. Read it front to back and you should be able to explain the architecture,
> train a model, fine-tune a new task, debug failures, and extend the framework.

**Repo state documented:** `main` @ `8ff98e4` (15 Jul 2026) — v1.1 complete (G1–G6 merged).
**Headline result:** out-of-time ROC-AUC **0.8468** / PR-AUC **0.0175** (100M model, 10% corpus)
vs XGBoost **0.7913 / 0.0057** on the same honest protocol.

## How to read this

**Start with Part 0** — the whole project in one picture and one traced loan; every later
chapter is one of its boxes, magnified. Parts 1–5 build the mental model (no code needed).
Parts 6–16 walk the pipeline stage by stage, in the order data flows (stage chapters open with
a *you-are-here* strip, and every chapter ends with a *Things to remember* box). Parts 17–19
turn you from reader into contributor. Part 20 is the glossary; Parts 21–23 are the supplements
that answer the questions you'll have at the end ("why not X?", "how do I debug this?", "how is
this like ChatGPT?").

One fictional loan — **Loan 731942800123, "the Ohio loan"** — travels through the entire book.
By Part 15 you will have watched it go from a CSV row to a calibrated probability of default.

| Part | File | What you'll learn |
|---|---|---|
| **0** | [00_part0_complete_journey.md](00_part0_complete_journey.md) | **The whole system in one picture + one traced loan — read first** |
| 1 | [01_introduction.md](01_introduction.md) | What a credit foundation model is, and why bother |
| 2 | [02_big_picture.md](02_big_picture.md) | The 11-stage pipeline and why it's modular |
| 3 | [03_project_structure.md](03_project_structure.md) | Every folder, what belongs where |
| 4 | [04_end_to_end_data_flow.md](04_end_to_end_data_flow.md) | One loan's journey, dataframe by dataframe |
| 5 | [05_dataset.md](05_dataset.md) | Fannie Mae data, defaults, prepayment, zero-balance codes |
| 6 | [06_ingestion.md](06_ingestion.md) | `ingest.py` + the Fannie adapter, line of business logic |
| 7 | [07_validation.md](07_validation.md) | Why every stage has an auditor, every check explained |
| 8 | [08_data_preparation.md](08_data_preparation.md) | Splits, leakage, labels, class imbalance |
| 9 | [09_tokenization.md](09_tokenization.md) | Turning a spreadsheet into a language |
| 10 | [10_transformer.md](10_transformer.md) | Attention from zero, with tiny numbers |
| 10½ | [10a_tensor_intuition.md](10a_tensor_intuition.md) | Tensors without tears: read any shape, follow any tensor |
| 11 | [11_model_architecture.md](11_model_architecture.md) | Every knob: dim, heads, layers, dropout… |
| 12 | [12_training.md](12_training.md) | One training step, tensor by tensor |
| 13 | [13_fine_tuning.md](13_fine_tuning.md) | Frozen vs LoRA vs full — the transfer-learning payoff |
| 14 | [14_metrics.md](14_metrics.md) | ROC, PR-AUC, calibration — and why ROC alone lies |
| 15 | [15_inference.md](15_inference.md) | Checkpoints, scoring, calibration, serving |
| 16 | [16_configurations.md](16_configurations.md) | The YAML engine: includes, `${...}`, CLI overrides |
| 17 | [17_framework_design.md](17_framework_design.md) | Adapters, label abstraction, streaming, DDP, packaging |
| 18 | [18_experiments.md](18_experiments.md) | Naming, lineage, reproducibility, checkpoint hygiene |
| 19 | [19_developer_guide.md](19_developer_guide.md) | Task-oriented tutorials: run, resume, extend, debug |
| 20 | [20_appendix_glossary.md](20_appendix_glossary.md) | Every ML / finance / engineering term |
| 21 | [21_engineering_notes.md](21_engineering_notes.md) | "Why didn't you just use BERT/GPT/LSTM/TFT/XGBoost/TabNet?" |
| 22 | [22_debugging_the_pipeline.md](22_debugging_the_pipeline.md) | Symptom → causes → diagnosis, as decision trees |
| 23 | [23_credit_fm_vs_llm.md](23_credit_fm_vs_llm.md) | The Credit FM vs LLM side-by-side that ties it all together |

## The one-paragraph summary of the whole project

Banks predict whether borrowers will default using snapshot models (XGBoost on a single row of
features). This project instead **reads each loan's entire month-by-month history as a sequence**
— like a sentence — and pretrains a transformer to understand "the language of loans" on 25 years
of real US mortgage data, without any labels. That pretrained understanding is then fine-tuned for
specific predictions (default, prepayment, …) and beats the strongest honest XGBoost baseline by a
wide margin, evaluated the only way that counts in credit: **trained on the past, tested on a
future it has never seen.**

## Conventions in this book

- `code font` = real names from the repo (files, functions, config keys).
- Boxes drawn with ASCII are worth studying slowly — they carry most of the architecture.
- Every new term gets a plain-English sentence *before* its technical definition.
- "DL-###" refers to entries in `docs/decision_log.md` — the repo's record of *why* choices were made.
