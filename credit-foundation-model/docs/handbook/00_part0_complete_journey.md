# Part 0 — The Complete Journey

> **Read this page before anything else.** It is the whole project in one picture and one story.
> Every box below gets its own chapter; if you ever feel lost later, come back here and find
> your box.

## 0.1 One picture

```
        ┌───────────────────────────────────────────────────────────────────┐
        │                    RAW MORTGAGE DATA                              │
        │   25 years · millions of loans · one row per loan per month       │
        └────────────────────────────────┬──────────────────────────────────┘
                                         ▼
                                 ①  INGESTION            scripts/ingest.py          → Part 6
                          clean columns, derive labels, sample loans
                                         ▼
                                 ②  VALIDATION           validate_ingest.py         → Part 7
                              prove the cleaned data is actually right
                                         ▼
                                 ③  DATASET (SPLIT)      prepare_data.py            → Part 8
                        train / val / test — by loan, ordered by time
                                         ▼
                                 ④  TOKENIZER            train_tokenizer.py         → Part 9
                          invent the alphabet: every fact → a symbol
                                         ▼
                                 ⑤  VOCABULARY           tokenizer.json (552 tokens)
                              the frozen dictionary: symbol ↔ number
                                         ▼
                                 ⑥  ENCODING             encode_dataset.py          → Part 9
                            every loan → a sequence of integers, once
                                         ▼
                                 ⑦  TRANSFORMER          src/credit_fm/models/      → Parts 10–11
                        the reading machine: three-branch encoder
                                         ▼
                                 ⑧  PRETRAINING          pretrain.py                → Part 12
                       fill-in-the-blanks on billions of loan-months
                                         ▼
                                 ⑨  .pt MODEL            m_100m.pt (the backbone)
                          100M numbers that "understand loans"
                                         ▼
                                 ⑩  FINE-TUNING          finetune.py                → Part 13
                     teach it ONE question: default in 12 months?
                                         ▼
                                 ⑪  PREDICTIONS          score_portfolio.py         → Part 15
                              every loan gets a raw risk score
                                         ▼
                                 ⑫  PROBABILITY          calibrate.py               → Part 15
                        score → honest PD ("0.42% chance of default")
                                         ▼
                                 ⑬  RISK DECISIONS       thresholds, review lists   → Part 14–15
                                         ▼
                                 ⑭  DEPLOYMENT           serve.py / batch runs      → Part 15
        └── quality gates the whole way down: validators + metrics vs an XGBoost bar (Parts 7, 14)
```

That's it. Fourteen boxes. The remaining ~300 pages explain every box, but you now know what
the car *is* before we open the engine.

## 0.2 Watch one loan become a number

The same journey again — but instead of stage names, watch the **data itself** transform.
One loan, real shapes, end to end (this is "the Ohio loan"; Part 4 tells its full story):

```
RAW CSV (one of its 66 monthly rows)
    loan_identifier="731942800123"  monthly_reporting_period="042020"
    original_ltv=87  current_actual_upb=186211.44  current_loan_delinquency_status="1"
        │
        ▼  ① INGEST — parse, rename, derive
    loan_id="731942800123"  reporting_date="2020-04-30"
    dlq_num=1  default_event=False  prepay_event=False  is_performing=False
        │
        ▼  ④⑤ TOKENIZE — every fact becomes a symbol (66 months → one "sentence")
    [BOS] [USR] original_ltv=5 dti=6 credit_score=4 channel=R ...
    [EVT_START] t=9 cal=2020Q2 current_interest_rate=7 current_upb=9 ... [EVT_END] ...
        │
        ▼  ⑥ ENCODE — symbols become integers (the model only eats numbers)
    input_ids = [1, 5, 217, 198, 164, 87, ..., 7, 63, 412, 91, 88, ..., 8, ...]   (~950 of them)
        │
        ▼  ⑦ EMBEDDING — each integer looks up its meaning-vector
    id 217  →  [-0.43, 0.82, 0.11, ..., -0.07]      (768 numbers)
    → the loan is now a stack of ~950 vectors           shape (950, 768)
        │
        ▼  ⑦ TRANSFORMER — three readers compress the stack
    ~950 token vectors ──Event encoder──▶ 66 month vectors        (66, 768)
                       ──History encoder─▶ 1 loan vector           (768,)
    "everything about this loan, as one point in 768-dim space"
        │
        ▼  ⑩ FINE-TUNED HEAD — one small layer asks the question
    loan vector → Linear(768→2) → softmax → raw score 0.31
    ("ranks riskier than ~85% of the book — but NOT a probability yet")
        │
        ▼  ⑫ CALIBRATION — fix the level, keep the ranking
    raw 0.31 ──isotonic map──▶ PD = 0.0042      (0.42% chance of default in 12 months)
        │
        ▼  ⑭ DEPLOYMENT
    {"loan_id": "731942800123", "score": 0.31, "pd": 0.0042, "rank": 1187}
```

Two numbers to hold onto from this trace: **~950 → 66 → 1** (the compression staircase every
loan walks down, Part 11) and **0.31 → 0.0042** (why raw scores are not probabilities, Parts
8 and 15).

## 0.3 The three ideas everything else hangs on

1. **Loans are sentences.** A payment history is a sequence of events, like words. So the
   machinery that revolutionized language (transformers, pretraining) applies — with a custom
   alphabet (Part 9).
2. **Learn first, ask later.** Pretraining learns *the language of credit* from billions of
   unlabeled months; fine-tuning then teaches one question with comparatively few labels
   (Parts 12–13). That split is the entire "foundation model" idea.
3. **Never let the model peek.** Every stage has a wall against information from the future or
   the outcome (leakage), and every wall has an auditor that proves it held (Parts 7–8). This
   is why the final numbers can be believed.

### Things to remember

1. Fourteen boxes: raw data → ingest → validate → split → tokenize → encode → transformer →
   pretrain → checkpoint → fine-tune → predict → calibrate → decide → deploy.
2. The data's shape journey: 66 monthly rows → ~950 tokens → ~950 vectors → 66 vectors →
   **1 vector** → 1 score → 1 probability.
3. A raw model score ranks loans; only after calibration is it a probability.
4. Every arrow in the picture is one script with one YAML recipe — and most have a validator.

---
*Next: [Part 1 — Introduction](01_introduction.md), where "why build this at all?" gets a proper answer.*
