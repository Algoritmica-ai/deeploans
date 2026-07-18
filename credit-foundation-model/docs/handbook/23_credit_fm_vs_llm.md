# Part 23 — How This Compares With an LLM

> The conceptual keystone chapter. If you understand ChatGPT even vaguely, this mapping makes
> the whole project click — and the *differences* teach you what "foundation model" really
> means, stripped of the language part.

## 23.1 The side-by-side

| Stage | Credit Foundation Model (this repo) | LLM (GPT/Llama-class) |
|---|---|---|
| Raw data | loan histories: 3.3B loan-months, 25 years | text: trillions of tokens from the web |
| "A document" | one loan (66 monthly events) | one web page / article |
| Tokenizer | **constructed** Key-Value-Time (`field=value`, `t=`, `cal=`) | **learned** BPE/WordPiece subwords |
| Vocabulary | 552 tokens, closed, human-readable | 50k–200k tokens, open-ended |
| Sequence length | ~950 tokens per loan (60 months) | 4k–1M+ context windows |
| Embeddings | credit facts → 768-dim vectors | word pieces → 4k–16k-dim vectors |
| Architecture | transformer, **encoder-only**, 3-branch hierarchy | transformer, **decoder-only**, flat stack |
| Positions | 3 axes: sequence + month + field (+ branch) | 1 axis: token position |
| Pretraining | masked-token prediction (MLM, 3 masking sources) | next-token prediction |
| Scale | 26M–100M params | 7B–1T+ params |
| Scaling law | measured locally: data must grow with params (E10–E12) | Chinchilla et al. — the same law, industrial size |
| The learned thing | "grammar of credit": amortization, delinquency arcs, regimes | grammar, facts, style, reasoning patterns |
| Output | a **loan embedding** → scores/PDs | generated **text** (or a text embedding) |
| Fine-tuning | frozen / LoRA / full → default, prepay, (cure…) | SFT/LoRA/RLHF → chat, QA, summarization |
| Serving | batch scoring + a small FastAPI example; deterministic | autoregressive sampling; token-by-token latency |
| Failure mode to fear | **leakage** (silently fake backtests) | **hallucination** (confidently fake facts) |
| Audit story | closed vocab, decodable inputs, validators, lineage | largely opaque corpus + emergent behavior |

## 23.2 What is *genuinely the same*

The core loop is identical, and that's the deep point:

```
      LLM:   text  → subword tokens → transformer → pretrain (predict hidden/next token)
                  → generic language understanding → cheap adaptation to many tasks
   Credit:   loans → KVT tokens     → transformer → pretrain (predict hidden tokens)
                  → generic credit understanding  → cheap adaptation to many tasks
```

Same embeddings-plus-attention machinery (Part 10 applies verbatim to both), same
self-supervision insight (the data labels itself), same transfer-learning economics (expensive
reader trained once, tiny task heads after), same scaling behavior (our 65M-flat result is
Chinchilla's lesson at 1/1000th scale), even the same adaptation toolbox — LoRA was invented
for LLMs and works unchanged here (Part 13).

## 23.3 The differences that matter (and what each teaches)

**Closed vs open vocabulary.** English needs 100k subwords because anyone can write anything.
Loans speak a language with a *known, finite* lexicon — every possible token is enumerable from
schema × bins (552 total). Consequences: the embedding table is tiny, the softmax is cheap,
every input is exactly decodable back to human-readable facts (`tok.decode` — try that with
BPE), and out-of-vocabulary is *impossible by construction* (clamping/OTHER at the tokenizer).
Lesson: vocabulary size is a property of the domain, not of the method.

**Encoder vs decoder.** An LLM must generate, so it reads left-to-right and predicts what comes
*next*. We must *understand and summarize*, so we read bidirectionally and predict what's
*hidden*. When you already have the whole (cutoff-truncated) history, letting month 30 see
month 50 is free extra context — the temporal honesty lives in the data layer, not the
attention mask. Lesson: the objective follows the product (representation vs generation), not
fashion.

**One position axis vs three.** Text has word order. Loans have order *and* month membership
*and* field identity — hence RoPE + `event_index` + `field_type`/`branch` embeddings, and the
hierarchical pooling (~950 → 60 → 1) that has no LLM analogue. Lesson: structure you know
belongs in the architecture, not rediscovered from data.

**100M vs 100B parameters — and why small is correct here.** Model size should track the
*entropy of the domain*, and the language of loans is vastly more regular than English: 552
symbols, strong local grammar, short documents. The measured evidence agrees — at 26M→65M the
bottleneck was already data, not capacity (E10). A 70B credit model on today's corpus would be
almost entirely idle capacity. Lesson: "foundation model" is a *training strategy*, not a
parameter count.

**Determinism, audit, sovereignty.** A regulated PD must be reproducible, explainable, and
computed inside the bank's walls. This repo is built around that: bit-reproducible scoring
(tested), decodable inputs, lineage in every artifact, nothing-phones-home logging (DL-009),
Apache-2.0 weights you can host. An API-served LLM fails all four requirements at once — which
is *the* practical reason "just prompt GPT with the loan history" isn't the architecture
(Part 21 adds the token-economics reason).

## 23.4 Traffic between the two worlds

Borrowed *from* LLMs, working here unchanged: the transformer stack (RoPE/RMSNorm/SwiGLU),
MLM pretraining, dynamic masking (RoBERTa), warmup-cosine + AdamW hygiene, LoRA, mixed
precision, DDP, checkpoint-resume culture, scaling-law thinking. Flowing *back* from domains
like this one: leakage-grade evaluation discipline (calendar-OOT would improve many an LLM
benchmark), calibration as a shipped stage rather than an afterthought, and closed-vocabulary
auditability as a design goal. The `[USR]` token is BERT's `[CLS]` wearing a bank badge.

### Things to remember

1. Same machine, different language: transformer + self-supervised pretraining + cheap
   adaptation is the whole "foundation model" pattern, and it transfers to any sequence domain.
2. 552 closed tokens vs 100k open subwords — vocabulary is a property of the domain.
3. Encoder-only because the product is understanding (an embedding), not generation.
4. Parameter count follows domain entropy: 100M is not "small," it's *fitted*.
5. LLMs fear hallucination; credit models fear leakage — each field's discipline is built
   around its own worst failure.

---
*This closes the handbook's conceptual arc: [back to the index](00_README.md).*
