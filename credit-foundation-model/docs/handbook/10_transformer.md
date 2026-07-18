# Part 10 — The Transformer, From Zero

> File: `src/credit_fm/models/base.py` — every block explained here exists there, in ~150 lines.
> If you've never touched a neural network, read this part slowly; everything later leans on it.

## 10.0 Sixty seconds of neural-network prerequisites

A **neural network** is a function with millions of adjustable numbers (**parameters** /
"weights") inside. You show it inputs, it produces outputs, you measure how wrong it was (the
**loss**), and an algorithm (**gradient descent**, Part 12) nudges every parameter to be slightly
less wrong. Repeat millions of times. A **tensor** is just an n-dimensional array of numbers
(`(B, L, dim)` = batch × sequence-length × width) — the currency all of this trades in.

## 10.1 What is a Transformer?

**Plain English:** an architecture for reading sequences where every element gets to *look at*
every other element and decide which ones matter for understanding itself.

**Real-world analogy:** a meeting where each participant (token), before speaking, scans the
whole room and borrows context from the colleagues most relevant to them — rather than the older
alternative (an RNN/LSTM): a relay race where information passes person-to-person and degrades
with distance.

**Why it won:** (1) any-to-any interaction in one step — no degradation across 60 months of loan
history; (2) everything computes in parallel on GPUs; (3) it scales — more data + more parameters
keeps paying (our own E10–E12 ladder is a miniature of this law).

## 10.2 Attention, with actual small numbers

Every token carries a vector. From each token's vector, three learned projections make:

- a **Query** (q): "what am I looking for?"
- a **Key** (k): "what do I advertise?"
- a **Value** (v): "what content do I offer if you attend to me?"

Token i's new representation = a weighted average of everyone's **values**, weighted by how well
i's *query* matches each *key*. Tiny example — 3 tokens, 2 dimensions:

```
q₂ = [1, 0]                 (token 2 is "looking for delinquency-ish info")
k₁ = [0.9, 0.1]   k₂ = [0.1, 0.8]   k₃ = [0.7, -0.2]

scores  = q₂·k₁ , q₂·k₂ , q₂·k₃  = 0.90 , 0.10 , 0.70     (dot product = compatibility)
scaled  = scores / √d = /√2      = 0.64 , 0.07 , 0.49     (keeps softmax gentle as d grows)
softmax → weights                = 0.42 , 0.24 , 0.35     (positive, sum to 1)

new_token₂ = 0.42·v₁ + 0.24·v₂ + 0.35·v₃
```

That's the entire trick: `softmax(QKᵀ/√d)·V`. Token 2 just rebuilt itself mostly out of tokens 1
and 3. In our model, a masked `dlq` token can rebuild itself by attending to the balance and
rate tokens of its own month.

**In this repo** (`MultiHeadSelfAttention.forward`): the formula runs through PyTorch's
`scaled_dot_product_attention`, which computes it **without materializing the L×L score matrix**
(FlashAttention on H100s). That single line is why the 100M model's big batches fit in memory —
attention memory is the O(L²) monster, and a ~950-token loan makes L² ≈ 900k per head per loan.

## 10.3 Multi-head attention

**Plain:** run several small attentions in parallel, each free to specialize — one head may track
"same field across months," another "everything in my month," another "the calendar tokens."

**Technical:** split `dim` into `n_heads` slices (dim 768, 8 heads → head_dim 96); each head does
attention in its slice; concatenate; one output projection mixes them. Cost: same as one big
head. Benefit: h different relationship patterns per layer. In code: the `qkv` Linear makes all
three projections for all heads in one matrix multiply, then a reshape to `(B, n_heads, L,
head_dim)`.

## 10.4 The supporting cast (each: what breaks without it)

**Residual connections** — `x = x + attn(norm(x))`: each layer *adds a correction* to the
running representation instead of replacing it.
*Analogy:* editors passing a manuscript with tracked changes, not retyping it.
*Without:* gradients can't flow through 14 layers; deep nets simply don't train.

**Normalization — RMSNorm** (`base.py::RMSNorm`): rescale each token's vector to a standard size
before each sub-layer (divide by the root-mean-square, multiply by a learned gain — no mean
subtraction, no bias; simpler and faster than classic LayerNorm, standard in Llama-era stacks).
*Why "pre-norm"* (normalize *before* attention/MLP, as here): markedly more stable training than
the original post-norm — deep models train without warmup gymnastics.
*Without:* activations drift in scale layer by layer until softmax saturates and learning stalls.

**Feed-forward network — SwiGLU** (`base.py::SwiGLU`): after attention *gathers* context, a
per-token 2-layer MLP *processes* it. This repo uses the gated variant:
`down( silu(gate(x)) ⊙ up(x) )` — the gate path decides how much of the up path passes, hidden
size ≈ 8/3·dim rounded to a multiple of 8 (GPU-friendly shapes). Empirically better than plain
ReLU MLPs at equal parameters; this is where most of a transformer's parameters live — and where
our 100M model's OOM actually happened (activation memory in `w_up`, fixed by gradient
accumulation, Part 12).

**Embeddings** (`base.py::Embeddings`) — the entry point: token id → learned `dim`-vector, plus
this repo's twist: **three added embeddings per position**:

```
vector(position) = token_embedding[input_id]
                 + field_type_embedding[field_type]     (-1 → a dedicated "none" row)
                 + branch_embedding[branch + 1]         (-1/0/1 → rows 0/1/2)
```

So `current_upb=12` arrives already knowing *what field* it is and *which branch* it belongs to.
(`[PAD]`'s row is pinned to zeros via `padding_idx`.)

**Positional encoding — RoPE** (`apply_rope`): attention is order-blind by default ("March
after February" would be invisible). Rotary Position Embeddings encode position by *rotating*
each q/k vector by an angle proportional to its position — like clock hands turning as you move
along the sequence — so the q·k dot product depends on **relative distance** ("3 months apart"),
which generalizes better than absolute slot numbers and adds zero parameters. Cheap sanity
check from the math: at distance 0 the rotation cancels entirely.

**Masks** (`padding_additive_mask`, `event_block_additive_mask`): additive matrices of 0 ("may
attend") and −inf ("may not") applied to scores before softmax (−inf → weight exactly 0). Two
uses here: hide `[PAD]` positions in batches, and — distinctive to this architecture — restrict
Profile/Event-encoder attention to *within one month* (the diagonal is always allowed so no row
is fully masked → no NaNs). Part 11 explains why.

## 10.5 Assembly: one block, then a stack

```
TransformerBlock(x):                          TransformerEncoder = N blocks + final RMSNorm
    x = x + Attention(RMSNorm(x), mask)         ← gather: talk to relevant tokens
    x = x + SwiGLU(RMSNorm(x))                  ← think: process what you heard
    return x
```

Layer 1 learns local patterns; layer 6's attention operates on already-contextualized vectors,
composing patterns of patterns. Depth = levels of composition.

## 10.6 Where each concept lives in this repo — the index

| Concept | Code | Used by |
|---|---|---|
| Attention (SDPA/Flash) | `base.py::MultiHeadSelfAttention` | every layer of all three branches |
| RoPE | `base.py::_rope_tables/apply_rope` | inside attention (q,k) |
| RMSNorm (pre-norm) | `base.py::RMSNorm` | `TransformerBlock`, encoder final norm |
| SwiGLU MLP | `base.py::SwiGLU` | every block |
| Residuals | `base.py::TransformerBlock` | everywhere |
| Token+field+branch embeddings | `base.py::Embeddings` | model entry |
| Padding mask | `padding_additive_mask` | History encoder (absent months) |
| Block-structure mask | `event_block_additive_mask` | Profile & Event encoders (intra-month attention) |

### Things to remember

1. Attention = softmax(QKᵀ/√d)·V: each token rebuilds itself from the tokens most relevant to it.
2. SDPA/FlashAttention computes it without the L×L matrix — the reason big batches fit.
3. The modern block: pre-RMSNorm → attention → residual → pre-RMSNorm → SwiGLU → residual.
4. RoPE encodes *relative* position by rotating q/k; masks are additive 0/−inf before softmax.
5. Entry point: token + field_type + branch embeddings, summed.

---
*Next: shapes making your head spin? Detour through [Part 10½ — Tensors Without Tears](10a_tensor_intuition.md) first. Otherwise: [Part 11 — Model Architecture](11_model_architecture.md): three of these encoders, arranged with intent.*
