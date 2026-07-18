# Part 10½ — Tensors Without Tears

> The chapter to read when `(B, L, D)` makes your eyes glaze. Nothing here is specific to
> credit — but every shape uses *this repo's* real numbers, and by the end you can follow any
> tensor through the whole model.

## 10½.1 A tensor is boxes inside boxes

```
0-D  scalar          loss = 0.334                                 shape ()
1-D  vector          one loan's embedding                          shape (768,)
2-D  matrix          a batch of token ids                          shape (2, 950)
3-D  "a stack of     a batch of token EMBEDDINGS                   shape (2, 950, 768)
      matrices"
```

**Read a shape aloud, always left to right, as "of":**
`(2, 950, 768)` = "2 loans, **of** 950 tokens each, **of** 768 numbers each."
That sentence *is* the understanding. When you see a new shape, say the sentence.

**The house convention:** dimension 0 is always the **batch** (`B` = how many loans at once);
dimension 1 is the **sequence** (`L` tokens, or `E` months); the last is the **feature width**
(`D` = dim, 768). So `(B, L, D)` = "B loans of L tokens of D numbers."

## 10½.2 The shape journey through this model

Two loans enter (B=2). Loan A has 950 tokens, loan B has 720 — the collator pads B to 950 so
the batch is rectangular. Now follow the shapes (this diagram is the model, seen shape-first):

```
input_ids            (2, 950)        integers — 2 loans of 950 token ids
event_index          (2, 950)        which month each token belongs to (-1 = none)
field_type, branch   (2, 950)        metadata, same shape, always in lockstep
n_events             (2,)            one number per loan: 60 and 48 months
        │
        ▼  Embeddings: every id looks up a 768-vector  (adds a dimension!)
hidden               (2, 950, 768)   "2 loans of 950 tokens of 768 numbers"
        │
        ├─▶ Profile encoder    (2, 950, 768) → masked-mean over ~10 profile tokens
        │                      profile_vec        (2, 768)        one vector per loan
        │
        ├─▶ Event encoder      (2, 950, 768) → pool per month (event_index says which)
        │                      event_vecs         (2, 60, 768)    E = max months in batch
        │                      event_mask         (2, 60)         True where a month exists
        │
        ▼  History encoder: build the timeline sequence
seq = [LOAN] + profile_vec + events   →   (2, 62, 768)      62 = 1 + 1 + 60
        │
        ▼  transformer over 62 positions
loan_embedding       (2, 768)        ← seq[:, 0]  — the [LOAN] slot, one vector per loan
hist_event_ctx       (2, 60, 768)    ← seq[:, 2:] — each month, timeline-aware
        │
        ├─▶ pretraining:  MLM head concat(local‖segment‖loan) (2, 950, 2304) → logits (2, 950, 552)
        │                 cross-entropy vs labels (2, 950)  →  loss  ()          a scalar!
        └─▶ downstream:   classification head (2, 768) → logits (2, 2) → softmax → score (2,)
```

The staircase to memorize: **(B, 950, 768) → (B, 60, 768) → (B, 768)** — tokens → months →
loan. Each step is a deliberate compression (Part 11 explains why).

## 10½.3 The five operations that change shape (and the ones that don't)

| Operation | Shape effect | Where you saw it |
|---|---|---|
| Embedding lookup | appends D: `(B,L)` → `(B,L,D)` | `Embeddings` |
| Pooling (masked mean / scatter-mean) | removes a dim: `(B,L,D)` → `(B,D)` or regroups `(B,L,D)` → `(B,E,D)` | profile vec; event vecs |
| Linear layer | changes ONLY the last dim: `(…, 768)` → `(…, 552)` | MLM head, classification head |
| Concatenate (dim=-1) | adds widths: 3×768 → 2304 | MLM head's 3-way concat |
| Indexing / slicing | drops or shrinks a dim: `seq[:, 0]` → `(B, D)` | taking the [LOAN] slot |
| Attention, norms, residuals, SwiGLU | **shape-preserving** `(B,L,D)` → `(B,L,D)` | every transformer block |

That last row is the great simplifier: the entire transformer stack never changes shape.
All shape drama happens at the edges (embed, pool, heads).

## 10½.4 Padding, masks, and why rectangles

Tensors must be rectangular — no ragged rows. But loans have different lengths. Solution:
pad to the batch max and carry a **mask** saying what's real:

```
loan A  [ 1, 5, 217, …, 8, 2 ]                950 real
loan B  [ 1, 5, 198, …, 2, 0, 0, …, 0 ]       720 real + 230 × [PAD]
attention_mask  [ 1 1 1 … 1 ],  [ 1 … 1 0 … 0 ]
```

Every consumer honors the mask: attention gets −inf on pad keys, the loss ignores label −100,
pooling divides by *real* counts. **Rule: wherever a tensor is padded, some mask travels with
it.** If you ever compute a mean without its mask, padding silently dilutes your numbers —
a classic silent bug.

## 10½.5 Broadcasting in sixty seconds

When shapes don't match, PyTorch stretches size-1 dimensions automatically:

```
(2, 60, 768)  *  (2, 60, 1)      ✓  the 1 stretches to 768   (masking months: mask.unsqueeze(-1))
(2, 768)      +  (768,)          ✓  the vector applies to every loan
(2, 60, 768)  *  (2, 60)         ✗  RuntimeError — trailing dims must align; add the unsqueeze
```

This is why the code is full of `.unsqueeze(-1)` (add a size-1 dim at the end) and
`[:, None, None, :]` (the padding mask reshaped to `(B,1,1,L)` so it broadcasts over heads and
query positions). They're not magic — they're shape adapters.

## 10½.6 Reading PyTorch's error messages

```
RuntimeError: mat1 and mat2 shapes cannot be multiplied (1900x768 and 2304x552)
```

Translate: something flattened `(2, 950, 768)` to `(1900, 768)` and hit a Linear expecting
2304 — you forgot the 3-way concat before the MLM head. The debugging move is always the same:
**print `.shape` at the boundary above the crash and say the sentence out loud.** If the
sentence sounds wrong ("1900 loans of 768 numbers"?!), you've found the bug — 1900 = 2×950, a
collapsed batch×sequence.

Three more habits: `assert x.shape == (B, L, D)` at function entry costs nothing and documents
intent; `dtype` matters too (`input_ids` must be int64 for embedding lookup; scores come back
float); and `device` completes the trio — a `(2,950)` tensor on CPU can't meet a model on GPU
(`.to(device)` at the boundary, exactly what `_to_device` does in the trainer).

### Things to remember

1. Read shapes as a sentence: `(2, 950, 768)` = "2 loans of 950 tokens of 768 numbers."
2. This model's staircase: `(B,950) → (B,950,768) → (B,60,768) → (B,62,768) → (B,768) → (B,2)`.
3. Transformer blocks never change shape; embeddings, pooling, and heads do.
4. Padding makes rectangles; a mask always travels with padded data.
5. When lost: print `.shape`, say the sentence, find the word that sounds wrong.

---
*Continue: [Part 11 — Model Architecture](11_model_architecture.md) — the same journey, now asking WHY each compression exists.*
