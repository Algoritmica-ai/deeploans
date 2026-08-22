# What this thing actually does

Written for someone who has never worked with loan data and has never heard of a
"synthetic data generator". No jargon without a definition. If you know the domain
already, the [README](../README.md) is faster.

---

## 1. The problem it exists to solve

A bank wants to build software that predicts which borrowers will stop paying.

To build it — or to test it, or to demonstrate it to a colleague — you need **loan
data**. And loan data is the hardest thing in the building to get hold of:

- it is real people's finances, so access needs legal approval, which takes months;
- you cannot put it on a laptop, email it, or show it in a demo;
- a new supplier cannot see any of it until a contract is signed.

So people are stuck. They cannot build the thing without the data, and they cannot
get the data until they have built the thing.

**This tool makes fake loan data that behaves like the real thing.** Not real
people — invented ones — but invented in a way that produces the same *patterns*:
some borrowers fall behind, some catch up, some default, balances go down as loans
are repaid.

---

## 2. First, three words you need

**Loan tape.** The industry's word for a spreadsheet of loans. One row per loan:
who borrowed, how much, what rate, are they behind on payments. "Tape" is a leftover
from when these arrived on magnetic tape, and the word stuck.

**Panel.** The same loans, listed again *every month*. So a thousand loans over two
years is 24,000 rows — each loan appearing 24 times, once per month, with its
balance a little lower each time. This is the shape that matters, because it shows
loans **changing**, and a single snapshot never does.

```
   a tape                      a panel
   one moment                  the same loans, month after month

   loan   balance              loan   month     balance   status
   L001   10,000               L001   Jan       10,000    fine
   L002    8,500               L001   Feb        9,800    fine
   L003   12,000               L001   Mar        9,600    one month late
                               L001   Apr        9,600    two months late
                               L002   Jan        8,500    fine
                               ...
```

**Default.** The borrower has stopped paying and is not expected to resume. The
event everyone is trying to predict.

---

## 3. How it works, in one line

> **You describe the loans you want in a text file. It writes the data.**

That text file is called a **spec** — short for specification. It is written in
YAML, a format designed to be readable by people. Here is a real fragment:

```yaml
- name: current_balance
  dtype: float
  generator:
    kind: gaussian     # a bell curve
    mean: 18000        # centred on 18,000
    stddev: 5200       # most loans within ±5,200 of that
    clip_min: 2000     # never below 2,000
    clip_max: 45000    # never above 45,000
```

That is the whole idea. You say *"balances should look roughly like this"*, and it
draws thousands of them that do.

**Why a file rather than a settings screen?** Because a file can be saved,
emailed, put in version control, and reviewed by a colleague — and because six
months later it still says exactly what you asked for. A screen where somebody
dragged some sliders remembers nothing.

---

## 4. The part that is genuinely difficult

Making up a column of plausible numbers is easy. The hard part is **time**.

A loan is not a row, it is a story. It starts healthy. It might miss a payment,
then another, then recover — or not. It might be repaid early. The balance falls
each month by an amount that depends on the rate and the term.

Getting that right is most of what this tool is. It models the story with a **state
machine**: a small set of situations a loan can be in, and the chance of moving
between them each month.

```
   Performing ──► 1 month late ──► 2 months late ──► 3+ months late ──► Defaulted
       ▲               │                  │
       └───────────────┴──────────────────┘
              some borrowers catch up again
```

Each arrow carries a probability. Every month, every loan rolls a die. Do that for
24 months across 10,000 loans and you get something that looks like a real
portfolio ageing — because the *mechanism* is the same one that operates in reality,
not because the numbers were copied from anywhere.

There is a second thing happening alongside: the balance going down (**amortisation**
— the technical word for a loan being paid off gradually), interest accruing, house
prices drifting.

---

## 5. It can also read your data and write the file for you

Writing a spec from scratch is work. So it can go the other way round.

Give it a real loan tape — on your own machine, nothing uploaded anywhere — and it
**reads the data and writes the spec that would reproduce it**. It measures what the
balances look like, how often loans fall behind, how often they recover, and puts
all of that into a YAML file you can then edit.

That is called **profiling**, and it is the difference between a two-day setup and a
two-minute one.

---

## 6. What makes this different from other fake-data tools

Plenty of tools produce fake tables. Here is the thing this one does that they
generally do not.

### It can make data whose answer is known

Ask a normal question about a prediction model: *"our model scores 0.84 — is that
good?"*

Nobody can answer it. Some borrowers default for reasons that appear nowhere in the
file — a lost job, an illness — so even a perfect model would not score 1.0. There
is a **ceiling**, and on real data it is invisible. You never learn whether 0.86 was
available or 0.99 was.

Here you can compute it, because **you invented the world**. The tool builds
portfolios where a hidden "true risk" is secretly assigned to each borrower and then
deliberately hidden from the model. It drives who falls behind. The model only sees
three noisy clues — a credit score, borrowing levels, a debt-to-income figure — and
has to work backwards.

Because you know exactly how those clues were generated, you can calculate the best
score *anything* could get from them. Real numbers from the shipped example:

```
  the model got               0.8923
  the best possible was       0.8987     ← the model found 98.4% of what was there
  seeing the hidden truth     0.9162     ← what perfect knowledge would buy
```

Now `0.84` is not a number floating in space. It is **"the model captured 98% of the
signal that exists"** — which is a statement about the model, not a shrug.

### And it catches results that are too good

A model cannot beat the ceiling. So if one does, something is wrong: usually the
model was accidentally shown the answer.

This is not theoretical. While building the feature, a model scored **0.93** against
a ceiling of **0.897** — because it had been tested on the same data it learned
from. On a real portfolio that looks like a triumph and gets celebrated. Here it is
arithmetically impossible, and the tool says so.

---

## 7. The five things you get out

Run it and you get a folder:

| | |
|---|---|
| **the panel** | every loan, every month, one big file |
| **monthly files** | the same data split by month, the way a bank would actually receive it |
| **the report** | portfolio totals per month — balance, arrears, averages |
| **the validation report** | dozens of automatic checks, each pass or fail |
| **the run manifest** | the settings, the random seed, versions — so anyone can reproduce it exactly |

The last two are why this is usable in a regulated setting. **Validation** means the
tool checks its own output: identifiers are unique, a loan marked "repaid" does not
come back to life, balances never go negative. **Reproducible** means the same
settings give byte-identical data next year.

---

## 8. Two honest limitations

**It is not real data.** Patterns are calibrated against published market ranges,
not fitted to any particular lender's book. Good for building, testing, training and
demonstrating. Not for deciding what a portfolio is worth.

**A good score on the invented data does not mean a model works on a real book.**
The data follows rules we wrote; a model that excels here has recovered *our rules*.
What does carry over is narrower and still worth having: whether the model extracts
the signal that is present, and whether your testing process actually works. To
learn about a real portfolio, point the same setup at real data — which is one file
change.

---

## 9. Where to go next

| you want to | go to |
|---|---|
| see it work | [`notebooks/known_ceiling.ipynb`](../notebooks/known_ceiling.ipynb), about a minute |
| click through it | `sdd ui`, then the six-step wizard |
| understand the ceiling | [`docs/KNOWN-CEILING.md`](KNOWN-CEILING.md) |
| learn every setting | [`docs/USER-GUIDE.md`](USER-GUIDE.md) |
