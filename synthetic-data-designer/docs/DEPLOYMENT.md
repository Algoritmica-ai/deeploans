# Hosting it for other people

The tool is built to run on your own machine, where the Upload screen's promise —
*nothing leaves this machine* — is simply true. Hosting it somewhere shared makes
that promise false, and everything in this document follows from that.

- [Should you?](#should-you)
- [Hugging Face Spaces](#hugging-face-spaces)
- [What changes when it is shared](#what-changes-when-it-is-shared)
- [Configuration](#configuration)
- [Any other host](#any-other-host)

---

## Should you?

A hosted instance is a good **demo** and a bad **workbench**.

| | |
|---|---|
| ✅ Showing what the tool does, using the bundled packs | Nothing sensitive is involved: the packs generate from a spec, not from anyone's data |
| ✅ Letting someone try the wizard before installing it | The whole flow works, end to end, in a browser tab |
| ⚠️ Generating from an uploaded tape | The tape lands on a shared disk that the operator can read |
| ❌ Anything confidential | Run it locally. That is the mode it was designed for, and it takes one command |

The install is two lines, so pointing people at a local run costs them very little:

```bash
pip install 'synthetic-data-designer[web] @ git+https://github.com/sriramarun/synthetic-data-designer'
sdd ui
```

---

## Hugging Face Spaces

Technically yes, on the **Docker** SDK — it is a FastAPI app serving a static
front end, which is what Docker Spaces are for. Gradio and Streamlit Spaces
expect the app to be written in those frameworks, so they are not an option.

**It is not free.** As of this writing, a Docker Space on `cpu-basic` requires:

| Namespace | Requires |
|---|---|
| A personal account | a **PRO** subscription — <https://huggingface.co/pro> |
| An organisation | a **Team or Enterprise** plan — <https://huggingface.co/enterprise> |

The free tier hosts **Static** Spaces only: HTML and JavaScript, no server
process. That rules this out entirely — profiling, generating and ageing all
need Python running, and there is no version of that which is static.

Attempting it without a plan fails at creation with a `402 Payment Required` and
a message naming which plan is needed, so you will not get a half-deployed
Space. If you are not paying Hugging Face, skip to
[any other host](#any-other-host) — the same Dockerfile runs anywhere.

Everything needed is in `deploy/huggingface/`:

```
Dockerfile         python:3.12-slim, runs as uid 1000, binds 0.0.0.0:7860
README.md          the Space card, with the front-matter Spaces reads
push_to_space.sh   assembles and pushes the Space repo
```

### Deploying

```bash
hf auth login
./deploy/huggingface/push_to_space.sh <your-username>/synthetic-data-designer
```

The script creates the Space if it does not exist, copies in the source, the
packs and the packaging metadata, adds the Dockerfile and the Space card, and
pushes. The first build takes a few minutes; watch the **Logs** tab.

### What the image does and does not include

The `deep` extra — CTGAN and TVAE — is **left out on purpose**. It pulls in
PyTorch, which would multiply the image size and then train unusably slowly on a
free CPU Space. The interface already greys those two methods out and explains
why, so nothing looks broken.

Everything else is there: all four schema-only generation methods, the full
ageing engine, both calibrated packs, the charts, and all five download formats.

### What `cpu-basic` gives you

| | |
|---|---|
| CPU | 2 vCPU, 16 GB — fine for the default 10,000 rows × 24 periods |
| Storage | **Ephemeral.** Wiped on every restart, and Spaces sleep after inactivity |
| Sleep | A sleeping Space cold-starts on the next visit, which takes a moment |
| Persistence | A further paid add-on |

Because storage is ephemeral, a generated panel is gone when the Space restarts.
That is acceptable for a demo — the Download step is one click from Results — and
the Space card says so.

---

## What changes when it is shared

Four things are true of a hosted instance that are not true of a local one. The
app now knows the difference, but it is worth understanding rather than trusting.

**1. Uploads land on someone else's disk.** Set `SDD_SHARED=1` and the Upload
screen replaces its privacy claim with a notice saying exactly that, naming the
limits in force and pointing at the local install. Without the flag the app would
go on telling visitors nothing leaves their machine, which on a Space is a lie.

**2. There is one workspace, and no accounts.** Uploads and outputs from every
visitor share a directory. Job ids and filenames are random, so nobody stumbles
into anyone else's by accident, but this is obscurity, not isolation — there is
no authentication and no per-user separation. **Do not host this for people
uploading confidential tapes.**

**3. An unbounded run is a denial of service with extra steps.** One visitor
asking for 5,000,000 rows × 240 periods occupies the whole instance. The three
ceilings below exist for that, and are unset by default so a local run is never
told what it may ask for.

**4. The engine is single-process.** Runs go through a two-worker thread pool.
Under real concurrency they queue, which is fine for a demo and not a design for
a service.

---

## Configuration

Every setting is an environment variable, and every one is optional. Unset, you
get the local behaviour the tool has always had.

| Variable | Default | What it does |
|---|---|---|
| `SDD_WORKSPACE` | `./.sdd-workspace` | Where uploads and outputs go. Needed wherever the working directory is not writable |
| `SDD_SHARED` | unset | Marks the instance as shared. Swaps the privacy copy for a notice |
| `SDD_MAX_RECORDS` | unset | Largest run, in entities. Over it, the run is refused with a readable message that points at the local install |
| `SDD_MAX_PERIODS` | unset | Longest run, in cut-offs |
| `SDD_MAX_ROWS` | unset | Largest run, in **output rows**. The one that bounds the machine — see below |
| `SDD_MAX_UPLOAD_MB` | unset | Largest upload. Checked *as the file streams*, so a declared size cannot lie |

The Space image sets:

```
SDD_SHARED=1
SDD_WORKSPACE=/home/user/workspace
SDD_MAX_RECORDS=50000
SDD_MAX_PERIODS=60
SDD_MAX_ROWS=2500000
SDD_MAX_UPLOAD_MB=50
```

To try shared mode locally before deploying:

```bash
SDD_SHARED=1 SDD_MAX_RECORDS=5000 SDD_MAX_ROWS=100000 SDD_MAX_UPLOAD_MB=5 sdd ui
```

### Set `SDD_MAX_ROWS`, not just `SDD_MAX_RECORDS`

An entity is a loan. A **row** is one loan at one cut-off. Age 3,000 loans over
12 months and you get roughly 34,000 rows — so a cap on entities is a cap on
rows only if you remember to multiply, and it is not even that once
`originations` are in play.

`originations` is the open-pool setting: new loans join the book as it ages.
Give it a `rate` and the pool compounds. Measured on this codebase, 1,000
entities aged 30 periods at `originations.rate: 1.0` produce **413,938 rows**,
896 MB of memory and 156 MB on disk — from a request that reads as a thousand
loans. And the spec is posted by the browser, so a visitor can set that field.

So `SDD_MAX_RECORDS` bounds nothing on its own.

`SDD_MAX_ROWS` is checked **during the run**, per period, against the running
row count — and nowhere else. There is no pre-check, on purpose.

The tempting one is `entities × periods > SDD_MAX_ROWS`, and it is wrong in the
direction that costs you users. Entities that reach a terminal state are dropped
and stop producing rows, so the product *over*-states a closed pool: 50,000
entities over 60 periods is 2,033,298 rows, not 3,000,000. Refuse on it and you
turn away runs that would have fitted, with a message the person cannot argue
with, because they cannot know the survival curve in advance. Originations break
the bound the other way. It is not reliable in either direction.

Counting the actual rows is. The loop stops at the period that crosses the line,
so the work wasted by not rejecting early is bounded by the ceiling itself.

When a run trips the ceiling it stops at that period, deletes its partial output,
and reports how far it got. It never materialises the panel.

Set it to what the box can hold. On 16 GB, 2.5M rows peaks around 2.2 GB, and
the run pool is two workers, so budget for two.

---

## Any other host

Nothing in the above is specific to Hugging Face. The app is a standard ASGI
application:

```bash
uvicorn sdd.web.app:app --host 0.0.0.0 --port 8000
```

Three things any host needs:

1. **A writable `SDD_WORKSPACE`**, on a disk with room for the panels people
   generate.
2. **The ceilings set**, if strangers can reach it.
3. **A reverse proxy in front**, if it is public. There is no authentication, no
   rate limiting and no TLS in the app itself — those are the proxy's job, and
   pretending otherwise would be worse than saying so.

For a private team instance, put it behind whatever SSO you already have and
leave `SDD_SHARED` unset only if the workspace genuinely is private to that team.

---

## The other Hugging Face option: publishing the data

Spaces host the *tool*. If what you want to share is the *output* — a synthetic
tape someone can download and model against — that is a **Dataset**, and it is a
better fit for the thing this tool produces:

```bash
sdd run auto_abs_esma_annex5 -n 100000 -o ./out
hf upload <your-username>/synthetic-auto-abs ./out --repo-type dataset
```

Upload the run's YAML and `run_manifest.json` alongside the data. Between them
they record the spec hash, the seed and the library versions, so anyone who finds
the dataset can regenerate it exactly rather than taking it on trust — which is
most of the argument for synthetic data in the first place.
