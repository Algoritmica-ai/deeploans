# Hugging Face Space image — Docker SDK.
#
# Spaces runs the container as uid 1000 with a read-only image layer, so the
# workspace has to live somewhere the app user owns, and the server has to bind
# 0.0.0.0:7860 rather than localhost.
#
# The `deep` extra is deliberately left out. CTGAN and TVAE pull in torch, which
# would multiply the image size and train unusably slowly on a free CPU Space.
# The interface already greys those two methods out and says why.

FROM python:3.12-slim

# Spaces expects uid 1000. Anything written by another user is unwritable at run
# time, and the failure surfaces as a permission error mid-upload.
RUN useradd --create-home --uid 1000 user
USER user

ENV HOME=/home/user \
    PATH=/home/user/.local/bin:$PATH \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /home/user/app

# Dependencies before source, so editing a file does not reinstall scipy.
# LICENSE is not optional here: pyproject declares `license = { file = ... }`,
# and hatchling refuses to generate metadata without it.
COPY --chown=user pyproject.toml README.md LICENSE ./
COPY --chown=user src ./src
COPY --chown=user packs ./packs
RUN pip install --no-cache-dir --user '.[web]'

# Uploads and generated panels. Inside $HOME because that is what the app user
# owns; ephemeral, and wiped whenever the Space restarts.
RUN mkdir -p /home/user/workspace
ENV SDD_WORKSPACE=/home/user/workspace

# Ceilings, because this is one machine serving strangers. Unset for local use.
#
# Sized against CPU Basic — 2 vCPU, 16 GB RAM, 50 GB of non-persistent disk —
# and against what a run actually costs, measured rather than guessed:
#
#     entities x periods      rows        peak RSS     workspace
#      5,000 x 12             58k          471 MB         32 MB
#     20,000 x 24            447k        1,437 MB        241 MB
#     50,000 x 24          1,117k        2,518 MB        603 MB
#     50,000 x 60          2,033k        2,172 MB        769 MB
#
# SDD_MAX_ROWS is the one that bounds the machine, and the other two do not
# imply it. A row is one entity at one cut-off, so 50,000 entities over 60
# periods is already 2.0M rows — and `originations` in the spec add entities as
# the pool ages, so the entity count is not even an upper bound on itself.
# Measured: 1,000 entities aged 30 periods with `originations.rate: 1.0` yields
# 413,938 rows, 896 MB and 156 MB on disk from a request that looks tiny. The
# row ceiling is checked inside the ageing loop, per period, against the running
# count, so no spec can talk its way past it.
#
# 3.0M, not 2.5M, because the two ceilings above have to be jointly reachable.
# The largest run the interface offers — 50,000 entities over 60 periods on the
# RMBS pack — lands at 2,462,613 rows, 1.5% under a 2.5M cap. A stress scenario
# prepays fewer loans, so more survive and more rows are written, and a request
# the interface itself advertised would die around period 59 citing a limit the
# user never set. 3.0M peaks near 2.6 GB; two at once — the run pool is two
# workers — is 5.2 GB, comfortably inside 16 GB.
ENV SDD_SHARED=1 \
    SDD_MAX_RECORDS=50000 \
    SDD_MAX_PERIODS=60 \
    SDD_MAX_ROWS=3000000 \
    SDD_MAX_UPLOAD_MB=50

EXPOSE 7860
CMD ["sdd", "ui", "--host", "0.0.0.0", "--port", "7860"]
