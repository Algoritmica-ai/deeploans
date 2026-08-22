"""Capture every screen, driving the wizard as a person would.

Uses the Chrome already installed rather than downloading a browser, at 2x so
the images stay legible when scaled down. Light theme explicitly, which is now
also what the app renders by default.
"""

from __future__ import annotations

import sys
from pathlib import Path

from playwright.sync_api import sync_playwright

OUT = Path(sys.argv[1] if len(sys.argv) > 1 else "docs/screenshots")
URL = "http://127.0.0.1:8111"
OUT.mkdir(parents=True, exist_ok=True)

shots: list[tuple[str, str]] = []


def shot(page, name: str, description: str, full: bool = True) -> None:
    path = OUT / f"{name}.png"
    page.screenshot(path=str(path), full_page=full)
    shots.append((path.name, description))
    print(f"  {path.name:34} {path.stat().st_size / 1024:6.0f} KB   {description}")


with sync_playwright() as p:
    browser = p.chromium.launch(channel="chrome")
    page = browser.new_context(
        viewport={"width": 1440, "height": 900},
        device_scale_factor=2,
        color_scheme="light",
    ).new_page()

    page.goto(URL, wait_until="networkidle")
    page.wait_for_selector(".pack", timeout=30_000)
    shot(page, "01-upload", "Step 1 — schema and sample, and the three calibrated packs")

    page.locator(".pack", has_text="European CLO").click()
    page.wait_for_selector("#view-review:not([hidden])", timeout=30_000)
    page.wait_for_timeout(1200)
    shot(page, "02-review", "Step 2 — detected columns, types, key, nullability")

    page.locator("#btn-next").click()
    page.wait_for_selector("#view-configure:not([hidden])", timeout=30_000)
    page.wait_for_timeout(1200)
    shot(page, "03-configure", "Step 3 — five groups, two open, and what the run will produce")

    # Every group open, to show what each holds.
    page.evaluate("document.querySelectorAll('details.group').forEach(g => g.open = true)")
    page.wait_for_timeout(900)
    shot(page, "04-configure-expanded", "Step 3 — every group expanded")

    page.locator("#btn-next").click()
    page.wait_for_selector("#view-generate:not([hidden])", timeout=15_000)
    page.wait_for_timeout(1400)
    shot(page, "05-generate", "Step 4 — the seven stages, progress and estimate", full=False)

    page.wait_for_selector("#view-results:not([hidden])", timeout=300_000)
    page.wait_for_timeout(6000)
    shot(page, "06-results", "Step 5 — summary, validation, and the pack's own six charts")

    page.locator(".rail .step[data-view=download]").click()
    page.wait_for_timeout(1200)
    shot(page, "07-download", "Step 6 — five formats, and the per-period files")

    browser.close()

print()
for name, description in shots:
    print(
        f"| [{description.split(' — ')[0]}](screenshots/{name}) | {description.split(' — ', 1)[-1]} |"
    )
