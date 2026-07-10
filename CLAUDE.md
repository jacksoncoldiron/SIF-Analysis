```markdown
# CLAUDE.md — SIF-Analysis Project

This file orients Claude Code at the start of every session. Read this in full 
before doing anything else. Update the "Session Log" and "Next Session Prompt" 
sections at the end of every working session — this is how continuity is 
maintained across sessions.

---

## Research Goals

**PI / advisor:** Zoe Pierrat (PIE Lab, Bren School, UCSB)
**Researcher:** [Waldorf], MESM Class of 2026, Water Resource Management
**Target venue:** Irrigation Science (Wiley)

**Core question:** Does irrigation buffer crop photosynthesis during drought, 
and does that buffering capacity change as drought severity increases?

**Approach:** Use Human ET (OpenET ensemble minus NLDAS Noah ET) as a satellite-
derived proxy for irrigation, and SIF (solar-induced chlorophyll fluorescence) 
as a proxy for in-season crop photosynthesis. Regress SIF against drought 
severity (SPEI), Human ET, and their interaction across western/central CONUS 
cropland, 2015–2024.

**Study domain:** Western and central CONUS cropland. NOT full CONUS — OpenET 
coverage ends around 84°W, so the eastern Corn Belt, Southeast, and mid-Atlantic 
are excluded. This is being framed as a methodological feature, not a limitation: 
it naturally focuses the study on the primary U.S. irrigation belt (High Plains, 
Central Valley, Columbia Basin, Northern Great Plains).

**Project history:** Started as an Iowa-specific corn analysis. Pivoted to 
CONUS-wide in May 2026 after determining Iowa has relatively little irrigation 
and a larger-scale analysis was more scientifically interesting given the 
resolution of the available data products.

**Headline finding so far (subject to revision):** Irrigation (Human ET) is 
associated with higher SIF, but the benefit shrinks as drought severity 
increases (positive but small SPEI × Human ET interaction term). Likely framing: 
"irrigation buffering breaks down under severe drought" — see Manuscript section 
below for framing options still being discussed with advisor.

---

## Repo / Environment

- Compute: GRIT HPC cluster (UCSB), accessed via Open OnDemand
- SLURM account: `slurm_pielab`
- Working directory: `/home/pielab-sandbox-jcoldiron/SIF-Analysis/` 
  (sandbox arrangement — pending full PIE Lab server provisioning on servers 
  named Apple and Pumpkin; paths will need to migrate once that's available)
- Python 3.13, `.venv`, Jupyter notebooks, GEE (authenticated via 
  `earthengine authenticate --auth_mode=notebook` — required for remote sessions, 
  prints a URL instead of opening a browser)
- GRIT contact for HPC issues: Sebastian Garcia (email, CC'd on technical threads)
- Known GRIT issue: Claude Code crashes on older nodes lacking AVX CPU 
  instructions (Bun runtime segfault). Node assignment is variable, so this is 
  intermittent — if a session crashes immediately on startup, this is the likely cause.

### File organization
```
code/         — functions/ (reusable scripts), notebooks/, scripts/ (numbered by use)
data/raw/     — untouched downloads
data/processed/ — harmonized panel data
figures/      — exported figures and tables
logs/         — SLURM job logs (must exist before submitting jobs, or jobs fail silently)
```
Code and small outputs are pushed to git; bulk data is gitignored.

### Notebook pipeline
- `00_*` — spatial harmonization
- `01_*` — data assembly
- `02_irrigation_sif_regression_conus.ipynb` — regression modeling
- `03_visualizations_conus.ipynb` — publication figures

---

## Data Sources

| Dataset | Resolution | Role | Notes |
|---|---|---|---|
| SIF | 0.05° | Dependent variable | OCO-2/3 GSP-filled monthly product |
| OpenET (ensemble) | 30m → resampled to 0.125° | Total ET (observational) | GEE collection `OpenET/ENSEMBLE/CONUS/GRIDMET/MONTHLY/v2_0`. The DAILY collection is not publicly accessible via GEE — requires explicit access request from OpenET. Use MONTHLY. |
| NLDAS Noah | 0.125° | Natural/modeled ET baseline | `NLDAS_NOAH0125_M` — monthly model output, NOT forcing data |
| NLDAS FORA | 0.125° | Met covariates (Tair, VPD, Wind, SWdown) | `NLDAS_FORA0125_M` — separate product from Noah, not yet downloaded as of last session |
| USDA CDL | ~30m → resampled to 0.125° | Cropland mask, corn/soy fraction | Threshold ≥50% cropland fraction for binary mask |
| SPEI | — | Drought index | SPEI-90d, lower = drier |
| GRIDMET VPD | — | Alternative met covariate via GEE | Script written, not yet run as of last session — may be redundant with NLDAS FORA VPD once that's downloaded; open question, see below |

**Human ET (irrigation proxy) definition:** ΔET = OpenET − NLDAS Noah ET. 
Rationale: OpenET has an observational constraint (satellite-based land surface 
temperature); NLDAS forcing variables don't control what OpenET does, so the 
residual captures water use beyond what natural/rainfed conditions would produce.

**CRS note:** OpenET uses `scale` in meters with EPSG:4326 CRS, which causes GEE 
to misinterpret units — explicit `crs_transform` is required when exporting, or 
resolution artifacts result.

---

## Known Data Quality Issues (do not re-discover these — read this first)

1. **OpenET malformed exports (Iowa-era, resolved):** Early OpenET TIFs had real 
   data only in the westernmost ~0.75° of Iowa, with a single pixel value tiled 
   across the remaining 90% of each file. Root cause: GEE export region/scale 
   misconfiguration. Fixed by re-exporting with explicit `crs_transform`.

2. **NLDAS latitude orientation (resolved):** Early NLDAS files had latitude 
   ascending south→north, which flipped the data upside down. Fixed.

3. **CONUS map border artifacts (active as of last session):** conus_human_et_mean 
   and human_et_trend_map show spurious values outside the true US boundary, 
   especially southern AZ/NM/TX and northern WA/ID. Likely cause: NLDAS pixels 
   that straddle the political border pass the CDL cropland-fraction threshold 
   even though part of the pixel is outside the US. Fix in progress: clip to 
   dissolved US states boundary AFTER masking, not before. See active Claude Code 
   prompt log below.

4. **High collinearity among met covariates:** ACond (aerodynamic conductance) 
   showed an implausibly large coefficient (−15.3) in the expanded regression — 
   flagged as a likely collinearity artifact, not yet resolved. A correlation 
   matrix of all candidate predictors (SPEI, ΔET, Tair, VPD, Wind, SoilM, 
   PotEvap, SWdown) is planned before finalizing the met-variable model.

5. **NLDAS Noah vs. FORA confusion:** Files on hand are Noah LSM *output* 
   (model results: Tair, Qair etc. as modeled), not FORA *forcing* data (the 
   actual input meteorology used to drive the model). These are different 
   products with the same variable names in some cases — easy to conflate. 
   NLDAS_FORA0125_M is the correct product for forcing variables and has not 
   yet been downloaded.

6. **GRIDMET VPD vs. NLDAS FORA VPD — open question:** Not yet resolved whether 
   to use GRIDMET VPD (already scripted, not yet run) or NLDAS FORA VPD (not 
   yet downloaded) or both. Discuss with Zoe — may be redundant.

---

## Regression Results So Far (most recent, do not treat as final)

**Baseline model:**
```
SIF_z ~ SPEI + ΔET + SPEI×ΔET + C(month)
Pooled OLS, HC3 robust SE
N = 434,245   R² = 0.2055   Adj. R² = 0.2055

SPEI-90d:        β = 0.17264  SE = 0.00209  p < .001  ***
ΔET (HumanET):   β = 0.00357  SE = 0.00006  p < .001  ***
SPEI × ΔET:      β = 0.00067  SE = 0.00006  p < .001  ***
```

**Expanded model (with NLDAS Noah met covariates):**
```
SIF_z ~ SPEI + ΔET + SPEI×ΔET + SWdown + AvgSurfT + PotEvap 
        + SoilM_0_10cm + Rainf + ACond + C(month)
N = 434,245   R² = 0.2119   Adj. R² = 0.2119
```
ACond coefficient (−15.3) flagged as likely collinearity artifact — see Known 
Issues #4.

**Pixel-level regressions (drought months only, SPEI ≤ -0.5):**
- 7,456 pixels had ≥5 drought-month observations
- 2,032 reached significance (p < 0.10)
- 83.7% positive slopes (irrigation buffering signal)
- 16.3% negative slopes — flagged as analytically interesting, worth mapping 
  and featuring, not dismissing as noise

**Raw SIF model:** Not yet run as of last session — planned to replace SIF_z 
with raw SIF values in the baseline model and compare R² and coefficients side 
by side in a table.

---

## Model Improvements Under Consideration

Discussed with Zoe but not yet implemented, in rough priority order:
1. Add year fixed effects (control for unusually wet/dry years overall)
2. Add pixel fixed effects (compare each pixel to itself, control for 
   time-invariant local factors like soil type, infrastructure)
3. Cluster spatial standard errors (current HC3 robust SEs don't account for 
   spatial autocorrelation between neighboring pixels — real effective N is 
   smaller than 434,245)
4. Lag analysis — test whether SIF responds before, during, or after drought 
   peaks (Zoe is especially interested in this; references the Liyin paper's 
   panel regression approach as a model to follow)
5. Crop type stratification (corn vs. soy) — CDL fractions already exist, 
   regression split not yet implemented

---

## Manuscript Status

**Target:** Irrigation Science (Wiley)
**Outline:** drafted, bare-bones, organized around 3 main figures + 1 table 
(see manuscript_outline.md if saved separately, or regenerate from this context)

**Framing decision — NOT YET FINALIZED, pending advisor discussion:**
- Option 1 (monitoring/detection): new method to quantify irrigation buffering 
  at scale
- Option 2 (process/threshold): irrigation buffers SIF during moderate drought 
  but breaks down under severe drought — currently the leading candidate, most 
  novel claim, best fit for journal
- Option 3 (trends): Human ET has changed 2015–2024, places with growing 
  irrigation show stronger buffering

**Main figures planned:**
- Figure 1: two-panel CONUS map — Human ET 10-yr mean + trend (in progress, 
  border artifact fix underway)
- Figure 2: pixel-slope buffering map + scatter of irrigation intensity vs. 
  slope strength (slopes already computed; map/scatter visualization pending)
- Figure 3: Human ET decile × drought severity → SIF response (line plot or 
  ridge/violin — design choice pending Zoe's input, see open question below)
- Table 1: regression model comparison (SIF_z vs. raw SIF, baseline vs. 
  expanded) — not yet built

**Outstanding for later (explicitly deferred, not urgent):**
- USDA NASS county yield validation — would require county-level aggregation, 
  noted as a limitation/future direction rather than immediate next step
- Corn/soy stratification — see Model Improvements above

**Open question for Zoe (asked, awaiting answer as of last session):** For 
Figure 3, should the line plot show Human ET decile on x-axis with one line per 
drought category (emphasizes "does irrigation help"), or drought category on 
x-axis with ridge/violin per decile (emphasizes "does drought hurt less with 
more irrigation")? Sent as a Slack question — check for reply before building 
the final version.

---

## Communication Norms

- Zoe (Slack): direct and concise
- GRIT / Sebastian (email): detailed and technical, CC Sebastian on relevant threads
- Claude Code prompts: step-by-step, specific notebook paths and cell insertion 
  points, explicit instructions not to modify existing cells
- Visualization notebooks should be self-contained and independently runnable
- Writing: preserve original sentences, build around them; avoid em dashes; 
  short simple sentences; show edits directly rather than full rewrites

---

## Session Log

*(Append a new dated entry after each working session. Keep entries factual 
and specific — file names, exact error messages, decisions made. This is the 
record that lets a future session pick up without re-deriving context.)*

### 2026-05-04 — 10-year data download & QC
- OpenET and NDVI completed full 2015–2024 download (120 files each)
- CDL 2015–2018 GEE export tasks submitted, pending
- NLDAS Noah not yet re-run for full 10-year range
- Fixed NDVI band name bug (`1_km_monthly_NDVI` → `NDVI`)
- Added QC visualization cells to three notebooks (July spot-check panels)

### 2026-05-20 — CONUS regression results presented to Zoe
- Presented baseline + expanded (met covariates) regression results
- Zoe flagged: R² of ~0.21 is reasonable for SIF anomaly data, not a problem
- Zoe interested in: lag analysis (based on Liyin paper panel regression), 
  crop type stratification, mapping the negative-slope pixels
- Decision: try raw SIF instead of z-scores next
- Decision: pull in NLDAS FORA forcing variables (not just Noah output)
- Journals discussed: Irrigation Science (Wiley) — leading choice, also 
  considered Journal of Hydrology and Environmental Research Letters

### 2026-06-19 — Claude Code prompt issued for visualization overhaul
- Sent a comprehensive Claude Code prompt covering:
  1. Fix CONUS map border artifacts + add state boundaries + fix aspect ratio 
     (EPSG:5070)
  2. Raw SIF regression + comparison table vs. z-score model
  3. New Figure 2 (pixel-slope map + scatter) and Figure 3 (percentile/violin)
  4. NLDAS FORA download script + VPD/Tair/Wind derivation
  5. Expanded regression with FORA met variables
- Manuscript outline drafted (3 figures + 1 table structure), framing options 
  laid out for advisor discussion (today's meeting)
- This CLAUDE.md file created to preserve continuity across sessions

### 2026-07-10 — Publication figure polish + environment fixes

**Environment fixes:**
- `/usr/bin/python3.11` no longer exists on GRIT nodes — system is Python 3.13 only.
  Kernel spec updated: `/home/jcoldiron/.local/share/jupyter/kernels/sif-python311/kernel.json`
  now points to `/home/pielab-sandbox-jcoldiron/.venv/bin/python3` (Python 3.13).
  Kernel display name updated to "SIF Python 3.13 (shared venv)".
- `nbconvert` was missing from both venvs (both created `--without-pip`).
  Installed into shared venv: `pyarrow`, `nbconvert`, `statsmodels`, `contextily`
  (contextily later dropped — see below).
- Run notebooks via:
  ```bash
  PYTHONPATH=/home/pielab-sandbox-jcoldiron/.venv/lib/python3.13/site-packages \
  /home/pielab-sandbox-jcoldiron/.venv/bin/jupyter nbconvert --to notebook --execute --inplace \
    --ExecutePreprocessor.kernel_name=sif-python311 --ExecutePreprocessor.timeout=900 \
    src/notebooks/conus/03_visualizations_conus.ipynb
  ```

**Basemap overhaul (03_visualizations_conus.ipynb, cell 63e90327):**
- Replaced broken `us_states_simple.geojson` (hand-crafted, bad geometry) with
  Natural Earth data fetched at runtime from GitHub:
  - `ne_110m_land.geojson` — land fill
  - `ne_110m_admin_1_states_provinces.geojson` — US state borders (lower 48)
  - `ne_110m_admin_0_countries.geojson` — US/Canada/Mexico borders
- Replaced manual `ax.plot()` boundary drawing with `gdf.boundary.plot()`.
- Final basemap style: white background everywhere, light gray (`#DCDCDC`) fill
  inside US states only, white outside. State lines thin dark gray, country
  borders 2× thicker. No labels, no tiles.
- `contextily` / ESRI NatGeo tile approach was tried and dropped — too busy for
  a publication figure. Simple vector fill is cleaner.
- `_add_basemap`, `_draw_states`, `_albers_axes` helper functions all updated.
- `_boundary_mask` now built from the Natural Earth dissolved US polygon (better
  than the old hand-crafted geojson).

**Figure 2 changes (cell 51e4eb91):**
- Removed panel (b) title — was overlapping with the top marginal histogram.
- Removed LOWESS smoothing line from scatter panel.
- Added gray legend patch to map panel (a): "Not significant (p ≥ 0.10 or < 5
  drought obs)".
- `_add_basemap` call updated (dropped `alpha=` kwarg no longer valid).

**Figure 3 changes (cell 4d40b549):**
- "No Drought" line color changed from dark green (`#2E7D32`) → steel blue
  (`#1565C0`); mild drought shifted to `#F9A825` to avoid clash.
- X-axis changed from decile number (1–10) to actual Human ET in mm/month:
  switched to 20 equal-count bins, each plotted at its median HumanET value.
  X-axis label now "Human ET [mm month⁻¹]".
- Zero reference line changed to bold solid dark gray (`lw=2.0, color='#333333',
  linestyle='-'`) — was dashed light gray.

**NLDAS FORA: still NOT downloaded.** Table 1 (2-model) is complete; 4-model
version awaits FORA data.

### 2026-06-20 — Full visualization overhaul implemented + kernel fix
**Notebook changes (all in 03_visualizations_conus.ipynb):**
- Task 1: Added 4 diagnosis cells (a–d) before Section 4 documenting border 
  artifact root causes. Section 4.1 now builds `_boundary_mask` (point-in-polygon 
  check of each CDL cropland pixel against dissolved 48-state US boundary using 
  shapely). Section 4.3 updated: figsize=(14,5), horizontal colorbars (shrink=0.7), 
  correct unit labels (mm month⁻¹), RdBu_r colormap, exports fig01_human_et_mean_trend.pdf/png
- Task 3 (Fig 2): Fixed groupby bug (for _lat,_lon,grp → for (lat,lon),grp). 
  Scatter now colored by corn_frac (YlGn), LOWESS frac=0.3, marginal histograms 
  added. Export: fig02_pixel_slope_map_scatter.pdf/png
- Task 3 (Fig 3): Line plot adds per-decile n= annotations. Export: 
  fig03_percentile_lines.pdf/png. Violin uses viridis colormap. Export: 
  fig03_ridge_violin.pdf/png

**Notebook changes (02_irrigation_sif_regression_conus.ipynb):**
- Task 2/5: Sections 11 and 12 already existed and were complete. Renamed 
  exports: table_regression_comparison.tex/png (was table1_sif_regression_comparison), 
  supp_correlation_matrix.png (was fora_predictor_correlation_matrix). 
  Section 12 guard cell added (raises AssertionError) — re-enable after 
  downloading NLDAS FORA via 20_download_nldas_fora.py
- Task 4: 20_download_nldas_fora.py already existed and complete. Removed 
  broken pip auto-install fallback; now exits cleanly if earthaccess missing.

**Environment fix (kernel):**
Both .venv directories were created with --without-pip, blocking VS Code's 
ipykernel installer. Fix: created hand-crafted kernel spec at 
/home/jcoldiron/.local/share/jupyter/kernels/sif-python311/kernel.json
Uses /usr/bin/python3.11 with PYTHONPATH combining:
  - /home/pielab-sandbox-jcoldiron/SIF-Analysis/.venv/lib/python3.11/site-packages
    (all project packages: numpy, geopandas, statsmodels, rasterio, etc.)
  - /home/pielab-sandbox-jcoldiron/.venv/lib/python3.11/site-packages
    (ipykernel lives here)
In VS Code: select kernel "SIF Python 3.11 (project venv)" — no pip needed.
Notebooks can also be run from command line (see Next Session Prompt).

**NLDAS FORA (Task 4/5): NOT yet downloaded.**
- Section 12 in 02_ is guarded by AssertionError — safe to run all cells
- When FORA is ready: run 20_download_nldas_fora.py, then comment out the 
  guard cell (id: sec12guard) in 02_ section 12, then re-run section 12
- earthaccess must be installed first: conda install -c conda-forge earthaccess

---

## Next Session Prompt

I'm working on SIF-Analysis at /home/pielab-sandbox-jcoldiron/SIF-Analysis/.
Read CLAUDE.md in full first.

**Last session (2026-07-10):** Publication figure polish. All three figures
regenerated with proper Natural Earth basemaps (white outside US, gray inside,
clean state/country borders). Figure 2 and Figure 3 layout/style updated.

**Environment note:** Kernel is now Python 3.13 (system upgraded from 3.11).
Kernel spec: `/home/jcoldiron/.local/share/jupyter/kernels/sif-python311/kernel.json`
points to `/home/pielab-sandbox-jcoldiron/.venv/bin/python3` with
`PYTHONPATH=/home/pielab-sandbox-jcoldiron/.venv/lib/python3.13/site-packages`.

**To run notebooks from command line:**
```bash
cd /home/pielab-sandbox-jcoldiron/SIF-Analysis
PYTHONPATH=/home/pielab-sandbox-jcoldiron/.venv/lib/python3.13/site-packages \
/home/pielab-sandbox-jcoldiron/.venv/bin/jupyter nbconvert --to notebook --execute --inplace \
  --ExecutePreprocessor.kernel_name=sif-python311 \
  --ExecutePreprocessor.timeout=900 \
  src/notebooks/conus/03_visualizations_conus.ipynb
```

**Start by checking:**
1. Confirm figures look correct (open them):
   - figures/conus/fig01_human_et_mean_trend.png — two-panel Albers, white/gray
     basemap, Natural Earth borders, no artifacts
   - figures/conus/fig02_pixel_slope_map_scatter.png — slope map with gray
     non-sig legend, no LOWESS line, no panel (b) title
   - figures/conus/fig03_percentile_lines.png — HumanET mm/month on x-axis
     (not decile numbers), blue "No Drought" line, bold zero line
2. Check if Zoe has answered the Figure 3 design question (line vs violin) —
   if answered, delete the unchosen version from the notebook
3. Check if NLDAS FORA has been downloaded yet (data/raw/NLDAS_FORA/) —
   if yes, remove the guard cell (id: sec12guard) in 02_ section 12 and run it

**Outstanding work (in priority order):**
1. NLDAS FORA download → section 12 → 4-model comparison table
2. Year + pixel fixed effects (Zoe's model improvement list)
3. Crop type stratification (corn vs soy split)
4. Lag analysis (Zoe specifically interested — see Liyin paper)
```

---

A few notes on how I built this:

- The **Session Log** and **Next Session Prompt** sections are the ones meant to be updated every time — I structured them so a future Claude Code session reads the static project context once, then jumps straight to the log to see what actually happened last.
- I pulled real numbers (R², coefficients, pixel counts) from your progress doc rather than inventing placeholders, so this file is actually useful as ground truth, not just a template.
- I flagged open questions (GRIDMET vs. FORA VPD, the Figure 3 design choice) explicitly so Claude Code doesn't silently pick one and diverge from what you and Zoe land on.

Want me to also draft a short reusable template (just the Session Log entry format) you can paste in quickly at the end of future sessions without rewriting the whole file?