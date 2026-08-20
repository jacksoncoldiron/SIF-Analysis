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
- **Python: use the `sif` conda env** — `/home/jcoldiron/miniforge3/envs/sif/bin/python`
  (Python 3.11). The two `.venv` directories are DEAD: they were built against
  `/usr/bin/python3.13` and `/usr/bin/python3.11`, both of which GRIT has since
  removed, so every compiled package fails to import. See the 2026-08-20 log.
  Add packages with `mamba install -y -n sif -c conda-forge <pkg>`.
- Jupyter notebooks: run with that env's jupyter and
  `--ExecutePreprocessor.kernel_name=python3`
- GEE: `earthengine authenticate --auth_mode=notebook` (prints a URL instead of
  opening a browser). **The stored token is currently EXPIRED** and needs an
  interactive re-auth before any new GEE export.
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
| NLDAS FORA | 0.125° | Met covariates (Tair, VPD, Wind, SWdown) | `NLDAS_FORA0125_M` — separate product from Noah. DOWNLOADED 2026-08-20, all 120 months, in `data/processed/conus/nldas_fora/`. NetCDF uses descriptive variable names (Tair, Qair, PSurf, Wind_E, Wind_N, SWdown), not GRIB short names |
| USDA CDL | ~30m → resampled to 0.125° | Cropland mask, corn/soy fraction | Threshold ≥50% cropland fraction for binary mask |
| SPEI | — | Drought index | SPEI-30d / 90d / 180d all in the panel, lower = drier. 180d came from the Climatology Lab direct NetCDF (no GEE auth needed), see `05b_process_spei180d.py`. 60d is NOT available in GRIDMET/DROUGHT |
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

3. **CONUS border artifacts (RESOLVED 2026-08-20 — and it was worse than a map 
   problem):** NLDAS pixels straddling the political border passed the CDL 
   cropland-fraction threshold even though part of the pixel lies outside the US. 
   The clip was only ever applied when *drawing maps*, so those pixels stayed in 
   the regression sample the whole time.
   
   Scale: `df_combined_gs.parquet` holds 25,153 unique pixels, but only ~9,250 
   fall inside a lower-48 state polygon. The other ~15,900 are ocean, Mexico and 
   Canada cells. They are ~98% empty, but the ~2% carrying values entered the 
   fit — the published N = 434,245 includes **14,160 out-of-CONUS observations 
   (3.3%)**.
   
   Fixed by defining the study domain as whole states and applying that clip to 
   the analysis sample, not just the basemap. See the 2026-08-20 session log for 
   the effect on the coefficients (it strengthens the headline result).
   
   This also explains the old "RZSM 55% fill rate" puzzle: RZSM is 87–90% present 
   on real CONUS pixels. The 55% was an artifact of averaging in the junk pixels. 
   Same for SPEI-180d (34% overall, 90% on real pixels). **Never quote a fill 
   rate or an N from this panel without restricting to in-domain pixels first.**

4. **High collinearity among met covariates:** ACond (aerodynamic conductance) 
   showed an implausibly large coefficient (−15.3) in the expanded regression — 
   flagged as a likely collinearity artifact, not yet resolved. A correlation 
   matrix of all candidate predictors (SPEI, ΔET, Tair, VPD, Wind, SoilM, 
   PotEvap, SWdown) is planned before finalizing the met-variable model.

5. **NLDAS Noah vs. FORA confusion (FORA now downloaded 2026-08-20):** Noah files 
   are LSM *output* (modeled Tair, Qair etc.); FORA is the *forcing* meteorology 
   that drives the model. Different products, overlapping variable names — easy 
   to conflate. All 120 months of NLDAS_FORA0125_M are now on disk.
   
   **Variable-name trap:** the GES DISC NetCDF distribution uses descriptive 
   names (`Tair, Qair, PSurf, Wind_E, Wind_N, SWdown, Rainf, PotEvap, LWdown`), 
   NOT the GRIB short names (`TMP, SPFH, PRES, UGRD, VGRD, DSWRF`). 
   `20_download_nldas_fora.py` originally used the GRIB names and silently 
   skipped all 120 files, reporting "Processed: 0" without an error. Fixed.

6. **GRIDMET VPD vs. NLDAS FORA VPD — open question:** Not yet resolved whether 
   to use GRIDMET VPD (already scripted, not yet run) or NLDAS FORA VPD (not 
   yet downloaded) or both. Discuss with Zoe — may be redundant.

---

## Regression Results So Far (most recent, do not treat as final)

> **SUPERSEDED 2026-08-20.** Everything in this section below the current-state
> block was fit on the UNCLIPPED sample and therefore includes 14,160
> out-of-CONUS observations (Known Issue #3). Do not quote these numbers.
> The clipped + fixed-effects results are the current ones.

**CURRENT — clipped to the 30-state study domain (N = 419,721):**
```
                        SPEI-90d    ΔET       SPEI×ΔET    R²
Pooled OLS, month FE     0.16575   0.00341    0.00106   0.2082
+ Year FE                0.15734   0.00360    0.00108   0.2263
Pixel + Year + Month FE  0.17223   0.00604    0.00127   0.2319 (within)
```
All p < .001. SE clustered by pixel for the FE rows; HC3 for the pooled row.
7,333 pixel effects absorbed by within-transformation, not dummies.
The FE R² is a WITHIN R² and is not comparable to the pooled R².

Marginal effect of +1 mm month⁻¹ Human ET on SIF z (pixel+year FE):
0.00604 at SPEI = 0 falling to 0.00349 at SPEI = −2.0 — a 42% reduction.
That is the buffering-breakdown result, net of every time-invariant
difference between locations.
Source: `src/scripts/08_build_pixel_states_and_fe.py` and Section 8 of the
03_ notebook. Table: `figures/conus/table_fixed_effects_comparison.csv`.

---

**SUPERSEDED — baseline model, unclipped sample:**
```
SIF_z ~ SPEI + ΔET + SPEI×ΔET + C(month)
Pooled OLS, HC3 robust SE
N = 434,245   R² = 0.2055   Adj. R² = 0.2055

SPEI-90d:        β = 0.17264  SE = 0.00209  p < .001  ***
ΔET (HumanET):   β = 0.00357  SE = 0.00006  p < .001  ***
SPEI × ΔET:      β = 0.00067  SE = 0.00006  p < .001  ***
```
Reproduced exactly by Model A of `08_build_pixel_states_and_fe.py`, which is
what validates the clipped-vs-unclipped comparison above.

**SUPERSEDED — expanded model (with NLDAS Noah met covariates), unclipped:**
```
SIF_z ~ SPEI + ΔET + SPEI×ΔET + SWdown + AvgSurfT + PotEvap 
        + SoilM_0_10cm + Rainf + ACond + C(month)
N = 434,245   R² = 0.2119   Adj. R² = 0.2119
```
ACond coefficient (−15.3) flagged as likely collinearity artifact — see Known 
Issues #4.

**Pixel-level regressions (drought months only, SPEI ≤ -0.5) — unclipped:**
- 7,456 pixels had ≥5 drought-month observations
- 2,032 reached significance (p < 0.10)
- 83.7% positive slopes (irrigation buffering signal)
- 16.3% negative slopes — flagged as analytically interesting, worth mapping 
  and featuring, not dismissing as noise

**Per-drought-category pixel slopes (clipped, current — drives Figure 2):**
```
No Drought  (SPEI > -0.5)        2,688 sig. pixels   (2,000 pos / 688 neg)
Mild        (-1.0 < SPEI ≤ -0.5) 1,395 sig. pixels   (1,175 pos / 220 neg)
Moderate    (-1.5 < SPEI ≤ -1.0)   688 sig. pixels     (595 pos /  93 neg)
Severe      (SPEI ≤ -1.5)           68 sig. pixels      (50 pos /  18 neg)
```
The Severe count is low because few pixels reach MIN_OBS_PIXEL = 5 observations
at SPEI ≤ -1.5 — a sampling limit, not a null result. Open design question:
whether the Severe panel earns a place in Figure 2 at all.

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

### 2026-07-31 (session 2) — Figure overhaul fully implemented

**All notebook changes made this session. Three new scripts written.**

**ltc palette (03_visualizations_conus.ipynb, cell ce29a1bd):**
- `PALETTE_PLOEN = ['#3F5671','#83A1C3','#CEB5C8','#FAC898','#B17776']`
- `PALETTE_HEATMAP0 = ['#001219','#005F73','#0A9396','#94D2BD','#E9D8A6','#EE9B00','#CA6702','#AE2012','#9B2226']`
- `cmap_ploen`, `cmap_heatmap0` colormaps + NaN-transparent `_map` copies
- `DROUGHT_COLORS_LTC` dict for Figure 3 line colors
- Hex codes verified from CRAN tarball (ltc 0.4.0 / R/ltc_functions.R)

**Basemap helpers updated (63e90327):**
- Left xlim tightened: `_xmin = X5070.min() - 10_000` (was -80_000) — CA now at left edge
- `_us_dissolved_5070` added (needed for boundary outline)
- `_east_x, _east_y` computed (longitude -84.1875° in Albers) for eastern cutoff line
- New `_draw_study_boundary(ax)` helper — draws dissolved US outline + eastern OpenET cutoff

**Figure 1 split into 3 separate exports:**
- fig01a_human_et_mean.png/pdf — ploen colormap, no title, bold "(a)"
- fig01b_human_et_trend.png/pdf — heatmap0 colormap, no title, bold "(b)"
- fig01c_mean_vs_trend_scatter.png/pdf — new scatter, CA=ploen[4], Iowa=ploen[0], gray=other
- supp_table_state_stats.csv — min/median/max mean ET + trend per state, >25 pixels

**Figure 2 replaced with 4 separate drought-category slope maps:**
- Computation cell (5fb71653): per-category pixel regressions using _pixel_slope_series()
  for No Drought/Mild/Moderate/Severe, stored in `pix_slopes_by_cat` dict
- Figure cell (51e4eb91): 4 maps using heatmap0 colormap, shared vmax, study boundary outline
  → fig02_slope_{no_drought,mild,moderate,severe}.png/pdf

**Figure 3 updated (4d40b549):**
- Filter to delta_et >= 0 (x-axis starts at 0)
- Removed marker='o' (lines only)
- Rug marks at bottom using `ax.plot(..., '|')`
- ploen palette for drought categories via DROUGHT_COLORS_LTC

**New scripts written:**
- `src/scripts/download/05_download_spei_multiperiod.py`: downloads SPEIbase v2.11
  (CSIC) SPEI 1-month, 2-month, 6-month; regrids 0.5° → 0.125°; saves
  spei_conus_month_{30d,60d,180d}.nc
- `src/scripts/download/06_extract_nldas_soilm.py`: extracts SoilM_0_100cm (or
  RootMoist fallback) from raw NLDAS_NOAH0125_M.A{YYYYMM}.020.nc; saves per-month
  processed files + rzsm_conus_gs.nc stacked file
- `src/scripts/download/07_extend_panel_parquet.py`: merges new columns
  (spei30d, spei60d, spei180d, rzsm, rzsm_z) into df_combined_gs.parquet in-place

**Multi-index section added to 03_ notebook (Section 7, cells 5a8ac2b0+):**
- Guarded — prints "NOT YET" for missing columns and skips cleanly
- Generates Fig 2 (4 maps) and Fig 3 (line plot) for every available index
  → fig02_{spei30d,spei60d,spei90d,spei180d,rzsm_z}_slope_{category}.png/pdf
  → fig03_{index}_percentile_lines.png/pdf

**NLDAS SoilM variable name:** confirmed raw files exist at
`data/raw/nldas/NLDAS_NOAH0125_M.A{YYYYMM}.020.nc`. Script auto-detects
variable (tries SoilM_0_100cm, RootMoist, SoilM_0_10cm in order).

**SPEI source clarification:** SPEIbase from CSIC (spei.csic.es, v2.11, 0.5°
global). Downloaded as spei_1.nc, spei_2.nc, spei_6.nc and regridded.
Note: the original SPEI_Data.nc from sbg-coba only has spei_90d — this
is why separate downloads are needed for other accumulation periods.

**Still NOT done (in order):**
1. Actually run the 3 new scripts (needs manual execution)
2. Confirm NLDAS SoilM variable name once 06_ script runs
3. NLDAS FORA download → 4-model regression table (sec12 guard still in place)
4. Year + pixel fixed effects
5. Crop type stratification
6. Lag analysis

### 2026-08-07 — Visualization notebook run; aesthetic patch script written

**Visualization notebook run by user.** Figures now exist in figures/conus/:
- fig01a/b/c (Human ET mean, trend, scatter)
- fig02_spei90d_slope_{no_drought,mild,moderate,severe}.png/pdf
- fig03_spei90d_percentile_lines.png/pdf
- spei30d and rzsm_z variants also generated (Section 7 activated)

**RZSM deprioritized.** 55% fill rate is likely NLDAS land-ocean mask — some CDL
cropland pixels fall on NLDAS water cells. Not worth fixing now; SPEI variants
are the primary drought indices for the manuscript.

**Aesthetic changes requested by user (NOT YET APPLIED):**
The patch script `src/scripts/patch_viz_notebook.py` encodes all changes:
1. Double all font sizes (axes labels, titles, colorbar labels, legends)
2. White map background (remove #DCDCDC state fill)
3. Fix eastern cutoff line — clip to US boundary using shapely intersection
4. Higher Human ET = blue, lower = red (reversed ploen colormap for fig01a)
5. Positive slopes = blue, negative = red (RdBu diverging for fig01b + fig02)
6. Non-significant pixels → darker grey (#888888, not #DCDCDC)
7. Fig 3 legend: move from 'lower right' to 'upper left'

**Bash tool broken this session.** All Claude Code bash commands fail with
exit code 144 (Bun SIGSYS — node lacks required CPU instructions). Python
itself works fine when run directly from user terminal. Fix: reconnect from
a different GRIT node. Caused significant delays — diagnosed early next time.

### 2026-08-01 — Pipeline scripts run; new drought indices added to parquet

**Three pipeline scripts run and debugged. df_combined_gs.parquet extended.**

**Key discovery — grid mismatch (now fixed):**
Raw NLDAS Noah files and GRIDMET drought TIFs are BOTH on the full CONUS NLDAS
grid (224 lat × 464 lon, spanning -125°W to -67°W, 25°N to 53°N). The analysis
study domain is a western subset (189 lat × 325 lon, -124.6875°W to -84.0625°W).
All three scripts had to be corrected to handle this:
- Script 05: uses `rasterio.warp.reproject` to regrid TIFs from 224×464 → 189×325
- Script 06: uses `xarray.sel(method='nearest')` to subset NetCDF by lat/lon coords

**SPEI source change (important):**
The CSIC SPEIbase URL used in the original script 05 returned HTTP 404. Root cause:
wrong URL format (unknown current format). Fixed by switching sources:
- SPEI-30d: extracted from existing GRIDMET/DROUGHT monthly TIFs (band 3 = spei30d),
  already on disk from 19_download_drought_conus.py. Script 05 rewritten to use this.
- SPEI-60d: NOT available in GRIDMET/DROUGHT (no 60d accumulation band). Skipped.
- SPEI-180d: In GRIDMET/DROUGHT as band 'spei180d' but NOT in existing TIFs (script
  19 only downloaded 8 bands). Needs GEE re-download via earthengine-api, which is
  NOT installed in the venv. MISSING.
- SPEI-90d: already in parquet as 'spei90d' from the 01_ notebook.

**RZSM (SoilM_0_100cm) confirmed:**
Auto-detection in script 06 found SoilM_0_100cm immediately (first candidate).
Full variable list from NLDAS Noah files now documented in 2026-07-31 session log.

**Results after running all three scripts:**
- `spei_conus_month_30d.nc`: 60 growing-season months, 189×325, variable=spei_30d ✓
- `rzsm_conus_gs.nc`: 60 growing-season months, 189×325, variable=rzsm ✓
- `spei_conus_month_60d.nc`: MISSING (GRIDMET has no 60d SPEI band)
- `spei_conus_month_180d.nc`: MISSING (needs GEE re-download)

**Parquet (df_combined_gs.parquet) after script 07:**
- Shape: (1,509,180, 15) — was 12 columns, now 15
- New columns: spei30d (100% fill), rzsm (55% fill), rzsm_z (55% fill)
- spei60d and spei180d skipped (files absent, script 07 handles gracefully)

**RZSM 55% fill rate — needs investigation next session:**
13,843 of 25,153 cropland pixels have RZSM data. Possible causes:
(a) NLDAS Noah SoilM has land-ocean mask: ocean/lake/water pixels = NaN,
    and some CDL "cropland" pixels overlap water bodies in NLDAS grid
(b) Lat/lon float32 precision mismatch in the lookup dict in _add_index_column
(c) Subset of NLDAS grid for study domain doesn't cover all crop pixels
Before trusting RZSM figures, plot a map of which pixels have RZSM vs. not
and compare to the crop mask to diagnose.

**earthengine-api NOT in venv:**
Script 05 GEE section failed with "earthengine-api not installed". The 01_/19_
scripts likely worked because they add ~/.local/lib/python3.12/site-packages to
sys.path before importing ee. To fix: either install ee into the shared venv, or
add the same sys.path hack to script 05's SPEI-180d section.
Fix command (if needed): ~/.venv/bin/pip install earthengine-api
(but venv was created --without-pip, so this may fail — check first).

**Section 7 in 03_ notebook now active:**
The multi-index guard cells check for spei30d and rzsm_z in the parquet. Both
now present, so Section 7 will generate Fig 2 and Fig 3 for those indices.
NOT YET RUN — notebook has not been executed since parquet was extended.

**NLDAS FORA: still NOT downloaded.**
Still guarded by AssertionError in 02_ section 12.

### 2026-07-31 — Figure overhaul requested, session interrupted before implementation

**Session was interrupted early — no notebook changes were made this session.**

User issued a large figure overhaul request. Claude read the current notebook
state (via subagent) and began looking up ltc color palette hex codes from GitHub
before the session was paused. Status: research phase only, no edits committed.

**Full change list requested (to be implemented next session):**

**Color palette (all figures):**
- Use `ltc` R package palettes (https://github.com/loukesio/ltc-color-palettes).
  This is an R package — extract hex codes and hardcode them in Python (no R dep).
  - `ploen` palette: for maps/plots where data runs 0 and up (Human ET mean,
    Figure 3 line graph drought severity colors)
  - `heatmap0` palette: for any diverging map (negative → positive), including
    Human ET trend map and per-pixel slope map
- Each figure notebook cell should define these palette hex lists at the top.

**Map design (all maps):**
- White background everywhere (already done for basemap fill outside US)
- Zoom in: CA should sit at the left edge of the panel — current maps have excess
  white space to the left of CA. Adjust xlim in Albers projection accordingly.
- Add bold study-area boundary outline that also wraps around the eastern cutoff
  (~84°W) where OpenET coverage ends, not just the US political border.

**Figure 1 — split into 3 separate exported figures (not one 2-panel):**
- fig01a_human_et_mean.png/pdf — 10-yr growing-season mean Human ET map
  - No title above map; bold "(a)" label only
  - `ploen` colormap (0 → max)
  - White background, zoomed in
- fig01b_human_et_trend.png/pdf — trend in growing-season Human ET
  - No title; bold "(b)" label only
  - `heatmap0` colormap (diverging, centered at 0)
- fig01c_mean_vs_trend_scatter.png/pdf — NEW scatter figure
  - X-axis: 10-yr mean Human ET (mm/month)
  - Y-axis: trend in growing-season Human ET (mm/month/year)
  - California pixels colored with `ploen` palette; Iowa pixels colored with
    a different `ploen` color; all other pixels gray
  - Purpose: call out CA and Iowa statistics in the written results

**Figure 1 supplemental table (new):**
- Per-state table: min, max, median of both Human ET mean AND trend
- Include only states with >25 valid cropland pixels
- Export as supp_table_state_stats.csv (and optionally .tex)

**Figure 2 — split into separate maps, one per drought category:**
- Current: 1 map (all drought months combined) + scatter → REPLACE with:
  - 4 separate maps, one per drought category:
    - fig02_slope_no_drought.png/pdf — SPEI > -0.5
    - fig02_slope_mild.png/pdf — -1.0 < SPEI ≤ -0.5
    - fig02_slope_moderate.png/pdf — -1.5 < SPEI ≤ -1.0
    - fig02_slope_severe.png/pdf — SPEI ≤ -1.5
  - All maps use `heatmap0` colormap, white background
- ALSO create the same 4-map set for additional drought indices:
  - SPEI-30d, SPEI-60d, SPEI-90d (current), SPEI-180d → need download scripts
    for 30d, 60d, 180d accumulation periods
  - RZSM (Root Zone Soil Moisture) → need to identify data source and download
    **OPEN QUESTION: confirm RZSM source (NLDAS? GLEAM? NASA SMAP?) before
    writing download script**
- This requires re-running the pixel-level regression for each drought index
  and each category separately. May need a new regression section or notebook.

**Figure 3 — line graph updates:**
- X-axis: start at 0 (not -20), keep mm/month units
- Remove markers/points from lines (lines only)
- Add rug/hash mark distribution at bottom of plot showing data density
- ALSO produce this figure for every drought index variant (same set as Fig 2):
  SPEI-30d, SPEI-60d, SPEI-90d, SPEI-180d, RZSM

**Implementation order (recommended):**
1. Extract ltc hex codes (ploen + heatmap0) from R package source on GitHub;
   hardcode as Python lists in a shared palette cell at top of 03_ notebook
2. Implement Fig 1a, 1b map changes (palette swap, zoom, outline)
3. Implement Fig 1c scatter (new) + supplemental table
4. Implement Fig 2 split into 4 drought-category maps using SPEI-90d (existing data)
5. Clarify RZSM source with user, then write download scripts for SPEI-30/60/180d + RZSM
6. Run harmonization + pixel regressions for new drought indices
7. Generate Fig 2 and Fig 3 variants for all drought indices

### 2026-08-20 — Environment rebuilt; both datasets landed; border artifact traced into the regression

**Environment: both .venv directories are dead — use the `sif` conda env.**
GRIT nodes now ship only Python 3.12. The shared venv was built against
`/usr/bin/python3.13` and the project venv against `/usr/bin/python3.11`; both
interpreters are gone, so `bin/python3` resolves to 3.12 while `lib/` still
holds cp311/cp313 site-packages. Every compiled extension (numpy, rasterio,
geopandas, netCDF4, pyarrow) fails to import. This is the third time a GRIT
upgrade has broken these venvs.

Use instead: `/home/jcoldiron/miniforge3/envs/sif/bin/python` (Python 3.11,
conda-managed, ships its own interpreter). Added `pyarrow`, `nbconvert`,
`earthaccess` via `mamba install -n sif -c conda-forge`. Run notebooks with
that env's jupyter and `--ExecutePreprocessor.kernel_name=python3`.
The `PROJ: proj_create_from_database` warning GDAL prints is benign.

**NLDAS FORA: DONE.** All 120 months (2015-2024) downloaded and processed to
data/processed/conus/nldas_fora/FORA_YYYYMM.nc.
Root cause of the earlier stall: `20_download_nldas_fora.py` looked for GRIB
short names (TMP, SPFH, PRES, UGRD, VGRD, DSWRF). The GES DISC NetCDF
distribution uses descriptive names (Tair, Qair, PSurf, Wind_E, Wind_N,
SWdown). The script reported "Processed: 0 / Errors: 0" and exited cleanly —
a silent no-op. Fixed, and Rainf / PotEvap / LWdown added while there.
QC (Jan 2020): Tair mean 275 K, SWdown mean 99 W/m2 — physically sensible.

**SPEI-180d: DONE, without Earth Engine.** The GEE refresh token in
~/.config/earthengine/credentials is expired and needs interactive
`earthengine authenticate` — not runnable from a Claude Code session.
Bypassed entirely: the Climatology Lab serves the same gridMET product as a
direct 6.2 GB NetCDF with no auth:
    https://www.northwestknowledge.net/metdata/data/spei180d.nc
New script `src/scripts/download/05b_process_spei180d.py` selects growing-season
pentads, averages to calendar months, and bilinearly regrids 1/24 deg -> 0.125
deg. Output structure matches spei_conus_month_30d.nc exactly (dims time/lat/lon,
datetime64 time coord, float32 lat/lon) so 07_extend_panel_parquet.py ingests it.
Panel is now (1,509,180 x 16); spei180d is 90.1% filled on real CONUS pixels.
THREDDS OPeNDAP subsetting was attempted first and abandoned — gridMET is not
in that catalog.

**Border artifact was never just a map problem — see Known Issue #3.**
Only ~9,250 of the panel's 25,153 pixels are inside a lower-48 state. The clip
had only ever been applied when drawing maps, so 14,160 out-of-CONUS
observations (3.3%) were in the published N = 434,245. Model A in
`src/scripts/08_build_pixel_states_and_fe.py` reproduces the published
coefficients exactly, which validates the comparison.

Clipping strengthens the headline result — the interaction term grows ~60%.

**Fixed-effects models (new Section 8 in 03_ notebook, N = 419,721):**
```
                     SPEI-90d    HumanET    SPEI x HumanET    R2
Pooled OLS (clipped)  0.16575    0.00341       0.00106      0.2082
+ Year FE             0.15734    0.00360       0.00108      0.2263
Pixel + Year FE       0.17223    0.00604       0.00127      0.2319 (within)
```
All p < .001, SE clustered by pixel, 7,333 pixel effects absorbed by
within-transformation (not dummies). Controlling for location makes the
irrigation effect STRONGER (HumanET nearly doubles), which rules out the
"irrigated places are just different places" confound rather than merely
acknowledging it. Under pixel+year FE the irrigation benefit at SPEI = -2.0 is
42% smaller than under no drought.
Exported: figures/conus/table_fixed_effects_comparison.csv

**Study domain redefined as WHOLE STATES (30 states).**
The eastern edge used to be a straight cut at the -84.1875 deg meridian, which
sliced through the middle of several states. The domain is now the union of
complete state polygons, so every edge of the boundary is a real state or
national border. A state qualifies if it holds >=1 valid cropland pixel AND has
>=50% of its area west of the OpenET cutoff. That rule drops Ohio, Georgia and
Florida — 128 pixels (1.4%) — and avoids pushing the map ~500 km east for
slivers. Constants live in the basemap cell: EAST_CUTOFF_LON,
MIN_STATE_AREA_WEST, STUDY_STATES.
Natural Earth tier raised 110m -> 50m; at 110m the polygons are too generalised
to assign 0.125 deg pixels (65% of pixels landed outside every polygon).

`data/processed/conus/regression/pixel_states.parquet` is a new reusable lookup
(lat, lon, state, in_conus).

**Figure changes (03_visualizations_conus.ipynb, patches v3 + v4):**
- All maps: whole-state boundary, tighter zoom (CA flush left), white band
  reserved above the domain so panel labels clear the Canada border
- fig01a / fig01b: added `_heatmap0` colour variants alongside the existing ones
- fig01c: larger axis/legend text, alpha-based density instead of opaque
  markers, CA + Iowa least-squares fit lines with white casing
- fig01d: NEW — 2015 mean growing-season Human ET vs trend, robust y-limits
- fig02: boundary/zoom formatting, colourbar text matched to fig01
- fig02b: NEW — 3x3 bivariate maps (per-pixel slope x mean drought), one per
  index. heatmap0 is a 1-D ramp so it cannot encode two axes; the grid is a
  bilinear RGB blend between four heatmap0-derived corners, giving a legend
  that reads as a proper 2-D square
- fig03: legend moved inside at lower right with reserved bottom headroom;
  NEW California-only and Iowa-only case-study variants
- Supplemental state table rewritten as a vectorised sjoin (was a nested loop
  over every pixel x every state polygon)

**fig02 grey-fill bug (found and fixed in patch v5) — worth remembering:**
`_cmap_s.set_bad('#888888')` colours EVERY NaN in the 189x325 array, and every
non-cropland cell is NaN too, so the "not significant" grey flooded the entire
map rectangle rather than marking only cropland pixels that failed
significance. It went unnoticed for months because the grey used to be a
near-white #DCDCDC that read as a background; switching to the darker #888888
made it obvious. Any map that needs to distinguish THREE states (valued /
present-but-null / absent) needs two pcolormesh layers — set_bad alone cannot
express it.

**fig02 Mild and Moderate maps were BLANK — pre-existing bug, fixed in v6.**
The per-category regression cell (5fb71653) keys its results dict on strings
that embed the threshold text, e.g. `'Mild\\n(-1.0<SPEI<=-0.5)'`. The figure
cell (51e4eb91) re-typed those keys by hand and dropped a minus sign in two of
them: `'Mild\\n(-1.0<SPEI<=0.5)'` and `'Moderate\\n(-1.5<SPEI<=1.0)'`. Because
the lookup is `pix_slopes_by_cat.get(key, pd.DataFrame())`, the miss returned
an empty frame instead of raising, so both panels rendered as all-grey with
"n=0 sig. pixels" — while 1,395 (Mild) and 688 (Moderate) significant pixels
sat unused in the dict. No Drought and Severe were typed correctly, which is
why only two of four panels looked wrong and nobody caught it.

Any previously exported fig02_slope_mild / fig02_slope_moderate is invalid.
The fix derives the lookup keys from `_CAT_DEFS_FIG2` instead of re-typing
them, and asserts every key resolves, so the two lists cannot drift again.
General lesson for this codebase: `.get(key, default)` on a dict keyed by
hand-typed display strings fails silently. Prefer deriving keys from the
single definition, and assert.

**Severe-drought map is nearly empty — a real result, not a rendering bug.**
fig02_slope_severe has only 68 significant pixels, because few pixels reach
MIN_OBS_PIXEL = 5 observations at SPEI <= -1.5. Decide whether that panel earns
its place in Figure 2, or whether severe drought is better carried by the
regression and Figure 3, which pool across pixels instead of requiring a
per-pixel fit. Same caveat applies to the severe panels of every index variant.

**Scripts added this session:**
- `src/scripts/08_build_pixel_states_and_fe.py`
- `src/scripts/download/05b_process_spei180d.py`
- `src/scripts/patch_viz_notebook_v3.py` through `_v7.py`

**Still open:**
- GEE re-auth (only needed if a new GEE export is required; SPEI-180d no longer
  depends on it)
- NLDAS FORA data is on disk but the 4-model regression table is NOT built —
  Section 12 of 02_ still has the `sec12guard` AssertionError in place
- Crop type stratification; lag analysis
- fig01d shows a negative slope partly by construction (trend is computed from
  a window that includes the first year, so regression to the mean contributes).
  Worth a sentence in the caption or computing the trend on 2016-2024 instead.


---

## Next Session Prompt

I'm working on SIF-Analysis at /home/pielab-sandbox-jcoldiron/SIF-Analysis/.
Read CLAUDE.md in full first, especially the 2026-08-20 session log entry.

**Environment — do this first, it has bitten three sessions in a row:**
Use `/home/jcoldiron/miniforge3/envs/sif/bin/python` for everything. Do NOT use
either `.venv` directory; they are built against Python versions GRIT has
removed and every compiled package fails to import. Verify with:
```
/home/jcoldiron/miniforge3/envs/sif/bin/python -c "import geopandas, rasterio, pyarrow, statsmodels"
```
Run notebooks with:
```
/home/jcoldiron/miniforge3/envs/sif/bin/jupyter nbconvert --to notebook --execute --inplace \
  --ExecutePreprocessor.kernel_name=python3 --ExecutePreprocessor.timeout=7200 \
  src/notebooks/conus/03_visualizations_conus.ipynb
```

**Status as of 2026-08-20:**
- NLDAS FORA (120 months) and SPEI-180d are both downloaded and merged
- The 03_ visualization notebook runs clean end to end (31/31 cells)
- Study domain is now 30 whole states; the analysis sample is clipped to it
- Fixed-effects models are implemented in Section 8 of the 03_ notebook

**Priority order:**

1. **Review the regenerated figures with Zoe.** Especially fig02b (new bivariate
   maps) and the fig03 California / Iowa case-study panels — both are new
   designs that have not had advisor input yet.

2. **The out-of-CONUS contamination needs to be reported, not just fixed.**
   Every regression number in the manuscript draft predates the clip. Rerun
   anything quoted and update the draft. The clip helps: the interaction term
   grows ~60%.

3. **Build the 4-model regression table with FORA covariates.** Data is on disk
   at data/processed/conus/nldas_fora/. Section 12 of
   02_irrigation_sif_regression_conus.ipynb is still guarded by an
   AssertionError in the cell with id `sec12guard` — remove it, then run.
   Compute VPD from Tair/Qair/PSurf and wind from Wind_E/Wind_N (formulas are
   in the 20_ script docstring). Check the collinearity issue (Known Issue #4)
   with the correlation matrix before trusting coefficients.

4. **Decide the fig01d framing.** Its negative slope is partly regression to the
   mean, since the trend window includes the baseline year. Either caption it or
   recompute the trend over 2016-2024.

5. Crop type stratification (corn vs soy), then lag analysis (Zoe's priority,
   Liyin paper panel regression as the model).

**Open questions for Zoe (unchanged, still unanswered):**
- Figure 3 orientation: Human ET decile on x with one line per drought category,
  or drought category on x with ridge/violin per decile?
- GRIDMET VPD vs NLDAS FORA VPD — now that FORA is on disk, is GRIDMET VPD
  still wanted, or is it redundant?

---

## STAGED, NOT YET EXECUTED (as of 2026-08-20)

`src/scripts/patch_viz_notebook_v8.py` has been applied to the notebook source
but the figures on disk predate it. v8 gives panel labels and the fig02 legend
`zorder=15` so the state/country strokes (zorder 5-10) stop painting over them;
without it the BC coastline is stroked across the label's white backing box.

The exported PNG/PDFs are otherwise current as of run 4 (v3-v7 applied,
31/31 cells clean). Re-run the notebook to pick v8 up — it takes ~70 minutes,
so batch it with whatever other figure feedback comes back from Zoe rather than
running it for the zorder fix alone.
