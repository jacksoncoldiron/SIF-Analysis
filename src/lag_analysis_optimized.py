# MEMORY-OPTIMIZED Lag Analysis: SIF response to drought with 6, 8, 10, and 12 week lags
# Following Parazoo et al. (2024) patterns
# This version is optimized to prevent kernel crashes by managing memory more efficiently

import gc
from scipy import stats
from rasterio.enums import Resampling

# Ensure file_info exists
if 'file_info' not in globals():
    raise NameError("file_info is not defined. Please run Cell 0 (Quick Setup) first.")

# Create file_info_2023 if it doesn't exist
if 'file_info_2023' not in globals():
    YEAR_TARGET = "2023"
    file_info_2023 = [info for info in file_info if info["year"] == YEAR_TARGET]
    if not file_info_2023:
        raise ValueError("No SIF files found for 2023 in file_info.")

# Define lags in half-months (each half-month is ~2 weeks)
# 6 weeks = 3 half-months, 8 weeks = 4, 10 weeks = 5, 12 weeks = 6
lags_halfmonths = {
    6: 3,
    8: 4,
    10: 5,
    12: 6
}

# Sort file_info_2023 chronologically
file_info_2023_sorted = sorted(file_info_2023, key=lambda x: (x["year"], x["month"], x["half"]))

def get_lagged_sif_drought_pairs_optimized(lag_halfmonths):
    """Get SIF and drought pairs with specified lag - MEMORY OPTIMIZED VERSION.
    
    For each drought period, pairs it with SIF from lag_halfmonths periods BEFORE.
    This tests if SIF from 6-12 weeks ago relates to current drought conditions.
    
    Memory optimizations:
    - Processes data in smaller chunks
    - Explicitly cleans up intermediate variables
    - Uses more efficient array operations
    """
    sif_values = []
    drought_values = []
    total_periods = len(file_info_2023_sorted)
    processed = 0
    
    for i, drought_info in enumerate(file_info_2023_sorted):
        # Skip early periods that don't have SIF data from far enough back
        if i < lag_halfmonths:
            continue
        
        # Get SIF data from lag_halfmonths periods BEFORE the drought period
        sif_info = file_info_2023_sorted[i - lag_halfmonths]
        
        # Load and process drought data (current period)
        month = int(drought_info["month"])
        half_idx = 1 if drought_info["half"] == "a" else 2
        tif_path = DROUGHT_DIR / f"Iowa_county_drought_DM_{drought_info['year']}-{month:02d}_{half_idx}.tif"
        
        if not tif_path.exists():
            continue
        
        try:
            # Load and process SIF data (from earlier period, lagged back)
            month_sif = int(sif_info["month"])
            clim_mean, clim_std = compute_sif_climatology(month_sif)
            if clim_mean is None or clim_std is None:
                continue
            
            sif_da = load_sif_da(sif_info)
            sif_da = _iowa_spatial(sif_da, sif_info)
            sif_da = apply_agricultural_mask(sif_da, sif_info)
            
            sif_da, clim_mean_aligned, clim_std_aligned = xr.align(
                sif_da, clim_mean, clim_std, join="outer"
            )
            sif_z = (sif_da - clim_mean_aligned) / clim_std_aligned
            sif_z = sif_z.where(~np.isnan(sif_da))
            
            # Clean up intermediate SIF variables
            del sif_da, clim_mean_aligned, clim_std_aligned
            
            # Load and process drought data (current period)
            drought_da = rxr.open_rasterio(tif_path).squeeze(drop=True)
            nodata = drought_da.rio.nodata
            if nodata is not None:
                drought_da = drought_da.where(drought_da != nodata)
            
            if drought_da.rio.crs is None:
                drought_da = drought_da.rio.write_crs("EPSG:4326")
            else:
                drought_da = drought_da.rio.reproject("EPSG:4326")
            
            drought_da = drought_da.rio.clip([iowa_shape], crs="EPSG:4326", drop=True)
            drought_da = drought_da.rename({"y": "lat", "x": "lon"})
            
            # Match drought to SIF grid
            drought_matched = load_drought_match_target(drought_info, sif_z)
            
            # Align and extract valid pairs
            sif_aligned, drought_aligned = xr.align(sif_z, drought_matched, join="inner")
            
            # Clean up before extracting arrays
            del sif_z, drought_matched, drought_da
            
            # Extract valid pairs more efficiently
            sif_arr = sif_aligned.values
            drought_arr = drought_aligned.values
            valid_mask = np.isfinite(sif_arr) & np.isfinite(drought_arr) & (drought_arr >= 0) & (drought_arr <= 5)
            
            # Clean up aligned arrays
            del sif_aligned, drought_aligned
            
            if np.any(valid_mask):
                # Only store the valid values (more memory efficient)
                sif_values.append(sif_arr[valid_mask].copy())
                drought_values.append(drought_arr[valid_mask].copy())
            
            # Clean up
            del sif_arr, drought_arr, valid_mask
            
            processed += 1
            if processed % 5 == 0:
                print(f"  Processed {processed}/{total_periods - lag_halfmonths} periods...", end='\r')
                gc.collect()  # Force garbage collection periodically
                
        except Exception as e:
            print(f"\n⚠️ Error processing SIF {sif_info['year']}-{sif_info['month']}{sif_info['half']} -> Drought {drought_info['year']}-{drought_info['month']}{drought_info['half']}: {e}")
            continue
    
    print(f"\n  Completed: {processed} periods processed")
    
    if not sif_values:
        return None, None
    
    # Concatenate all values
    sif_all = np.concatenate(sif_values)
    drought_all = np.concatenate(drought_values)
    
    # Clean up the list of arrays
    del sif_values, drought_values
    gc.collect()
    
    return sif_all, drought_all

# Create figure with 4 subplots (one for each lag)
fig, axes = plt.subplots(2, 2, figsize=(16, 12))
fig.suptitle("SIF z-score Distribution by Drought Category with Time Lags\n(Iowa Agricultural Lands, 2023)", 
             fontsize=14, fontweight="bold", y=0.995)

axes_flat = axes.flatten()
lag_weeks_list = [6, 8, 10, 12]

# Process each lag separately and clear memory between them
for plot_idx, lag_weeks in enumerate(lag_weeks_list):
    ax = axes_flat[plot_idx]
    lag_halfmonths = lags_halfmonths[lag_weeks]
    
    print(f"\n{'='*60}")
    print(f"Processing {lag_weeks}-week lag ({lag_halfmonths} half-months)...")
    print(f"{'='*60}")
    
    sif_all, drought_all = get_lagged_sif_drought_pairs_optimized(lag_halfmonths)
    
    if sif_all is None or len(sif_all) == 0:
        ax.text(0.5, 0.5, f"No data for {lag_weeks}-week lag", 
                ha='center', va='center', transform=ax.transAxes)
        ax.set_title(f"{lag_weeks}-Week Lag", fontsize=12, fontweight="bold")
        continue
    
    # Group SIF values by drought category
    drought_categories = np.round(drought_all).astype(int)
    drought_categories = np.clip(drought_categories, 0, 5)
    
    drought_groups = {}
    for cat in range(6):
        mask = drought_categories == cat
        if np.any(mask):
            drought_groups[cat] = sif_all[mask]
    
    available_categories = sorted([cat for cat in drought_groups.keys() if len(drought_groups[cat]) > 0])
    
    if not available_categories:
        ax.text(0.5, 0.5, f"No valid categories for {lag_weeks}-week lag", 
                ha='center', va='center', transform=ax.transAxes)
        ax.set_title(f"{lag_weeks}-Week Lag", fontsize=12, fontweight="bold")
        # Clean up before next iteration
        del sif_all, drought_all, drought_categories, drought_groups
        gc.collect()
        continue
    
    # Prepare boxplot data
    boxplot_data = [drought_groups[cat] for cat in available_categories]
    boxplot_labels = [str(cat) for cat in available_categories]
    
    # Create boxplot
    bp = ax.boxplot(
        boxplot_data,
        labels=boxplot_labels,
        patch_artist=True,
        showmeans=True,
        meanline=False,
        widths=0.6
    )
    
    # Style the boxplots
    colors = plt.cm.YlOrRd(np.linspace(0.3, 0.9, len(available_categories)))
    for patch, color in zip(bp['boxes'], colors):
        patch.set_facecolor(color)
        patch.set_alpha(0.7)
        patch.set_edgecolor('#333333')
        patch.set_linewidth(1.2)
    
    # Style other elements
    for element in ['whiskers', 'fliers', 'means', 'medians', 'caps']:
        if element in bp:
            if element == 'medians':
                for line in bp[element]:
                    line.set_color('#1a1a1a')
                    line.set_linewidth(2)
            elif element == 'means':
                for marker in bp[element]:
                    marker.set_markerfacecolor('#2c3e50')
                    marker.set_markeredgecolor('#2c3e50')
                    marker.set_markersize(8)
            else:
                for line in bp[element]:
                    line.set_color('#666666')
                    line.set_linewidth(1)
    
    ax.set_xlabel("Drought Category (DM)", fontsize=11, fontweight="bold")
    ax.set_ylabel("SIF z-score", fontsize=11, fontweight="bold")
    ax.set_title(f"{lag_weeks}-Week Lag", fontsize=12, fontweight="bold")
    ax.grid(True, alpha=0.3, linestyle='--', axis='y')
    ax.axhline(y=0, color='gray', linestyle='-', linewidth=0.8, alpha=0.5)
    
    # Add sample size annotations
    for i, cat in enumerate(available_categories):
        n = len(drought_groups[cat])
        ax.text(i + 1, ax.get_ylim()[0] + 0.05 * (ax.get_ylim()[1] - ax.get_ylim()[0]),
                f'n={n:,}', ha='center', va='bottom', fontsize=9, style='italic', color='#555555')
    
    print(f"✓ {lag_weeks}-week lag: {len(sif_all):,} data points across {len(available_categories)} categories")
    
    # Clean up before next iteration
    del sif_all, drought_all, drought_categories, drought_groups, boxplot_data
    gc.collect()

plt.tight_layout()

# Save figure
lag_analysis_path = figures_dir / "lag_analysis_2023.png"
fig.savefig(lag_analysis_path, dpi=300, bbox_inches="tight")
print(f"\n✓ Saved lag analysis figure to {lag_analysis_path}")

plt.show()

# Final cleanup
gc.collect()
print("\n✅ Lag analysis complete!")

