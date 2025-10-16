# Define US bounding box for focused visualization
us_bounds = {
    'lon_min': -125.0,  # West Coast
    'lon_max': -66.0,   # East Coast  
    'lat_min': 24.0,    # Southern border
    'lat_max': 49.0     # Northern border
}

print("US bounding box:")
print(f"Longitude: {us_bounds['lon_min']} to {us_bounds['lon_max']}")
print(f"Latitude: {us_bounds['lat_min']} to {us_bounds['lat_max']}")

# Set up fixed color scale for consistent comparison across time
vmin, vmax = 0.0, 2.0  # mW/m²/nm/sr
cmap = 'viridis'

print(f"\nColor scale: {vmin} - {vmax} mW/m²/nm/sr")
print(f"Colormap: {cmap}")


# Function to load and process SIF data
def load_sif_data(file_path, fill_value=-9999):
    """Load SIF data and mask fill values"""
    ds = xr.open_dataset(file_path)
    sif = ds['sif_ann']
    
    # Mask fill values
    sif_masked = sif.where(sif != fill_value)
    
    return sif_masked

# Create animation frames for US region
print("Creating animation frames for US region...")
frames = []

for i, info in enumerate(file_info):
    print(f"Processing frame {i+1}/{len(file_info)}: {info['filename']}")
    
    # Load SIF data
    sif_data = load_sif_data(info['file_path'])
    
    # Extract US region
    sif_us = sif_data.sel(
        longitude=slice(us_bounds['lon_min'], us_bounds['lon_max']),
        latitude=slice(us_bounds['lat_min'], us_bounds['lat_max'])
    )
    
    # Create plot
    fig, ax = plt.subplots(figsize=(14, 8))
    
    # Plot SIF data
    im = sif_us.plot(ax=ax, vmin=vmin, vmax=vmax, cmap=cmap, add_colorbar=True)
    
    # Customize plot
    ax.set_title(f'SIF - {info["year"]}-{info["month"]} ({info["half_description"]})', 
                fontsize=16, fontweight='bold')
    ax.set_xlabel('Longitude', fontsize=12)
    ax.set_ylabel('Latitude', fontsize=12)
    
    # Add text annotation with frame info
    ax.text(0.02, 0.98, f'Frame {i+1}/{len(file_info)}', transform=ax.transAxes, 
            fontsize=12, fontweight='bold', verticalalignment='top',
            bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))
    
    plt.tight_layout()
    
    # Save frame
    frame_path = figures_dir / f'frame_{i+1:03d}.png'
    plt.savefig(frame_path, dpi=150, bbox_inches='tight')
    plt.close()
    
    # Add to frames list
    frames.append(str(frame_path))

print(f"\nCreated {len(frames)} frames")


# Create GIF animation
print("Creating GIF animation...")
gif_path = figures_dir / 'sif_us_animation_2014_2024.gif'

# Read images and create GIF
images = []
for frame_path in frames:
    images.append(imageio.imread(frame_path))

# Create GIF with 0.5 seconds per frame (faster animation)
imageio.mimsave(gif_path, images, duration=0.5)

print(f"✅ GIF animation saved to: {gif_path}")
print(f"Animation shows SIF changes from {file_info[0]['year']}-{file_info[0]['month']} to {file_info[-1]['year']}-{file_info[-1]['month']}")
print(f"Total frames: {len(frames)}")

# Clean up individual frame files (optional)
print("\nCleaning up individual frame files...")
for frame_path in frames:
    Path(frame_path).unlink()
print("✅ Frame cleanup complete")

print(f"\n🎬 Your US SIF animation is ready!")
print(f"📁 Location: {gif_path}")
print(f"📊 Shows: {len(file_info)} time points from {min(years)} to {max(years)}")
print(f"🗺️  Region: Continental United States")
print(f"📈 Color scale: {vmin} - {vmax} mW/m²/nm/sr")