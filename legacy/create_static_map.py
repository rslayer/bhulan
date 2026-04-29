#!/usr/bin/env python3
"""
Create a static map visualization of GPS tracking data for Palo Alto
Uses matplotlib to create a simple, reliable visualization
"""

import random
import math
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import Circle

PALO_ALTO_CENTER_LAT = 37.4419
PALO_ALTO_CENTER_LON = -122.1430

LOCATIONS = {
    'stanford': (37.4275, -122.1697),
    'university_ave': (37.4467, -122.1589),
    'old_palo_alto': (37.4520, -122.1580),
    'professorville': (37.4480, -122.1650),
    'downtown': (37.4443, -122.1598),
    'rinconada_park': (37.4460, -122.1520),
    'palo_alto_high': (37.4394, -122.1619),
    'midtown': (37.4400, -122.1500),
    'esther_clark_park': (37.4380, -122.1380),
    'college_terrace': (37.4350, -122.1450),
    'california_ave': (37.4291, -122.1421),
    'cubberley': (37.4290, -122.1320),
    'ventura': (37.4220, -122.1280),
    'barron_park': (37.4100, -122.1200),
    'mitchell_park': (37.4180, -122.1113),
    'stanford_shopping': (37.4437, -122.1727),
}

def kilDist(point1, point2):
    """Calculate distance between two lat/lon points in kilometers"""
    lat1 = point1['lat']
    lat2 = point2['lat']
    lon1 = point1['lon']
    lon2 = point2['lon']
    
    if lat1 == lat2 and lon1 == lon2:
        return 0
    
    degreesRadians = math.pi / 180.0
    phi1 = (90.0 - float(lat1)) * degreesRadians
    phi2 = (90.0 - float(lat2)) * degreesRadians
    theta1 = float(lon1) * degreesRadians
    theta2 = float(lon2) * degreesRadians
    
    cos = (math.sin(phi1) * math.sin(phi2) * math.cos(theta1 - theta2) + 
           math.cos(phi1) * math.cos(phi2))
    cos = min(1, max(cos, -1))
    arc = math.acos(cos)
    
    return arc * 6373

def generate_gps_trace(start_loc, end_loc, num_points=20, add_noise=True):
    """Generate GPS points between two locations"""
    lat1, lon1 = start_loc
    lat2, lon2 = end_loc
    
    points = []
    for i in range(num_points):
        t = i / float(num_points - 1)
        lat = lat1 + (lat2 - lat1) * t
        lon = lon1 + (lon2 - lon1) * t
        
        if add_noise:
            lat += random.gauss(0, 0.00005)
            lon += random.gauss(0, 0.00005)
        
        points.append((lat, lon))
    
    return points

def generate_stop_points(location, duration_minutes=15, frequency_seconds=30):
    """Generate GPS points for a vehicle stopped at a location"""
    lat, lon = location
    num_points = int((duration_minutes * 60) / frequency_seconds)
    
    points = []
    for i in range(num_points):
        noise_lat = random.gauss(0, 0.00003)
        noise_lon = random.gauss(0, 0.00003)
        points.append((lat + noise_lat, lon + noise_lon))
    
    return points

def generate_delivery_route():
    """Generate a realistic delivery route in Palo Alto with 15 stops"""
    route_points = []
    
    route_sequence = [
        ('stanford', 22),           # Stop 1: Depot start
        ('university_ave', 14),     # Stop 2
        ('old_palo_alto', 11),      # Stop 3
        ('professorville', 16),     # Stop 4
        ('downtown', 13),           # Stop 5
        ('rinconada_park', 18),     # Stop 6
        ('palo_alto_high', 10),     # Stop 7
        ('midtown', 15),            # Stop 8
        ('esther_clark_park', 12),  # Stop 9
        ('college_terrace', 19),    # Stop 10
        ('california_ave', 17),     # Stop 11
        ('cubberley', 14),          # Stop 12
        ('ventura', 11),            # Stop 13
        ('barron_park', 16),        # Stop 14
        ('mitchell_park', 13),      # Stop 15
    ]
    
    for i in range(len(route_sequence)):
        location_name, duration = route_sequence[i]
        location = LOCATIONS[location_name]
        
        route_points.extend(generate_stop_points(location, duration_minutes=duration))
        
        if i < len(route_sequence) - 1:
            next_location = LOCATIONS[route_sequence[i + 1][0]]
            num_points = random.randint(10, 25)
            route_points.extend(generate_gps_trace(location, next_location, num_points=num_points))
    
    return route_points

def find_stops_simple(points, constraint_km=0.02, min_stop_minutes=10):
    """Detect stops using bhulan's algorithm"""
    if len(points) == 0:
        return []
    
    clusters = []
    current_cluster = [points[0]]
    
    for i in range(1, len(points)):
        point = points[i]
        cluster_lat = sum(p['lat'] for p in current_cluster) / len(current_cluster)
        cluster_lon = sum(p['lon'] for p in current_cluster) / len(current_cluster)
        centroid = {'lat': cluster_lat, 'lon': cluster_lon}
        
        if kilDist(centroid, point) < constraint_km:
            current_cluster.append(point)
        else:
            if len(current_cluster) > 1:
                clusters.append(current_cluster)
            current_cluster = [point]
    
    if len(current_cluster) > 1:
        clusters.append(current_cluster)
    
    stops = []
    for cluster in clusters:
        duration_minutes = len(cluster) * 0.5
        if duration_minutes >= min_stop_minutes:
            lat = sum(p['lat'] for p in cluster) / len(cluster)
            lon = sum(p['lon'] for p in cluster) / len(cluster)
            max_dist = max(kilDist({'lat': lat, 'lon': lon}, p) for p in cluster)
            
            stops.append({
                'lat': lat,
                'lon': lon,
                'radius_km': max_dist,
                'duration_minutes': duration_minutes,
                'num_points': len(cluster)
            })
    
    return stops

def create_static_map(points, stops, output_file='palo_alto_map.png'):
    """Create a static map visualization using matplotlib"""
    
    lats = [p['lat'] for p in points]
    lons = [p['lon'] for p in points]
    
    fig, ax = plt.subplots(figsize=(16, 12))
    
    ax.set_facecolor('#f0f0f0')
    
    ax.plot(lons, lats, 'b-', linewidth=2, alpha=0.6, label='GPS Trace', zorder=1)
    
    sample_lons = [lons[i] for i in range(0, len(lons), 10)]
    sample_lats = [lats[i] for i in range(0, len(lats), 10)]
    ax.scatter(sample_lons, sample_lats, c='#3498db', s=20, alpha=0.8, zorder=2, label='GPS Points')
    
    for i, stop in enumerate(stops):
        circle = Circle((stop['lon'], stop['lat']), 
                       stop['radius_km'] / 111.0,
                       color='#2ecc71', alpha=0.2, zorder=3)
        ax.add_patch(circle)
        
        ax.scatter(stop['lon'], stop['lat'], c='#e74c3c', s=200, 
                  marker='o', edgecolors='#c0392b', linewidths=2, 
                  zorder=4, label='Stop' if i == 0 else '')
        
        ax.text(stop['lon'], stop['lat'], str(i+1), 
               ha='center', va='center', color='white', 
               fontweight='bold', fontsize=9, zorder=5)
        
        duration_text = f"{stop['duration_minutes']:.0f}m"
        ax.text(stop['lon'], stop['lat'] - 0.0015, duration_text,
               ha='center', va='top', color='#e74c3c',
               fontweight='bold', fontsize=8, zorder=5,
               bbox=dict(boxstyle='round,pad=0.2', facecolor='white', 
                        edgecolor='#e74c3c', alpha=0.9, linewidth=1))
    
    ax.scatter(lons[0], lats[0], c='#2ecc71', s=400, marker='*', 
              edgecolors='white', linewidths=2, zorder=6, label='Start')
    ax.scatter(lons[-1], lats[-1], c='#e74c3c', s=400, marker='*', 
              edgecolors='white', linewidths=2, zorder=6, label='End')
    
    for name, (lat, lon) in LOCATIONS.items():
        ax.text(lon, lat + 0.002, name.replace('_', ' ').title(), 
               ha='center', fontsize=8, style='italic', 
               bbox=dict(boxstyle='round,pad=0.3', facecolor='white', alpha=0.7))
    
    ax.set_xlabel('Longitude', fontsize=12, fontweight='bold')
    ax.set_ylabel('Latitude', fontsize=12, fontweight='bold')
    ax.set_title('Bhulan GPS Tracking Demo - Palo Alto Delivery Route\n' +
                f'{len(points)} GPS Points | {len(stops)} Stops Detected', 
                fontsize=16, fontweight='bold', pad=20)
    
    ax.grid(True, alpha=0.3, linestyle='--')
    
    lat_range = max(lats) - min(lats)
    lon_range = max(lons) - min(lons)
    ax.set_xlim(min(lons) - lon_range * 0.1, max(lons) + lon_range * 0.1)
    ax.set_ylim(min(lats) - lat_range * 0.1, max(lats) + lat_range * 0.1)
    
    handles, labels = ax.get_legend_handles_labels()
    by_label = dict(zip(labels, handles))
    ax.legend(by_label.values(), by_label.keys(), 
             loc='upper right', fontsize=10, framealpha=0.9)
    
    total_distance = sum(kilDist(points[i], points[i+1]) for i in range(len(points)-1))
    stats_text = f"""Route Statistics:
• Total Distance: {total_distance:.2f} km
• Duration: {len(points) * 0.5:.1f} minutes
• Avg Speed: {(total_distance / (len(points) * 0.5 / 60)):.1f} km/h
• Stops: {len(stops)}"""
    
    ax.text(0.02, 0.98, stats_text, transform=ax.transAxes,
           fontsize=10, verticalalignment='top',
           bbox=dict(boxstyle='round', facecolor='white', alpha=0.9))
    
    plt.tight_layout()
    
    plt.savefig(output_file, dpi=150, bbox_inches='tight')
    print(f"✓ Map saved to: {output_file}")
    
    return output_file

def main():
    print("=" * 60)
    print("Bhulan GPS Visualization - Static Map Generator")
    print("=" * 60)
    print()
    
    print("Generating realistic delivery route in Palo Alto...")
    route_coords = generate_delivery_route()
    points = [{'lat': lat, 'lon': lon} for lat, lon in route_coords]
    print(f"Generated {len(points)} GPS points")
    print()
    
    print("Detecting stops using bhulan's algorithm...")
    stops = find_stops_simple(points, constraint_km=0.02, min_stop_minutes=10)
    print(f"Identified {len(stops)} stops")
    print()
    
    print("Creating static map visualization...")
    output_file = create_static_map(points, stops)
    print()
    print("=" * 60)

if __name__ == '__main__':
    main()
