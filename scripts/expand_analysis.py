#!/usr/bin/env python3
"""
Expanded Epstein Files Analysis
Generates:
1. Topic embeddings (3D) for document visualization
2. Names + Emails directory (JSON, CSV, HTML)
3. Timeline data from extracted dates
4. Location network with coordinates
"""

import sqlite3
import json
import csv
import re
from datetime import datetime
from pathlib import Path
from collections import defaultdict
import numpy as np

# Paths
PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "preprocessed" / "epstein_emails.db"
OUTPUT_DIR = PROJECT_ROOT / "data" / "expanded"
OUTPUT_DIR.mkdir(exist_ok=True)

def parse_timestamp(ts_str):
    """Parse various timestamp formats from the email data"""
    if not ts_str:
        return None
    
    formats = [
        "%m/%d/%Y %I:%M %p",
        "%m/%d/%Y %I:%M:%S %p",
        "%a, %b %d, %Y at %I:%M %p",
        "%a, %b %d, %Y at %I:%M:%S %p",
        "%b %d, %Y, at %I:%M %p",
        "%b %d, %Y, at %I:%M:%S %p",
        "%Y-%m-%d",
        "%m/%d/%Y",
    ]
    
    for fmt in formats:
        try:
            return datetime.strptime(ts_str.strip(), fmt)
        except ValueError:
            continue
    
    # Try extracting date portions
    date_patterns = [
        r'(\d{1,2}/\d{1,2}/\d{4})',
        r'(\w+\s+\d{1,2},?\s+\d{4})',
    ]
    for pattern in date_patterns:
        match = re.search(pattern, ts_str)
        if match:
            try:
                return datetime.strptime(match.group(1), "%m/%d/%Y")
            except:
                try:
                    return datetime.strptime(match.group(1).replace(",", ""), "%B %d %Y")
                except:
                    pass
    return None


def extract_emails_and_names():
    """Extract all names and emails from the database"""
    print("Extracting names and emails...")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    # Get all people with their thread counts
    cursor.execute("""
        SELECT p.name, p.email, p.total_threads,
               (SELECT COUNT(DISTINCT thread_id) FROM email_participants WHERE person_id = p.id) as actual_threads
        FROM people p
        WHERE p.name != '' AND LENGTH(p.name) > 1
        ORDER BY p.total_threads DESC
    """)
    
    people = {}
    for name, email, total_threads, actual_threads in cursor.fetchall():
        name_lower = name.lower().strip()
        if name_lower not in people:
            people[name_lower] = {
                "name": name.strip(),
                "emails": set(),
                "mention_count": 0,
                "thread_count": 0
            }
        
        if email and email.strip() and '@' in email and len(email) > 3:
            # Clean up weird email artifacts
            clean_email = email.strip().lower()
            if not any(x in clean_email for x in ['___', '===', '|||']):
                people[name_lower]["emails"].add(clean_email)
        
        people[name_lower]["mention_count"] += total_threads or 0
        people[name_lower]["thread_count"] += actual_threads or 0
    
    # Convert to list and clean up
    people_list = []
    for key, data in people.items():
        if data["mention_count"] >= 2:  # Filter out one-offs
            people_list.append({
                "name": data["name"],
                "name_normalized": key,
                "emails": sorted(list(data["emails"])),
                "mention_count": data["mention_count"],
                "thread_count": data["thread_count"]
            })
    
    people_list.sort(key=lambda x: x["mention_count"], reverse=True)
    
    conn.close()
    return people_list


def extract_timeline_data():
    """Extract dates from emails for timeline visualization"""
    print("Extracting timeline data...")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT thread_id, subject, messages_json FROM emails")
    
    timeline_events = []
    date_counts = defaultdict(int)
    
    for thread_id, subject, messages_json in cursor.fetchall():
        try:
            messages = json.loads(messages_json) if messages_json else []
        except json.JSONDecodeError:
            continue
        
        for msg in messages:
            ts = msg.get("timestamp")
            dt = parse_timestamp(ts)
            if dt and dt.year >= 2000 and dt.year <= 2025:
                date_str = dt.strftime("%Y-%m-%d")
                date_counts[date_str] += 1
                
                # Sample some events
                if len([e for e in timeline_events if e["date"] == date_str]) < 3:
                    sender = msg.get("sender") or "Unknown"
                    # Clean sender name
                    sender_name = re.sub(r'\s*[\[<].*', '', str(sender)).strip()
                    timeline_events.append({
                        "date": date_str,
                        "timestamp": dt.isoformat(),
                        "subject": subject or "(no subject)",
                        "sender": sender_name,
                        "snippet": (msg.get("body", "")[:150] + "...") if msg.get("body") else ""
                    })
    
    conn.close()
    
    # Aggregate by date
    timeline_summary = []
    for date, count in sorted(date_counts.items()):
        events = [e for e in timeline_events if e["date"] == date][:3]
        timeline_summary.append({
            "date": date,
            "count": count,
            "sample_events": events
        })
    
    return timeline_summary


def extract_locations():
    """Extract location mentions from email text using pattern matching"""
    print("Extracting locations...")
    
    # Known locations from Epstein case
    known_locations = {
        "new york": {"lat": 40.7128, "lng": -74.0060, "type": "city"},
        "manhattan": {"lat": 40.7831, "lng": -73.9712, "type": "area"},
        "palm beach": {"lat": 26.7056, "lng": -80.0364, "type": "city"},
        "little st. james": {"lat": 18.2999, "lng": -64.8263, "type": "island"},
        "little saint james": {"lat": 18.2999, "lng": -64.8263, "type": "island"},
        "virgin islands": {"lat": 18.3358, "lng": -64.8963, "type": "territory"},
        "us virgin islands": {"lat": 18.3358, "lng": -64.8963, "type": "territory"},
        "paris": {"lat": 48.8566, "lng": 2.3522, "type": "city"},
        "london": {"lat": 51.5074, "lng": -0.1278, "type": "city"},
        "florida": {"lat": 27.6648, "lng": -81.5158, "type": "state"},
        "cambridge": {"lat": 42.3736, "lng": -71.1097, "type": "city"},
        "harvard": {"lat": 42.3770, "lng": -71.1167, "type": "institution"},
        "mit": {"lat": 42.3601, "lng": -71.0942, "type": "institution"},
        "santa fe": {"lat": 35.6870, "lng": -105.9378, "type": "city"},
        "new mexico": {"lat": 34.5199, "lng": -105.8701, "type": "state"},
        "ohio": {"lat": 40.4173, "lng": -82.9071, "type": "state"},
        "israel": {"lat": 31.0461, "lng": 34.8516, "type": "country"},
        "tel aviv": {"lat": 32.0853, "lng": 34.7818, "type": "city"},
        "china": {"lat": 35.8617, "lng": 104.1954, "type": "country"},
        "beijing": {"lat": 39.9042, "lng": 116.4074, "type": "city"},
        "japan": {"lat": 36.2048, "lng": 138.2529, "type": "country"},
        "tokyo": {"lat": 35.6762, "lng": 139.6503, "type": "city"},
        "washington": {"lat": 38.9072, "lng": -77.0369, "type": "city"},
        "dc": {"lat": 38.9072, "lng": -77.0369, "type": "city"},
        "washington dc": {"lat": 38.9072, "lng": -77.0369, "type": "city"},
        "los angeles": {"lat": 34.0522, "lng": -118.2437, "type": "city"},
        "la": {"lat": 34.0522, "lng": -118.2437, "type": "city"},
        "california": {"lat": 36.7783, "lng": -119.4179, "type": "state"},
        "arizona": {"lat": 34.0489, "lng": -111.0937, "type": "state"},
        "boston": {"lat": 42.3601, "lng": -71.0589, "type": "city"},
        "chicago": {"lat": 41.8781, "lng": -87.6298, "type": "city"},
        "monaco": {"lat": 43.7384, "lng": 7.4246, "type": "country"},
        "dubai": {"lat": 25.2048, "lng": 55.2708, "type": "city"},
        "abu dhabi": {"lat": 24.4539, "lng": 54.3773, "type": "city"},
        "saudi arabia": {"lat": 23.8859, "lng": 45.0792, "type": "country"},
        "russia": {"lat": 61.5240, "lng": 105.3188, "type": "country"},
        "moscow": {"lat": 55.7558, "lng": 37.6173, "type": "city"},
    }
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("""
        SELECT e.thread_id, e.messages_json, GROUP_CONCAT(DISTINCT p.name) as participants
        FROM emails e
        LEFT JOIN email_participants ep ON e.thread_id = ep.thread_id
        LEFT JOIN people p ON ep.person_id = p.id
        GROUP BY e.thread_id
    """)
    
    location_mentions = defaultdict(lambda: {"count": 0, "people": set(), "threads": set()})
    
    for thread_id, messages_json, participants in cursor.fetchall():
        try:
            messages = json.loads(messages_json) if messages_json else []
        except json.JSONDecodeError:
            continue
        
        text = " ".join((m.get("body") or "") + " " + (m.get("subject") or "") for m in messages).lower()
        participant_list = participants.split(",") if participants else []
        
        for loc_name, loc_data in known_locations.items():
            if loc_name in text:
                location_mentions[loc_name]["count"] += 1
                location_mentions[loc_name]["threads"].add(thread_id)
                for p in participant_list[:5]:  # Limit to avoid noise
                    if p and len(p.strip()) > 2:
                        location_mentions[loc_name]["people"].add(p.strip())
    
    conn.close()
    
    # Build location data
    locations = []
    for loc_name, mentions in location_mentions.items():
        if mentions["count"] >= 3:
            loc_info = known_locations[loc_name]
            locations.append({
                "name": loc_name.title(),
                "lat": loc_info["lat"],
                "lng": loc_info["lng"],
                "type": loc_info["type"],
                "mention_count": mentions["count"],
                "thread_count": len(mentions["threads"]),
                "associated_people": sorted(list(mentions["people"]))[:20]
            })
    
    locations.sort(key=lambda x: x["mention_count"], reverse=True)
    return locations


def extract_topic_snippets():
    """Extract text snippets for topic embedding visualization"""
    print("Extracting topic snippets...")
    
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()
    
    cursor.execute("SELECT thread_id, subject, messages_json FROM emails")
    
    snippets = []
    for thread_id, subject, messages_json in cursor.fetchall():
        try:
            messages = json.loads(messages_json) if messages_json else []
        except json.JSONDecodeError:
            continue
        
        for msg in messages:
            body = msg.get("body") or ""
            if len(body) > 100:  # Only substantial messages
                # Clean up the text
                clean_text = re.sub(r'\s+', ' ', body).strip()
                if len(clean_text) > 100:
                    snippets.append({
                        "thread_id": thread_id,
                        "subject": subject or "(no subject)",
                        "sender": msg.get("sender") or "Unknown",
                        "text": clean_text[:500],  # Truncate for embedding
                        "timestamp": msg.get("timestamp") or ""
                    })
    
    conn.close()
    return snippets


def generate_embeddings(snippets, max_samples=2000):
    """Generate 3D embeddings using sentence transformers and UMAP"""
    print(f"Generating embeddings for {min(len(snippets), max_samples)} snippets...")
    
    try:
        from sentence_transformers import SentenceTransformer
        import umap
        from sklearn.cluster import KMeans
    except ImportError as e:
        print(f"Missing dependency: {e}")
        print("Install with: pip install sentence-transformers umap-learn scikit-learn")
        return None
    
    # Sample if too many
    if len(snippets) > max_samples:
        np.random.seed(42)
        indices = np.random.choice(len(snippets), max_samples, replace=False)
        snippets = [snippets[i] for i in indices]
    
    # Generate embeddings
    model = SentenceTransformer('all-MiniLM-L6-v2')
    texts = [s["text"] for s in snippets]
    embeddings = model.encode(texts, show_progress_bar=True)
    
    # Reduce to 3D with UMAP
    reducer = umap.UMAP(n_components=3, n_neighbors=15, min_dist=0.1, random_state=42)
    coords_3d = reducer.fit_transform(embeddings)
    
    # Cluster for coloring
    kmeans = KMeans(n_clusters=8, random_state=42, n_init=10)
    clusters = kmeans.fit_predict(embeddings)
    
    # Cluster names (manual based on inspection)
    cluster_names = [
        "Legal/Court", "Financial", "Science/Research", "Travel/Logistics",
        "Media/Press", "Personal/Social", "Business", "Political"
    ]
    
    # Build result
    result = []
    for i, snippet in enumerate(snippets):
        result.append({
            "x": float(coords_3d[i, 0]),
            "y": float(coords_3d[i, 1]),
            "z": float(coords_3d[i, 2]),
            "cluster": int(clusters[i]),
            "cluster_name": cluster_names[clusters[i] % len(cluster_names)],
            "subject": snippet["subject"],
            "sender": snippet["sender"],
            "text_preview": snippet["text"][:200],
            "timestamp": snippet["timestamp"]
        })
    
    return result


def main():
    print("=" * 60)
    print("Epstein Files - Expanded Analysis")
    print("=" * 60)
    
    # 1. Names and Emails
    people = extract_emails_and_names()
    
    # Save as JSON
    with open(OUTPUT_DIR / "people_directory.json", "w") as f:
        json.dump(people, f, indent=2)
    
    # Save as CSV
    with open(OUTPUT_DIR / "people_directory.csv", "w", newline='') as f:
        writer = csv.DictWriter(f, fieldnames=["name", "name_normalized", "emails", "mention_count", "thread_count"])
        writer.writeheader()
        for p in people:
            row = p.copy()
            row["emails"] = "; ".join(p["emails"])
            writer.writerow(row)
    
    print(f"Saved {len(people)} people to directory")
    
    # 2. Timeline
    timeline = extract_timeline_data()
    with open(OUTPUT_DIR / "timeline.json", "w") as f:
        json.dump(timeline, f, indent=2)
    print(f"Saved {len(timeline)} timeline entries")
    
    # 3. Locations
    locations = extract_locations()
    with open(OUTPUT_DIR / "locations.json", "w") as f:
        json.dump(locations, f, indent=2)
    print(f"Saved {len(locations)} locations")
    
    # 4. Topic embeddings
    snippets = extract_topic_snippets()
    embeddings = generate_embeddings(snippets)
    if embeddings:
        with open(OUTPUT_DIR / "topic_embeddings.json", "w") as f:
            json.dump(embeddings, f)
        print(f"Saved {len(embeddings)} topic embeddings")
    
    print("\n" + "=" * 60)
    print("Analysis complete! Files saved to:", OUTPUT_DIR)
    print("=" * 60)


if __name__ == "__main__":
    main()
