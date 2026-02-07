#!/usr/bin/env python3
"""
Build Epstein Email relationship graph visualizations.
Uses preprocessed email data from the epstein_emails.db database.
"""

import sqlite3
import json
import os
import sys
from collections import defaultdict
from pathlib import Path

import numpy as np
import pandas as pd
import networkx as nx
from pyvis.network import Network

# Add project root to path
PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "preprocessed" / "epstein_emails.db"
OUTPUT_DIR = PROJECT_ROOT / "output"
DATA_DIR = PROJECT_ROOT / "data" / "processed"

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
DATA_DIR.mkdir(parents=True, exist_ok=True)

# Name normalization mapping - merge variations of the same person
NAME_CANONICALIZATION = {
    # Jeffrey Epstein variations
    'jeffrey e.': 'jeffrey epstein',
    'jeffrey epstein': 'jeffrey epstein',
    'jeevacation': 'jeffrey epstein',
    'j': 'jeffrey epstein',
    'jeff epstein': 'jeffrey epstein',
    'jeff': 'jeffrey epstein',
    'je': 'jeffrey epstein',
    'ee': 'jeffrey epstein',
    
    # Ghislaine Maxwell
    'ghislaine': 'ghislaine maxwell',
    'maxwell': 'ghislaine maxwell',
    'g.maxwell': 'ghislaine maxwell',
    
    # Reid Weingarten variations
    'weingarten': 'reid weingarten',
    'weingarten, reid': 'reid weingarten',
    'reid': 'reid weingarten',
    
    # Common variations
    'unknown': None,  # Exclude
}

# Emails that identify the same person
EMAIL_CANONICALIZATION = {
    'jeevacation@gmail.com': 'jeffrey epstein',
}


def get_db_connection():
    """Get SQLite database connection."""
    return sqlite3.connect(str(DB_PATH))


def normalize_name(name, email=None):
    """Normalize and canonicalize name."""
    if not name:
        return None
    
    name_lower = name.lower().strip()
    
    # Check email canonicalization first
    if email and email.lower() in EMAIL_CANONICALIZATION:
        return EMAIL_CANONICALIZATION[email.lower()]
    
    # Check name canonicalization
    if name_lower in NAME_CANONICALIZATION:
        return NAME_CANONICALIZATION[name_lower]
    
    # Filter out noise
    if name_lower in ('unknown', 'none', 'n/a', '', 'redacted'):
        return None
    
    # Return cleaned name
    return name_lower


def extract_person_entities(conn, min_threads=3):
    """Extract person entities with minimum thread threshold, merged by canonical name."""
    query = """
    SELECT p.id, p.name, p.email, p.total_threads
    FROM people p
    WHERE p.total_threads >= ?
    ORDER BY p.total_threads DESC
    """
    
    cursor = conn.execute(query, (min_threads,))
    
    # Merge by canonical name
    canonical_data = defaultdict(lambda: {'total_threads': 0, 'ids': [], 'emails': set()})
    
    for row in cursor:
        person_id, name, email, total_threads = row
        canonical = normalize_name(name, email)
        
        if canonical is None:
            continue
        
        canonical_data[canonical]['total_threads'] += total_threads
        canonical_data[canonical]['ids'].append(person_id)
        if email:
            canonical_data[canonical]['emails'].add(email)
    
    # Convert to DataFrame format
    rows = []
    for canonical, data in canonical_data.items():
        rows.append({
            'normalized': canonical,
            'total': data['total_threads'],
            'file_count': data['total_threads'],  # Use threads as file count
            'person_ids': data['ids'],
            'emails': list(data['emails'])
        })
    
    df = pd.DataFrame(rows)
    df = df.sort_values('total', ascending=False).reset_index(drop=True)
    
    return df


def extract_cooccurrences(conn, person_df, min_cooccur=2):
    """Extract co-occurrences between persons, using canonical names."""
    # Build mapping from person_id to canonical name
    id_to_canonical = {}
    for _, row in person_df.iterrows():
        for pid in row['person_ids']:
            id_to_canonical[pid] = row['normalized']
    
    # Get all co-occurrences
    query = """
    SELECT person_a, person_b, thread_count 
    FROM person_cooccurrence 
    WHERE thread_count >= 1
    """
    
    cursor = conn.execute(query)
    
    # Merge by canonical pairs
    canonical_cooccur = defaultdict(int)
    
    for row in cursor:
        p_a, p_b, count = row
        
        canonical_a = id_to_canonical.get(p_a)
        canonical_b = id_to_canonical.get(p_b)
        
        if canonical_a is None or canonical_b is None:
            continue
        if canonical_a == canonical_b:
            continue  # Skip self-loops
        
        # Ensure consistent ordering
        pair = tuple(sorted([canonical_a, canonical_b]))
        canonical_cooccur[pair] += count
    
    # Convert to DataFrame
    rows = []
    for (a, b), count in canonical_cooccur.items():
        if count >= min_cooccur:
            rows.append({
                'entity_a': a,
                'entity_b': b,
                'file_count': count
            })
    
    df = pd.DataFrame(rows)
    if len(df) > 0:
        df = df.sort_values('file_count', ascending=False).reset_index(drop=True)
    
    return df


def build_network_graph(persons_df, cooccur_df, output_path, top_n=150):
    """Build and save force-directed network graph."""
    print(f"Building network graph with top {top_n} people...")
    
    # Create NetworkX graph
    G = nx.Graph()
    
    # Get top N persons by thread count (excluding Epstein himself for cleaner viz)
    top_persons = set(persons_df.head(top_n)['normalized'].tolist())
    
    # Add nodes
    for _, row in persons_df.iterrows():
        name = row['normalized']
        if name in top_persons:
            G.add_node(name, 
                      size=min(50, 10 + row['total'] / 50),
                      title=f"{name.title()}\nThreads: {row['total']}\nEmails: {', '.join(row.get('emails', [])[:3])}",
                      label=name.title()[:25])
    
    # Add edges
    for _, row in cooccur_df.iterrows():
        a, b = row['entity_a'], row['entity_b']
        if a in top_persons and b in top_persons and a != b:
            G.add_edge(a, b, weight=row['file_count'], 
                      title=f"Co-occur in {row['file_count']} threads")
    
    # Remove isolated nodes
    G.remove_nodes_from(list(nx.isolates(G)))
    
    print(f"Graph has {G.number_of_nodes()} nodes and {G.number_of_edges()} edges")
    
    # Create PyVis network
    net = Network(height="900px", width="100%", bgcolor="#222222", font_color="white")
    net.barnes_hut(gravity=-30000, central_gravity=0.3, spring_length=200)
    
    # Add nodes with colors based on degree
    degrees = dict(G.degree())
    max_degree = max(degrees.values()) if degrees else 1
    
    for node in G.nodes():
        degree = degrees[node]
        # Color gradient from blue (low) to red (high)
        ratio = degree / max_degree
        r = int(255 * ratio)
        b = int(255 * (1 - ratio))
        color = f"rgb({r}, 50, {b})"
        
        net.add_node(node, 
                    label=node.title()[:25],
                    title=G.nodes[node].get('title', node),
                    size=G.nodes[node].get('size', 15),
                    color=color)
    
    # Add edges
    for u, v, data in G.edges(data=True):
        weight = data.get('weight', 1)
        net.add_edge(u, v, 
                    value=weight,
                    title=data.get('title', ''))
    
    # Save
    net.save_graph(str(output_path))
    print(f"Network graph saved to {output_path}")
    
    return G


def build_3d_embedding(persons_df, cooccur_df, output_path, top_n=200):
    """Build 3D embedding visualization using UMAP."""
    import plotly.graph_objects as go
    from sklearn.preprocessing import normalize
    
    print(f"Building 3D embedding with top {top_n} people...")
    
    # Get top persons DataFrame
    top_persons_df = persons_df.head(top_n)
    top_persons = top_persons_df['normalized'].tolist()
    person_idx = {p: i for i, p in enumerate(top_persons)}
    
    # Build co-occurrence matrix
    n = len(top_persons)
    cooccur_matrix = np.zeros((n, n))
    
    for _, row in cooccur_df.iterrows():
        a, b = row['entity_a'], row['entity_b']
        if a in person_idx and b in person_idx:
            i, j = person_idx[a], person_idx[b]
            cooccur_matrix[i, j] = row['file_count']
            cooccur_matrix[j, i] = row['file_count']
    
    # Add self-connections based on thread count
    for _, row in top_persons_df.iterrows():
        name = row['normalized']
        if name in person_idx:
            i = person_idx[name]
            cooccur_matrix[i, i] = row['total']
    
    # Normalize
    cooccur_matrix = np.log1p(cooccur_matrix)
    
    # Use UMAP for dimensionality reduction
    try:
        import umap
        reducer = umap.UMAP(n_components=3, n_neighbors=15, min_dist=0.1, 
                          metric='cosine', random_state=42)
        embedding = reducer.fit_transform(cooccur_matrix)
    except Exception as e:
        print(f"UMAP failed: {e}, using PCA fallback")
        from sklearn.decomposition import PCA
        pca = PCA(n_components=3)
        embedding = pca.fit_transform(cooccur_matrix)
    
    # Get metadata for hover
    mentions = top_persons_df.set_index('normalized')['total'].to_dict()
    
    # Size based on mentions
    sizes = [min(30, 5 + mentions.get(p, 0) / 100) for p in top_persons]
    
    # Color based on connectivity
    connectivity = cooccur_matrix.sum(axis=1)
    
    # Create hover text
    hover_text = [
        f"{p.title()}<br>Threads: {mentions.get(p, 0)}"
        for p in top_persons
    ]
    
    # Create 3D scatter plot
    fig = go.Figure(data=[go.Scatter3d(
        x=embedding[:, 0],
        y=embedding[:, 1],
        z=embedding[:, 2],
        mode='markers+text',
        marker=dict(
            size=sizes,
            color=connectivity,
            colorscale='Viridis',
            opacity=0.8,
            colorbar=dict(title='Connectivity')
        ),
        text=[p.title()[:15] for p in top_persons],
        hovertext=hover_text,
        hoverinfo='text',
        textposition='top center',
        textfont=dict(size=8, color='white')
    )])
    
    fig.update_layout(
        title='Epstein Emails: Person Relationship Embedding (3D)',
        scene=dict(
            xaxis=dict(showgrid=False, showticklabels=False, title=''),
            yaxis=dict(showgrid=False, showticklabels=False, title=''),
            zaxis=dict(showgrid=False, showticklabels=False, title=''),
            bgcolor='rgb(20, 20, 30)'
        ),
        paper_bgcolor='rgb(20, 20, 30)',
        font=dict(color='white'),
        height=800
    )
    
    fig.write_html(str(output_path))
    print(f"3D embedding saved to {output_path}")
    
    return embedding


def generate_summary(persons_df, cooccur_df, G, conn, output_path):
    """Generate summary report."""
    print("Generating summary report...")
    
    # Get total email count
    cursor = conn.execute("SELECT COUNT(*) FROM emails")
    total_emails = cursor.fetchone()[0]
    
    summary = []
    summary.append("# Epstein Emails Relationship Analysis Summary\n")
    summary.append(f"Generated from Hugging Face dataset: `notesbymuneeb/epstein-emails`\n")
    summary.append("\n## Overview\n")
    summary.append(f"- **Total email threads**: {total_emails:,}")
    summary.append(f"- **Total unique people (normalized)**: {len(persons_df):,}")
    summary.append(f"- **Total co-occurrence pairs**: {len(cooccur_df):,}")
    summary.append(f"- **Network nodes (after filtering)**: {G.number_of_nodes()}")
    summary.append(f"- **Network edges**: {G.number_of_edges()}")
    
    summary.append("\n## Top 30 Most Active Email Participants\n")
    summary.append("| Rank | Name | Email Threads |")
    summary.append("|------|------|---------------|")
    for i, (_, row) in enumerate(persons_df.head(30).iterrows(), 1):
        emails_str = row.get('emails', [])
        email_display = emails_str[0] if emails_str else '-'
        summary.append(f"| {i} | {row['normalized'].title()} | {row['total']:,} |")
    
    summary.append("\n## Top 30 Strongest Connections\n")
    summary.append("| Person A | Person B | Shared Threads |")
    summary.append("|----------|----------|----------------|")
    for _, row in cooccur_df.head(30).iterrows():
        if row['entity_a'] != row['entity_b']:
            summary.append(f"| {row['entity_a'].title()} | {row['entity_b'].title()} | {row['file_count']:,} |")
    
    # Network analysis
    summary.append("\n## Network Analysis\n")
    
    # Most connected (by degree)
    degrees = dict(G.degree())
    top_connected = sorted(degrees.items(), key=lambda x: -x[1])[:20]
    summary.append("\n### Most Connected People (by network degree)\n")
    summary.append("| Rank | Name | Connections |")
    summary.append("|------|------|-------------|")
    for i, (name, degree) in enumerate(top_connected, 1):
        summary.append(f"| {i} | {name.title()} | {degree} |")
    
    # Betweenness centrality (bridge nodes)
    try:
        betweenness = nx.betweenness_centrality(G)
        top_bridges = sorted(betweenness.items(), key=lambda x: -x[1])[:15]
        summary.append("\n### Key Bridge People (high betweenness centrality)\n")
        summary.append("These people connect otherwise separate groups.\n")
        summary.append("| Rank | Name | Centrality |")
        summary.append("|------|------|------------|")
        for i, (name, score) in enumerate(top_bridges, 1):
            summary.append(f"| {i} | {name.title()} | {score:.4f} |")
    except:
        pass
    
    # Community detection
    try:
        communities = list(nx.community.louvain_communities(G, seed=42))
        summary.append(f"\n### Detected Communities: {len(communities)}\n")
        for i, comm in enumerate(sorted(communities, key=len, reverse=True)[:7], 1):
            members = sorted(comm, key=lambda x: -degrees.get(x, 0))[:8]
            members_str = ", ".join([m.title() for m in members])
            summary.append(f"**Cluster {i}** ({len(comm)} members): {members_str}...")
    except:
        pass
    
    # Interesting email threads
    summary.append("\n## Notable Email Threads\n")
    summary.append("Threads with the most participants:\n")
    
    cursor = conn.execute("""
    SELECT e.thread_id, e.subject, e.message_count, 
           COUNT(DISTINCT ep.person_id) as participants
    FROM emails e
    JOIN email_participants ep ON e.thread_id = ep.thread_id
    GROUP BY e.thread_id
    ORDER BY participants DESC, e.message_count DESC
    LIMIT 20
    """)
    
    summary.append("| Subject | Messages | Participants |")
    summary.append("|---------|----------|--------------|")
    for row in cursor:
        subject = (row[1] or 'No Subject')[:50]
        summary.append(f"| {subject} | {row[2]} | {row[3]} |")
    
    summary.append("\n## Data Source\n")
    summary.append("- **Dataset**: `notesbymuneeb/epstein-emails` on Hugging Face")
    summary.append("- **Total threads**: 5,082 email threads from released documents")
    summary.append("- **Processing**: Names normalized to merge variations (e.g., 'Jeffrey E.', 'J', 'jeevacation@gmail.com' → 'Jeffrey Epstein')")
    summary.append("- **Co-occurrence**: Two people co-occur if they appear in the same email thread (as sender, recipient, or CC)")
    
    report = "\n".join(summary)
    
    with open(output_path, 'w') as f:
        f.write(report)
    
    print(f"Summary saved to {output_path}")
    return report


def export_data(persons_df, cooccur_df, data_dir):
    """Export processed data to JSON/JSONL files."""
    # Export names with document references
    names_path = data_dir / "names.jsonl"
    with open(names_path, 'w') as f:
        for _, row in persons_df.iterrows():
            entry = {
                'name': row['normalized'],
                'mentions': int(row['total']),
                'file_count': int(row['file_count']),
                'emails': row.get('emails', [])
            }
            f.write(json.dumps(entry) + '\n')
    print(f"Names exported to {names_path}")
    
    # Export edges
    edges_path = data_dir / "edges.jsonl"
    with open(edges_path, 'w') as f:
        for _, row in cooccur_df.iterrows():
            edge = {
                'source': row['entity_a'],
                'target': row['entity_b'],
                'weight': int(row['file_count'])
            }
            f.write(json.dumps(edge) + '\n')
    print(f"Edges exported to {edges_path}")


def main():
    print("=" * 60)
    print("Epstein Emails Relationship Graph Builder")
    print("=" * 60)
    
    if not DB_PATH.exists():
        print(f"ERROR: Database not found at {DB_PATH}")
        print("Run scripts/process_emails.py first to create the database.")
        sys.exit(1)
    
    conn = get_db_connection()
    
    # Extract data
    print("\nExtracting and normalizing person entities...")
    persons_df = extract_person_entities(conn, min_threads=3)
    print(f"Found {len(persons_df)} unique people (normalized) with 3+ threads")
    
    print("\nExtracting co-occurrences...")
    cooccur_df = extract_cooccurrences(conn, persons_df, min_cooccur=2)
    print(f"Found {len(cooccur_df)} co-occurrence pairs (2+ threads)")
    
    # Export data
    print("\nExporting processed data...")
    export_data(persons_df, cooccur_df, DATA_DIR)
    
    # Build visualizations
    print("\n" + "=" * 60)
    print("Building Visualizations")
    print("=" * 60)
    
    # Force-directed network
    network_path = OUTPUT_DIR / "epstein_network.html"
    G = build_network_graph(persons_df, cooccur_df, network_path, top_n=150)
    
    # 3D embedding
    embedding_path = OUTPUT_DIR / "epstein_3d_embedding.html"
    embedding = build_3d_embedding(persons_df, cooccur_df, embedding_path, top_n=200)
    
    # Summary report
    summary_path = OUTPUT_DIR / "summary.md"
    generate_summary(persons_df, cooccur_df, G, conn, summary_path)
    
    conn.close()
    
    print("\n" + "=" * 60)
    print("COMPLETE!")
    print("=" * 60)
    print(f"\nOutput files:")
    print(f"  1. Network Graph: {network_path}")
    print(f"  2. 3D Embedding:  {embedding_path}")
    print(f"  3. Summary:       {summary_path}")
    print(f"  4. Names data:    {DATA_DIR / 'names.jsonl'}")
    print(f"  5. Edges data:    {DATA_DIR / 'edges.jsonl'}")


if __name__ == "__main__":
    main()
