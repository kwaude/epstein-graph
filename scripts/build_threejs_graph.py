#!/usr/bin/env python3
"""
Build Three.js visualization data for Epstein Email network.
Uses the epstein_emails.db database.
"""

import sqlite3
import json
from collections import defaultdict
from pathlib import Path
import networkx as nx

PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "preprocessed" / "epstein_emails.db"
OUTPUT_PATH = PROJECT_ROOT / "output" / "epstein_3d_threejs.html"

# Name normalization mapping (same as build_graph.py)
NAME_CANONICALIZATION = {
    'jeffrey e.': 'jeffrey epstein',
    'jeffrey epstein': 'jeffrey epstein',
    'jeevacation': 'jeffrey epstein',
    'j': 'jeffrey epstein',
    'jeff epstein': 'jeffrey epstein',
    'jeff': 'jeffrey epstein',
    'je': 'jeffrey epstein',
    'ee': 'jeffrey epstein',
    'ghislaine': 'ghislaine maxwell',
    'maxwell': 'ghislaine maxwell',
    'g.maxwell': 'ghislaine maxwell',
    'weingarten': 'reid weingarten',
    'weingarten, reid': 'reid weingarten',
    'reid': 'reid weingarten',
    'unknown': None,
}

EMAIL_CANONICALIZATION = {
    'jeevacation@gmail.com': 'jeffrey epstein',
}


def normalize_name(name, email=None):
    if not name:
        return None
    name_lower = name.lower().strip()
    if email and email.lower() in EMAIL_CANONICALIZATION:
        return EMAIL_CANONICALIZATION[email.lower()]
    if name_lower in NAME_CANONICALIZATION:
        return NAME_CANONICALIZATION[name_lower]
    if name_lower in ('unknown', 'none', 'n/a', '', 'redacted'):
        return None
    return name_lower


def get_db_connection():
    return sqlite3.connect(str(DB_PATH))


def build_graph_data(top_n=200):
    """Build graph data with cluster assignments from email database."""
    conn = get_db_connection()
    
    # Get all people with their thread counts
    cursor = conn.execute("""
        SELECT id, name, email, total_threads
        FROM people 
        WHERE total_threads >= 3
        ORDER BY total_threads DESC
    """)
    
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
    
    # Sort and take top N
    sorted_persons = sorted(canonical_data.items(), key=lambda x: -x[1]['total_threads'])[:top_n]
    
    persons = []
    person_set = set()
    id_to_canonical = {}
    
    for canonical, data in sorted_persons:
        persons.append({
            'id': canonical,
            'name': canonical.title(),
            'mentions': data['total_threads'],
            'files': data['total_threads'],
            'emails': list(data['emails'])[:3]
        })
        person_set.add(canonical)
        for pid in data['ids']:
            id_to_canonical[pid] = canonical
    
    # Get co-occurrences and merge by canonical
    cursor = conn.execute("""
        SELECT person_a, person_b, thread_count 
        FROM person_cooccurrence 
        WHERE thread_count >= 2
    """)
    
    canonical_cooccur = defaultdict(int)
    for row in cursor:
        p_a, p_b, count = row
        canonical_a = id_to_canonical.get(p_a)
        canonical_b = id_to_canonical.get(p_b)
        if canonical_a and canonical_b and canonical_a != canonical_b:
            pair = tuple(sorted([canonical_a, canonical_b]))
            canonical_cooccur[pair] += count
    
    edges = []
    for (a, b), count in canonical_cooccur.items():
        if a in person_set and b in person_set:
            edges.append({
                'source': a,
                'target': b,
                'weight': count
            })
    
    conn.close()
    
    # Build NetworkX graph for clustering
    G = nx.Graph()
    for p in persons:
        G.add_node(p['id'])
    for e in edges:
        G.add_edge(e['source'], e['target'], weight=e['weight'])
    
    # Remove isolated nodes
    isolated = list(nx.isolates(G))
    G.remove_nodes_from(isolated)
    persons = [p for p in persons if p['id'] not in isolated]
    
    # Community detection
    try:
        communities = list(nx.community.louvain_communities(G, seed=42, resolution=1.0))
    except:
        communities = [set(G.nodes())]
    
    # Assign cluster to each person
    node_to_cluster = {}
    for cluster_idx, community in enumerate(communities):
        for node in community:
            node_to_cluster[node] = cluster_idx
    
    # Calculate degree (connections) for each node
    degrees = dict(G.degree())
    
    # Finalize nodes with cluster info
    nodes = []
    for p in persons:
        if p['id'] in G.nodes():
            nodes.append({
                'id': p['id'],
                'name': p['name'],
                'mentions': p['mentions'],
                'files': p['files'],
                'connections': degrees.get(p['id'], 0),
                'cluster': node_to_cluster.get(p['id'], 0)
            })
    
    # Sort by mentions
    nodes.sort(key=lambda x: -x['mentions'])
    
    # Filter edges
    node_ids = set(n['id'] for n in nodes)
    edges = [e for e in edges if e['source'] in node_ids and e['target'] in node_ids]
    
    print(f"Graph: {len(nodes)} nodes, {len(edges)} edges, {len(communities)} clusters")
    
    return {'nodes': nodes, 'edges': edges}


def generate_html(graph_data):
    """Generate standalone Three.js HTML visualization."""
    json_data = json.dumps(graph_data, indent=2)
    
    html = f'''<!DOCTYPE html>
<html>
<head>
    <meta charset="utf-8">
    <title>Epstein Email Network - 3D Visualization</title>
    <style>
        body {{
            margin: 0;
            overflow: hidden;
            background: #0a0a15;
            font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;
        }}
        #info {{
            position: absolute;
            top: 10px;
            left: 10px;
            color: #fff;
            background: rgba(0,0,0,0.7);
            padding: 15px;
            border-radius: 8px;
            max-width: 300px;
            z-index: 100;
        }}
        #info h2 {{
            margin: 0 0 10px 0;
            font-size: 18px;
        }}
        #info p {{
            margin: 5px 0;
            font-size: 12px;
            color: #aaa;
        }}
        #tooltip {{
            position: absolute;
            background: rgba(0,0,0,0.85);
            color: #fff;
            padding: 10px 15px;
            border-radius: 5px;
            font-size: 13px;
            pointer-events: none;
            display: none;
            z-index: 1000;
        }}
        #legend {{
            position: absolute;
            bottom: 10px;
            right: 10px;
            color: #fff;
            background: rgba(0,0,0,0.7);
            padding: 10px;
            border-radius: 8px;
            font-size: 11px;
        }}
    </style>
</head>
<body>
    <div id="info">
        <h2>Epstein Email Network</h2>
        <p><strong>{len(graph_data['nodes'])}</strong> people, <strong>{len(graph_data['edges'])}</strong> connections</p>
        <p>Source: Hugging Face notesbymuneeb/epstein-emails</p>
        <p>Drag to rotate, scroll to zoom, click for details</p>
    </div>
    <div id="tooltip"></div>
    <div id="legend">
        <strong>Node size</strong>: Email thread count<br>
        <strong>Colors</strong>: Community clusters
    </div>

    <script src="https://cdnjs.cloudflare.com/ajax/libs/three.js/r128/three.min.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/three@0.128.0/examples/js/controls/OrbitControls.js"></script>
    
    <script>
    const graphData = {json_data};
    
    // Scene setup
    const scene = new THREE.Scene();
    scene.background = new THREE.Color(0x0a0a15);
    
    const camera = new THREE.PerspectiveCamera(60, window.innerWidth / window.innerHeight, 0.1, 5000);
    camera.position.set(0, 0, 400);
    
    const renderer = new THREE.WebGLRenderer({{ antialias: true }});
    renderer.setSize(window.innerWidth, window.innerHeight);
    renderer.setPixelRatio(window.devicePixelRatio);
    document.body.appendChild(renderer.domElement);
    
    const controls = new THREE.OrbitControls(camera, renderer.domElement);
    controls.enableDamping = true;
    controls.dampingFactor = 0.05;
    
    // Cluster colors
    const clusterColors = [
        0xff6b6b, 0x4ecdc4, 0xffe66d, 0x95e1d3, 
        0xf38181, 0xaa96da, 0xfcbad3, 0xa8d8ea,
        0xffd3b6, 0xc9b1ff, 0x98ddca, 0xffaaa5
    ];
    
    // Create nodes
    const nodes = {{}};
    const nodePositions = {{}};
    const nodeGroup = new THREE.Group();
    
    // Random sphere layout
    graphData.nodes.forEach((node, i) => {{
        const phi = Math.acos(-1 + (2 * i) / graphData.nodes.length);
        const theta = Math.sqrt(graphData.nodes.length * Math.PI) * phi;
        const radius = 150 + Math.random() * 50;
        
        const x = radius * Math.cos(theta) * Math.sin(phi);
        const y = radius * Math.sin(theta) * Math.sin(phi);
        const z = radius * Math.cos(phi);
        
        nodePositions[node.id] = {{ x, y, z }};
        
        const size = 2 + Math.log(node.mentions + 1) * 1.5;
        const geometry = new THREE.SphereGeometry(size, 16, 16);
        const color = clusterColors[node.cluster % clusterColors.length];
        const material = new THREE.MeshPhongMaterial({{ 
            color: color,
            emissive: color,
            emissiveIntensity: 0.3
        }});
        
        const mesh = new THREE.Mesh(geometry, material);
        mesh.position.set(x, y, z);
        mesh.userData = node;
        
        nodes[node.id] = mesh;
        nodeGroup.add(mesh);
    }});
    
    scene.add(nodeGroup);
    
    // Create edges
    const edgeGroup = new THREE.Group();
    graphData.edges.forEach(edge => {{
        const start = nodePositions[edge.source];
        const end = nodePositions[edge.target];
        if (!start || !end) return;
        
        const points = [
            new THREE.Vector3(start.x, start.y, start.z),
            new THREE.Vector3(end.x, end.y, end.z)
        ];
        
        const geometry = new THREE.BufferGeometry().setFromPoints(points);
        const opacity = Math.min(0.6, 0.1 + edge.weight / 50);
        const material = new THREE.LineBasicMaterial({{ 
            color: 0x4488ff,
            transparent: true,
            opacity: opacity
        }});
        
        const line = new THREE.Line(geometry, material);
        edgeGroup.add(line);
    }});
    
    scene.add(edgeGroup);
    
    // Lighting
    const ambient = new THREE.AmbientLight(0xffffff, 0.4);
    scene.add(ambient);
    
    const point1 = new THREE.PointLight(0xffffff, 0.8);
    point1.position.set(200, 200, 200);
    scene.add(point1);
    
    const point2 = new THREE.PointLight(0x4488ff, 0.5);
    point2.position.set(-200, -200, -200);
    scene.add(point2);
    
    // Raycaster for interaction
    const raycaster = new THREE.Raycaster();
    const mouse = new THREE.Vector2();
    const tooltip = document.getElementById('tooltip');
    
    function onMouseMove(event) {{
        mouse.x = (event.clientX / window.innerWidth) * 2 - 1;
        mouse.y = -(event.clientY / window.innerHeight) * 2 + 1;
        
        raycaster.setFromCamera(mouse, camera);
        const intersects = raycaster.intersectObjects(nodeGroup.children);
        
        if (intersects.length > 0) {{
            const node = intersects[0].object.userData;
            tooltip.style.display = 'block';
            tooltip.style.left = event.clientX + 15 + 'px';
            tooltip.style.top = event.clientY + 15 + 'px';
            tooltip.innerHTML = `
                <strong>${{node.name}}</strong><br>
                Threads: ${{node.mentions}}<br>
                Connections: ${{node.connections}}
            `;
            document.body.style.cursor = 'pointer';
        }} else {{
            tooltip.style.display = 'none';
            document.body.style.cursor = 'default';
        }}
    }}
    
    window.addEventListener('mousemove', onMouseMove);
    
    // Resize handler
    window.addEventListener('resize', () => {{
        camera.aspect = window.innerWidth / window.innerHeight;
        camera.updateProjectionMatrix();
        renderer.setSize(window.innerWidth, window.innerHeight);
    }});
    
    // Animation
    function animate() {{
        requestAnimationFrame(animate);
        controls.update();
        nodeGroup.rotation.y += 0.001;
        renderer.render(scene, camera);
    }}
    
    animate();
    </script>
</body>
</html>'''
    
    with open(OUTPUT_PATH, 'w') as f:
        f.write(html)
    
    print(f"Three.js visualization saved to {OUTPUT_PATH}")


def main():
    print("Building Three.js visualization...")
    graph_data = build_graph_data(top_n=200)
    generate_html(graph_data)
    print("Done!")


if __name__ == "__main__":
    main()
