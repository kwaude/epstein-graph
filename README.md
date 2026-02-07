# Epstein Email Network Visualization

Interactive network visualization of email relationships from the Epstein document releases.

## Data Source

- **Dataset**: [`notesbymuneeb/epstein-emails`](https://huggingface.co/datasets/notesbymuneeb/epstein-emails) on Hugging Face
- **Total Emails**: 5,082 email threads
- **Unique People**: 510 (normalized from 2,745 raw entries)
- **Connections**: 1,597 co-occurrence pairs

## Visualizations

1. **[Network Graph](output/epstein_network.html)** - Force-directed graph using PyVis
2. **[3D Embedding](output/epstein_3d_embedding.html)** - UMAP dimensionality reduction with Plotly
3. **[3D Three.js](output/epstein_3d_threejs.html)** - Interactive Three.js visualization

## Key Findings

### Top Email Participants
1. Jeffrey Epstein - 5,369 threads
2. Michael Wolff - 297 threads
3. Richard Kahn - 256 threads
4. Kathy Ruemmler - 254 threads
5. Reid Weingarten - 247 threads
6. Darren Indyke - 213 threads
7. Steve Bannon - 170 threads
8. Larry Summers - 99 threads

### Key Bridge People (connecting separate groups)
- Jes Staley (JP Morgan banker)
- Lawrence Krauss (physicist)
- Darren Indyke (Epstein's lawyer)
- Peter Aldhous (journalist)

### Detected Communities
The network contains 10 distinct communities, including:
- **Legal team cluster**: Weinberg, Goldberger, Dershowitz, Roy Black
- **Media cluster**: Michael Wolff, journalists
- **Science cluster**: Lawrence Krauss, Noam Chomsky, Joi Ito

## Setup

```bash
# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install datasets networkx pyvis plotly pandas numpy scikit-learn umap-learn

# Process email dataset
python scripts/process_emails.py

# Build visualizations
python scripts/build_graph.py
python scripts/build_threejs_graph.py
```

## Database Schema

The SQLite database (`preprocessed/epstein_emails.db`) contains:

- `emails` - Email threads (thread_id, subject, messages)
- `people` - Unique people extracted from emails
- `email_participants` - Links people to email threads
- `person_cooccurrence` - Co-occurrence counts between people

## License

Data sourced from publicly released documents. For research purposes only.
