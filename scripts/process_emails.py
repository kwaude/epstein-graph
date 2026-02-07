#!/usr/bin/env python3
"""
Process Epstein email dataset from Hugging Face.
Downloads notesbymuneeb/epstein-emails and builds SQLite database.
"""

import json
import re
import sqlite3
from collections import defaultdict
from pathlib import Path
from datasets import load_dataset

PROJECT_ROOT = Path(__file__).parent.parent
DB_PATH = PROJECT_ROOT / "preprocessed" / "epstein_emails.db"


def normalize_email(email: str) -> str:
    """Normalize email address."""
    if not email:
        return ""
    return email.lower().strip()


def normalize_name(name: str) -> str:
    """Normalize person name."""
    if not name:
        return ""
    name = name.strip()
    # Remove extra whitespace
    name = re.sub(r'\s+', ' ', name)
    # Remove quotes
    name = name.strip('"\'')
    return name


def parse_participant(raw: str) -> tuple[str, str]:
    """
    Parse participant string like 'J [jeevacation@gmail.com]' or 'Name <email@domain.com>'
    Returns (name, email)
    """
    if not raw:
        return "", ""
    
    raw = raw.strip()
    
    # Pattern: Name [email] or Name <email>
    match = re.match(r'^(.+?)\s*[\[<]([^\]>]+)[\]>]$', raw)
    if match:
        name = normalize_name(match.group(1))
        email = normalize_email(match.group(2))
        return name, email
    
    # Pattern: just email
    if '@' in raw:
        email = normalize_email(raw)
        # Extract name from email local part
        name = email.split('@')[0].replace('.', ' ').replace('_', ' ').title()
        return name, email
    
    # Just a name
    return normalize_name(raw), ""


def is_automated_sender(name: str, email: str) -> bool:
    """Filter out automated/system senders."""
    automated_patterns = [
        'noreply', 'no-reply', 'donotreply', 'do-not-reply',
        'automated', 'system', 'mailer-daemon', 'postmaster',
        'notification', 'alert', 'info@', 'support@',
        'calendar', 'events@', 'newsletter'
    ]
    combined = (name + ' ' + email).lower()
    return any(pat in combined for pat in automated_patterns)


def create_database():
    """Create SQLite database with schema."""
    conn = sqlite3.connect(str(DB_PATH))
    cursor = conn.cursor()
    
    # Drop existing tables
    cursor.execute("DROP TABLE IF EXISTS person_cooccurrence")
    cursor.execute("DROP TABLE IF EXISTS email_participants")
    cursor.execute("DROP TABLE IF EXISTS people")
    cursor.execute("DROP TABLE IF EXISTS emails")
    
    # Create tables
    cursor.execute("""
    CREATE TABLE emails (
        thread_id TEXT PRIMARY KEY,
        source_file TEXT,
        subject TEXT,
        message_count INTEGER,
        messages_json TEXT
    )
    """)
    
    cursor.execute("""
    CREATE TABLE people (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT NOT NULL,
        email TEXT,
        total_threads INTEGER DEFAULT 0,
        UNIQUE(name, email)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE email_participants (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        thread_id TEXT NOT NULL,
        person_id INTEGER NOT NULL,
        role TEXT NOT NULL,
        FOREIGN KEY (thread_id) REFERENCES emails(thread_id),
        FOREIGN KEY (person_id) REFERENCES people(id)
    )
    """)
    
    cursor.execute("""
    CREATE TABLE person_cooccurrence (
        person_a INTEGER NOT NULL,
        person_b INTEGER NOT NULL,
        thread_count INTEGER DEFAULT 0,
        PRIMARY KEY (person_a, person_b),
        FOREIGN KEY (person_a) REFERENCES people(id),
        FOREIGN KEY (person_b) REFERENCES people(id)
    )
    """)
    
    # Indexes
    cursor.execute("CREATE INDEX idx_participants_thread ON email_participants(thread_id)")
    cursor.execute("CREATE INDEX idx_participants_person ON email_participants(person_id)")
    cursor.execute("CREATE INDEX idx_cooccurrence_a ON person_cooccurrence(person_a)")
    cursor.execute("CREATE INDEX idx_cooccurrence_b ON person_cooccurrence(person_b)")
    cursor.execute("CREATE INDEX idx_people_name ON people(name)")
    
    conn.commit()
    return conn


def process_dataset():
    """Download and process the Hugging Face dataset."""
    print("Downloading dataset from Hugging Face...")
    ds = load_dataset('notesbymuneeb/epstein-emails', split='train')
    print(f"Downloaded {len(ds)} email threads")
    
    conn = create_database()
    cursor = conn.cursor()
    
    # Track people: (name, email) -> person_id
    people_cache = {}
    # Track thread participants: thread_id -> set of person_ids
    thread_participants = defaultdict(set)
    
    def get_or_create_person(name: str, email: str) -> int:
        """Get or create person, return person_id."""
        # Normalize
        name = normalize_name(name)
        email = normalize_email(email)
        
        if not name and not email:
            return None
        
        # Use email as primary key if available, else name
        key = (name.lower() if name else "", email)
        
        if key in people_cache:
            return people_cache[key]
        
        # Insert new person
        cursor.execute(
            "INSERT OR IGNORE INTO people (name, email) VALUES (?, ?)",
            (name or email.split('@')[0].title(), email)
        )
        
        # Get the ID
        cursor.execute(
            "SELECT id FROM people WHERE name = ? AND email = ?",
            (name or email.split('@')[0].title(), email)
        )
        row = cursor.fetchone()
        if row:
            person_id = row[0]
            people_cache[key] = person_id
            return person_id
        return None
    
    print("Processing email threads...")
    processed = 0
    skipped = 0
    
    for item in ds:
        thread_id = item.get('thread_id', f"thread_{processed}")
        source_file = item.get('source_file', '')
        subject = item.get('subject', '')
        messages_raw = item.get('messages', '[]')
        
        # Parse messages - may be list of dicts already, or JSON string
        try:
            if isinstance(messages_raw, str):
                messages = json.loads(messages_raw)
            elif isinstance(messages_raw, list):
                messages = messages_raw
            else:
                messages = [messages_raw] if messages_raw else []
        except (json.JSONDecodeError, TypeError):
            skipped += 1
            continue
        
        if not isinstance(messages, list):
            messages = [messages] if messages else []
        
        message_count = len(messages)
        
        # Insert email thread
        cursor.execute(
            "INSERT OR REPLACE INTO emails (thread_id, source_file, subject, message_count, messages_json) VALUES (?, ?, ?, ?, ?)",
            (thread_id, source_file, subject, message_count, json.dumps(messages))
        )
        
        # Extract participants from each message
        participants_in_thread = set()
        
        for msg in messages:
            if not isinstance(msg, dict):
                continue
            
            # Process sender (field is 'sender' not 'from')
            sender = msg.get('sender', '') or msg.get('from', '')
            if sender:
                name, email = parse_participant(sender)
                if not is_automated_sender(name, email):
                    person_id = get_or_create_person(name, email)
                    if person_id:
                        participants_in_thread.add(person_id)
                        cursor.execute(
                            "INSERT INTO email_participants (thread_id, person_id, role) VALUES (?, ?, ?)",
                            (thread_id, person_id, 'sender')
                        )
            
            # Process recipients (field is 'recipients' not 'to')
            recipients = msg.get('recipients', []) or msg.get('to', [])
            if isinstance(recipients, str):
                recipients = [recipients]
            
            for recip in recipients:
                name, email = parse_participant(recip)
                if not is_automated_sender(name, email):
                    person_id = get_or_create_person(name, email)
                    if person_id:
                        participants_in_thread.add(person_id)
                        cursor.execute(
                            "INSERT INTO email_participants (thread_id, person_id, role) VALUES (?, ?, ?)",
                            (thread_id, person_id, 'recipient')
                        )
            
            # Process CC
            cc_list = msg.get('cc', [])
            if isinstance(cc_list, str):
                cc_list = [cc_list]
            
            for cc in cc_list:
                name, email = parse_participant(cc)
                if not is_automated_sender(name, email):
                    person_id = get_or_create_person(name, email)
                    if person_id:
                        participants_in_thread.add(person_id)
                        cursor.execute(
                            "INSERT INTO email_participants (thread_id, person_id, role) VALUES (?, ?, ?)",
                            (thread_id, person_id, 'cc')
                        )
        
        thread_participants[thread_id] = participants_in_thread
        processed += 1
        
        if processed % 500 == 0:
            print(f"  Processed {processed} threads...")
            conn.commit()
    
    conn.commit()
    print(f"Processed {processed} threads, skipped {skipped}")
    
    # Update total_threads for each person
    print("Updating thread counts...")
    cursor.execute("""
    UPDATE people SET total_threads = (
        SELECT COUNT(DISTINCT thread_id) 
        FROM email_participants 
        WHERE email_participants.person_id = people.id
    )
    """)
    conn.commit()
    
    # Build co-occurrence matrix
    print("Building co-occurrence matrix...")
    cooccurrence = defaultdict(int)
    
    for thread_id, participants in thread_participants.items():
        participants_list = sorted(participants)
        for i, p1 in enumerate(participants_list):
            for p2 in participants_list[i+1:]:
                # Always store smaller id first
                key = (min(p1, p2), max(p1, p2))
                cooccurrence[key] += 1
    
    print(f"Found {len(cooccurrence)} unique co-occurrence pairs")
    
    # Insert co-occurrences
    for (p1, p2), count in cooccurrence.items():
        cursor.execute(
            "INSERT OR REPLACE INTO person_cooccurrence (person_a, person_b, thread_count) VALUES (?, ?, ?)",
            (p1, p2, count)
        )
    
    conn.commit()
    
    # Print stats
    cursor.execute("SELECT COUNT(*) FROM people")
    total_people = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM emails")
    total_emails = cursor.fetchone()[0]
    
    cursor.execute("SELECT COUNT(*) FROM person_cooccurrence")
    total_cooccur = cursor.fetchone()[0]
    
    print("\n" + "=" * 60)
    print("DATABASE CREATED SUCCESSFULLY")
    print("=" * 60)
    print(f"  Emails:        {total_emails:,}")
    print(f"  People:        {total_people:,}")
    print(f"  Co-occurrences: {total_cooccur:,}")
    print(f"  Database:      {DB_PATH}")
    
    # Top 20 most connected people
    print("\n" + "=" * 60)
    print("TOP 20 MOST CONNECTED PEOPLE")
    print("=" * 60)
    cursor.execute("""
    SELECT p.id, p.name, p.email, p.total_threads,
           (SELECT COUNT(*) FROM person_cooccurrence 
            WHERE person_a = p.id OR person_b = p.id) as connections
    FROM people p
    ORDER BY connections DESC, total_threads DESC
    LIMIT 20
    """)
    
    top_people = []
    for i, row in enumerate(cursor.fetchall(), 1):
        print(f"{i:2}. {row[1]:30} ({row[2] or 'no email':30}) - {row[3]} threads, {row[4]} connections")
        top_people.append({
            'rank': i,
            'name': row[1],
            'email': row[2],
            'threads': row[3],
            'connections': row[4]
        })
    
    # Sample interesting threads (high participant count)
    print("\n" + "=" * 60)
    print("SAMPLE INTERESTING EMAIL THREADS")
    print("=" * 60)
    cursor.execute("""
    SELECT e.thread_id, e.subject, e.message_count, 
           COUNT(DISTINCT ep.person_id) as participants
    FROM emails e
    JOIN email_participants ep ON e.thread_id = ep.thread_id
    GROUP BY e.thread_id
    ORDER BY participants DESC, e.message_count DESC
    LIMIT 15
    """)
    
    for row in cursor.fetchall():
        subject = (row[1] or 'No Subject')[:60]
        print(f"  [{row[2]} msgs, {row[3]} people] {subject}")
    
    conn.close()
    
    return {
        'total_people': total_people,
        'total_emails': total_emails,
        'total_cooccur': total_cooccur,
        'top_people': top_people
    }


if __name__ == "__main__":
    process_dataset()
