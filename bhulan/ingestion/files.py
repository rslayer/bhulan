"""
File ingestion for GPS data.

Supports CSV, JSON, Excel (XLSX), and Parquet files from local filesystem or S3.
"""

import csv
import json
from typing import List, Dict, Any, Optional, Iterator
from pathlib import Path
import pandas as pd
from bhulan.ingestion.normalize import MappingPlan, normalize_batch
from bhulan.models.canonical import NormalizationResult, TrackPoint
from bhulan.models.vendor.generic import infer_field_mapping, create_generic_mapping
from bhulan.storage.mongo_repo import MongoTrackPointRepository, MongoJobRegistry
from bhulan.config.settings import settings
import uuid


class FileIngestionError(Exception):
    """Raised when file ingestion fails."""
    pass


def detect_file_type(file_path: str) -> str:
    """
    Detect file type from extension.
    
    Args:
        file_path: Path to file
        
    Returns:
        File type (csv, json, xlsx, parquet)
        
    Raises:
        FileIngestionError: If file type is not supported
    """
    path = Path(file_path)
    ext = path.suffix.lower()
    
    type_map = {
        '.csv': 'csv',
        '.json': 'json',
        '.jsonl': 'jsonl',
        '.ndjson': 'jsonl',
        '.xlsx': 'xlsx',
        '.xls': 'xlsx',
        '.parquet': 'parquet',
    }
    
    if ext not in type_map:
        raise FileIngestionError(f"Unsupported file type: {ext}")
    
    return type_map[ext]


def read_csv_file(file_path: str, chunk_size: int = 1000) -> Iterator[List[Dict[str, Any]]]:
    """
    Read CSV file in chunks.
    
    Args:
        file_path: Path to CSV file
        chunk_size: Number of rows per chunk
        
    Yields:
        Chunks of records as list of dictionaries
    """
    for chunk_df in pd.read_csv(file_path, chunksize=chunk_size):
        records = chunk_df.to_dict('records')
        yield records


def read_json_file(file_path: str) -> List[Dict[str, Any]]:
    """
    Read JSON file (array or NDJSON).
    
    Args:
        file_path: Path to JSON file
        
    Returns:
        List of records
    """
    with open(file_path, 'r') as f:
        first_char = f.read(1)
        f.seek(0)
        
        if first_char == '[':
            return json.load(f)
        else:
            records = []
            for line in f:
                if line.strip():
                    records.append(json.loads(line))
            return records


def read_excel_file(file_path: str, chunk_size: int = 1000) -> Iterator[List[Dict[str, Any]]]:
    """
    Read Excel file in chunks.
    
    Args:
        file_path: Path to Excel file
        chunk_size: Number of rows per chunk
        
    Yields:
        Chunks of records as list of dictionaries
    """
    df = pd.read_excel(file_path)
    
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i+chunk_size]
        records = chunk.to_dict('records')
        yield records


def read_parquet_file(file_path: str, chunk_size: int = 1000) -> Iterator[List[Dict[str, Any]]]:
    """
    Read Parquet file in chunks.
    
    Args:
        file_path: Path to Parquet file
        chunk_size: Number of rows per chunk
        
    Yields:
        Chunks of records as list of dictionaries
    """
    df = pd.read_parquet(file_path)
    
    for i in range(0, len(df), chunk_size):
        chunk = df.iloc[i:i+chunk_size]
        records = chunk.to_dict('records')
        yield records


def infer_mapping_from_file(file_path: str, file_type: str) -> MappingPlan:
    """
    Infer field mapping from file headers.
    
    Args:
        file_path: Path to file
        file_type: File type (csv, json, xlsx, parquet)
        
    Returns:
        Inferred MappingPlan
    """
    if file_type == 'csv':
        with open(file_path, 'r') as f:
            reader = csv.DictReader(f)
            headers = reader.fieldnames
    elif file_type == 'xlsx':
        df = pd.read_excel(file_path, nrows=0)
        headers = df.columns.tolist()
    elif file_type == 'parquet':
        df = pd.read_parquet(file_path)
        headers = df.columns.tolist()
    elif file_type in ['json', 'jsonl']:
        records = read_json_file(file_path)
        if records:
            headers = list(records[0].keys())
        else:
            headers = []
    else:
        raise FileIngestionError(f"Cannot infer mapping for file type: {file_type}")
    
    field_map = infer_field_mapping(headers)
    
    return MappingPlan(
        field_map=field_map,
        unit_map={},
        defaults={'src': 'file'},
        vendor='generic'
    )


def ingest_file(
    file_path: str,
    mapping: Optional[MappingPlan] = None,
    ingest_id: Optional[str] = None,
    vendor: str = 'generic',
    repo: Optional[MongoTrackPointRepository] = None,
    job_registry: Optional[MongoJobRegistry] = None
) -> NormalizationResult:
    """
    Ingest GPS data from file.
    
    Args:
        file_path: Path to file (local or s3://)
        mapping: Mapping plan (inferred if not provided)
        ingest_id: Ingestion job ID (generated if not provided)
        vendor: Vendor identifier
        repo: Track point repository (created if not provided)
        job_registry: Job registry (created if not provided)
        
    Returns:
        NormalizationResult with statistics
    """
    if ingest_id is None:
        ingest_id = str(uuid.uuid4())
    
    if repo is None:
        repo = MongoTrackPointRepository()
    if job_registry is None:
        job_registry = MongoJobRegistry()
    
    job_registry.create_job(
        ingest_id=ingest_id,
        source='file',
        params={'file_path': file_path, 'vendor': vendor}
    )
    
    try:
        file_type = detect_file_type(file_path)
        
        if mapping is None:
            mapping = infer_mapping_from_file(file_path, file_type)
        
        total_read = 0
        total_accepted = 0
        total_rejected = 0
        all_errors = {}
        
        if file_type == 'csv':
            reader = read_csv_file(file_path, chunk_size=settings.MAX_BATCH_SIZE)
        elif file_type == 'xlsx':
            reader = read_excel_file(file_path, chunk_size=settings.MAX_BATCH_SIZE)
        elif file_type == 'parquet':
            reader = read_parquet_file(file_path, chunk_size=settings.MAX_BATCH_SIZE)
        elif file_type in ['json', 'jsonl']:
            records = read_json_file(file_path)
            reader = [records[i:i+settings.MAX_BATCH_SIZE] 
                     for i in range(0, len(records), settings.MAX_BATCH_SIZE)]
        
        for chunk in reader:
            total_read += len(chunk)
            
            result, points = normalize_batch(chunk, mapping, ingest_id)
            
            total_accepted += result.accepted
            total_rejected += result.rejected
            
            for idx, error in result.errors.items():
                global_idx = total_read - len(chunk) + idx
                all_errors[global_idx] = error
            
            if points:
                repo.upsert_batch(points)
        
        job_registry.update_job_status(
            ingest_id=ingest_id,
            status='succeeded' if total_rejected == 0 else 'partial',
            stats={
                'read': total_read,
                'accepted': total_accepted,
                'rejected': total_rejected
            },
            error_sample=dict(list(all_errors.items())[:10])  # First 10 errors
        )
        
        return NormalizationResult(
            accepted=total_accepted,
            rejected=total_rejected,
            errors=all_errors,
            ingest_id=ingest_id
        )
        
    except Exception as e:
        job_registry.update_job_status(
            ingest_id=ingest_id,
            status='failed',
            error_sample={0: str(e)}
        )
        raise FileIngestionError(f"File ingestion failed: {str(e)}")
