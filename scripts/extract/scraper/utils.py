import time
import csv
import json
import re
from datetime import datetime
from dateutil import parser as dateparser
from typing import List, Dict, Any, Optional, Union
from pathlib import Path
from .core.sealed import sealed

class TimeManager(metaclass=sealed()):
    @staticmethod
    def polite_sleep(delay: float) -> None:
        if delay and delay > 0:
            time.sleep(delay)
    
    @staticmethod
    def now_stamp() -> str:
        return datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    
    @staticmethod
    def parse_iso_date(date_string: Optional[str]) -> Optional[str]:
        if not date_string:
            return None
        try:
            return dateparser.parse(date_string).date().isoformat()
        except Exception:
            return None


class DirectoryManager(metaclass=sealed()):
    @staticmethod
    def ensure_outdir(path: str = "out") -> None:
        Path(path).mkdir(exist_ok=True)
    
    @staticmethod
    def ensure_directory(path: Union[str, Path]) -> None:
        Path(path).mkdir(parents=True, exist_ok=True)


class FileWriter(metaclass=sealed()):
    @staticmethod
    def write_jsonl(records: List[Dict[str, Any]], path: str) -> None:
        with open(path, "w", encoding="utf-8") as file:
            for record in records:
                file.write(json.dumps(record, ensure_ascii=False) + "\n")
    
    @staticmethod
    def write_csv(records: List[Dict[str, Any]], path: str, 
                  field_order: Optional[List[str]] = None) -> None:
        if not records:
            Path(path).touch()
            return
        
        fieldnames = field_order or sorted(set().union(*[record.keys() for record in records]))
        
        with open(path, "w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=fieldnames)
            writer.writeheader()
            
            for record in records:
                normalized_record = {
                    key: (json.dumps(value, ensure_ascii=False) 
                         if isinstance(value, (dict, list)) else value)
                    for key, value in record.items()
                }
                writer.writerow(normalized_record)


class TextProcessor(metaclass=sealed()):
    @staticmethod
    def slugify(text: Optional[str]) -> str:
        if not text:
            return ""
        
        text = re.sub(r"[^a-zA-Z0-9_-]+", "-", text.strip())
        return re.sub(r"-+", "-", text).strip("-").lower()
    
    @staticmethod
    def clean_text(text: Optional[str]) -> Optional[str]:
        if not text:
            return None
        
        cleaned = re.sub(r'\s+', ' ', text.strip())
        return cleaned if cleaned else None
    
    @staticmethod
    def extract_numbers(text: str) -> List[float]:
        pattern = r"[-+]?(?:\d*\.?\d+)"
        matches = re.findall(pattern, text)
        return [float(match) for match in matches if match]


def polite_sleep(delay: float) -> None:
    TimeManager.polite_sleep(delay)


def now_stamp() -> str:
    return TimeManager.now_stamp()


def ensure_outdir(path: str = "out") -> None:
    DirectoryManager.ensure_outdir(path)


def write_jsonl(records: List[Dict[str, Any]], path: str) -> None:
    FileWriter.write_jsonl(records, path)


def write_csv(records: List[Dict[str, Any]], path: str, 
              field_order: Optional[List[str]] = None) -> None:
    FileWriter.write_csv(records, path, field_order)


def slugify(text: Optional[str]) -> str:
    return TextProcessor.slugify(text)


def parse_iso_date(date_string: Optional[str]) -> Optional[str]:
    return TimeManager.parse_iso_date(date_string)

def load_env() -> dict:
    env_path = Path(__file__).parent / '.env'
    env_vars = {}
    
    if env_path.exists():
        with open(env_path, 'r') as f:
            for line in f:
                line = line.strip()
                if line and not line.startswith('#') and '=' in line:
                    key, value = line.split('=', 1)
                    env_vars[key] = value.strip('"').strip("'")
    
    return env_vars