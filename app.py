#!/usr/bin/env python3
"""
Milkbasket GRN Automation - GitHub Actions Compatible
Runs Gmail → Drive and Drive → Sheet workflows every 3 hours via GitHub Actions scheduler.
"""

import os
import json
import base64
import tempfile
import time
import logging
import schedule
import re
import warnings
from datetime import datetime, timedelta, timezone
from typing import List, Dict, Any, Optional
from io import BytesIO

from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
import io

# === LLAMA CLOUD SDK ===
try:
    from llama_cloud import LlamaCloud
    LLAMA_AVAILABLE = True
except ImportError:
    LLAMA_AVAILABLE = False
    print("WARNING: llama_cloud not available. Run: pip install llama-cloud>=2.1")

warnings.filterwarnings("ignore")

# Configure logging — file + stdout (GitHub Actions captures both)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hyperpure_automation.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Hardcoded configuration
CONFIG = {
    'gmail': {
        'sender': "noreply@hyperpure.com",
        'search_term': "Please find attached a copy of PO",
        'days_back': 1,
        'max_results': 1,
        'gdrive_folder_id': "1hjF05nxRUrK-bH67SVow1p140RVWs278"
    },
    'pdf': {
        'drive_folder_id': "1oRc5usVewCoAiq6uKpgY7G6xtAO_6Hz3",
        'llama_api_key': os.environ.get('LLAMA_CLOUD_API_KEY', 'llx-fscu1pzDJ2LATNBsIhXXrqmde8ajmQVx3LQ6eStBCjQr2WhF'),
        'llama_agent': "HyperPO",
        'spreadsheet_id': "1ZfKCYAaUJCSw2g-yTC9zxxYyZ4h5Ffyn2mUvA7aFufU",
        'sheet_range': "pos",
        'days_back': 1,
        'max_files': 10,
        'failed_extractions_sheet': 'failed_extractions'
    },
    'workflow_log': {
        'spreadsheet_id': "1ZfKCYAaUJCSw2g-yTC9zxxYyZ4h5Ffyn2mUvA7aFufU",
        'sheet_range': "workflow_logs"
    },
    'remaining_files': {
        'spreadsheet_id': "1ZfKCYAaUJCSw2g-yTC9zxxYyZ4h5Ffyn2mUvA7aFufU",
        'sheet_range': "remaining_files"
    },
    'credentials_path': 'credentials.json',
    'token_path': 'token.json'
}

MAX_RETRIES = 3
RETRY_WAIT = 2


class MilkbasketAutomation:
    def __init__(self):
        self.gmail_service = None
        self.drive_service = None
        self.sheets_service = None

        self.gmail_scopes = ['https://www.googleapis.com/auth/gmail.readonly']
        self.drive_scopes = ['https://www.googleapis.com/auth/drive']
        self.sheets_scopes = ['https://www.googleapis.com/auth/spreadsheets']

        self.logs = []
        self.current_stats = {
            'gmail': {'processed_emails': 0, 'total_attachments': 0, 'successful_uploads': 0, 'failed_uploads': 0},
            'pdf': {'total_pdfs': 0, 'processed_pdfs': 0, 'skipped_pdfs': 0, 'failed_pdfs': 0, 'rows_added': 0}
        }

    # ─────────────────────────── Logging helpers ────────────────────────────

    def log(self, message: str, level: str = "INFO"):
        if level.upper() == "ERROR":
            logging.error(message)
        elif level.upper() == "WARNING":
            logging.warning(message)
        elif level.upper() == "SUCCESS":
            logging.info(f"✅ {message}")
        else:
            logging.info(message)

        timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.logs.append({"timestamp": timestamp, "level": level.upper(), "message": message})
        if len(self.logs) > 200:
            self.logs = self.logs[-200:]

    def get_logs(self): return self.logs
    def clear_logs(self): self.logs = []

    def reset_stats(self):
        self.current_stats = {
            'gmail': {'processed_emails': 0, 'total_attachments': 0, 'successful_uploads': 0, 'failed_uploads': 0},
            'pdf': {'total_pdfs': 0, 'processed_pdfs': 0, 'skipped_pdfs': 0, 'failed_pdfs': 0, 'rows_added': 0}
        }

    def get_stats(self): return self.current_stats

    # ──────────────────────────── Authentication ────────────────────────────

    def authenticate(self):
        """Authenticate using credentials.json / token.json (pre-restored by GitHub Actions)."""
        try:
            self.log("Starting authentication process...", "INFO")
            combined_scopes = list(set(self.gmail_scopes + self.drive_scopes + self.sheets_scopes))

            creds = None
            if os.path.exists(CONFIG['token_path']):
                creds = Credentials.from_authorized_user_file(CONFIG['token_path'], combined_scopes)

            if not creds or not creds.valid:
                if creds and creds.expired and creds.refresh_token:
                    self.log("Refreshing expired token...", "INFO")
                    creds.refresh(Request())
                    # Persist refreshed token so GitHub Actions can capture it
                    with open(CONFIG['token_path'], 'w') as token:
                        token.write(creds.to_json())
                    self.log("Token refreshed and saved.", "INFO")
                else:
                    if not os.path.exists(CONFIG['credentials_path']):
                        self.log(f"credentials.json not found at '{CONFIG['credentials_path']}'", "ERROR")
                        return False
                    self.log("Starting new OAuth flow (local only — not supported in CI)...", "INFO")
                    flow = InstalledAppFlow.from_client_secrets_file(CONFIG['credentials_path'], combined_scopes)
                    creds = flow.run_local_server(port=0)
                    with open(CONFIG['token_path'], 'w') as token:
                        token.write(creds.to_json())
                    self.log("OAuth authentication successful!", "SUCCESS")

            self.gmail_service = build('gmail', 'v1', credentials=creds)
            self.drive_service = build('drive', 'v3', credentials=creds)
            self.sheets_service = build('sheets', 'v4', credentials=creds)

            self.log("Authentication successful!", "INFO")
            return True

        except Exception as e:
            self.log(f"Authentication failed: {str(e)}", "ERROR")
            return False

    # ─────────────────────────── Retry wrapper ──────────────────────────────

    def retry_wrapper(self, func, *args, **kwargs):
        for attempt in range(1, MAX_RETRIES + 1):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                self.log(f"Attempt {attempt} failed: {str(e)}", "WARNING")
                if attempt < MAX_RETRIES:
                    time.sleep(RETRY_WAIT)
                else:
                    raise

    # ───────────────────────── Gmail → Drive workflow ───────────────────────

    def search_emails(self, sender: str = "", search_term: str = "",
                      days_back: int = 7, max_results: int = 50) -> List[Dict]:
        def _search():
            query_parts = ["has:attachment"]
            if sender:
                query_parts.append(f'from:"{sender}"')
            if search_term:
                if "," in search_term:
                    keywords = [k.strip() for k in search_term.split(",")]
                    keyword_query = " OR ".join([f'"{k}"' for k in keywords if k])
                    if keyword_query:
                        query_parts.append(f"({keyword_query})")
                else:
                    query_parts.append(f'"{search_term}"')
            start_date = datetime.now() - timedelta(days=days_back)
            query_parts.append(f"after:{start_date.strftime('%Y/%m/%d')}")
            query = " ".join(query_parts)
            self.log(f"Gmail search query: {query}", "INFO")
            result = self.gmail_service.users().messages().list(
                userId='me', q=query, maxResults=max_results
            ).execute()
            messages = result.get('messages', [])
            self.log(f"Found {len(messages)} emails matching criteria", "INFO")
            return messages
        try:
            return self.retry_wrapper(_search)
        except Exception as e:
            self.log(f"Email search failed: {str(e)}", "ERROR")
            return []

    def get_email_details(self, message_id: str) -> Dict:
        try:
            message = self.gmail_service.users().messages().get(
                userId='me', id=message_id, format='metadata'
            ).execute()
            headers = message['payload'].get('headers', [])
            return {
                'id': message_id,
                'sender': next((h['value'] for h in headers if h['name'] == "From"), "Unknown"),
                'subject': next((h['value'] for h in headers if h['name'] == "Subject"), "(No Subject)"),
                'date': next((h['value'] for h in headers if h['name'] == "Date"), "")
            }
        except Exception as e:
            self.log(f"Failed to get email details: {str(e)}", "ERROR")
            return {'id': message_id, 'sender': 'Unknown', 'subject': 'Unknown', 'date': ''}

    def _create_drive_folder(self, folder_name: str, parent_folder_id: Optional[str] = None) -> str:
        try:
            query = f"name='{folder_name}' and mimeType='application/vnd.google-apps.folder' and trashed=false"
            if parent_folder_id:
                query += f" and '{parent_folder_id}' in parents"
            existing = self.drive_service.files().list(q=query, fields='files(id, name)').execute()
            if existing.get('files'):
                return existing['files'][0]['id']
            folder_metadata = {'name': folder_name, 'mimeType': 'application/vnd.google-apps.folder'}
            if parent_folder_id:
                folder_metadata['parents'] = [parent_folder_id]
            folder = self.drive_service.files().create(body=folder_metadata, fields='id').execute()
            return folder.get('id')
        except Exception as e:
            self.log(f"Failed to create folder '{folder_name}': {str(e)}", "ERROR")
            return ""

    def _file_exists_in_folder(self, filename: str, folder_id: str) -> bool:
        try:
            query = f"name='{filename}' and '{folder_id}' in parents and trashed=false"
            files = self.drive_service.files().list(q=query, fields='files(id, name)').execute().get('files', [])
            return len(files) > 0
        except Exception:
            return False

    def _sanitize_filename(self, filename: str) -> str:
        cleaned = re.sub(r'[<>:"/\\|?*]', '_', filename)
        if len(cleaned) > 100:
            parts = cleaned.split('.')
            cleaned = f"{'.'.join(parts[:-1])[:95]}.{parts[-1]}" if len(parts) > 1 else cleaned[:100]
        return cleaned

    def _classify_extension(self, filename: str) -> str:
        if not filename or '.' not in filename:
            return "Other"
        ext = filename.split(".")[-1].lower()
        return {
            "pdf": "PDFs", "doc": "Documents", "docx": "Documents", "txt": "Documents",
            "xls": "Spreadsheets", "xlsx": "Spreadsheets", "csv": "Spreadsheets",
            "jpg": "Images", "jpeg": "Images", "png": "Images", "gif": "Images",
            "ppt": "Presentations", "pptx": "Presentations",
            "zip": "Archives", "rar": "Archives", "7z": "Archives",
        }.get(ext, "Other")

    def _extract_attachments_from_email(self, message_id: str, payload: Dict, config: dict, base_folder_id: str) -> int:
        processed_count = 0
        if "parts" in payload:
            for part in payload["parts"]:
                processed_count += self._extract_attachments_from_email(message_id, part, config, base_folder_id)
        elif payload.get("filename") and "attachmentId" in payload.get("body", {}):
            filename = payload.get("filename", "")
            try:
                attachment_id = payload["body"].get("attachmentId")
                att = self.gmail_service.users().messages().attachments().get(
                    userId='me', messageId=message_id, id=attachment_id
                ).execute()
                file_data = base64.urlsafe_b64decode(att["data"].encode("UTF-8"))

                search_folder_id = self._create_drive_folder(config.get('search_term', 'all-attachments'), base_folder_id)
                type_folder_id = self._create_drive_folder(self._classify_extension(filename), search_folder_id)

                clean_filename = self._sanitize_filename(filename)
                if not self._file_exists_in_folder(clean_filename, type_folder_id):
                    file_metadata = {'name': clean_filename, 'parents': [type_folder_id]}
                    media = MediaIoBaseUpload(BytesIO(file_data), mimetype='application/octet-stream', resumable=True)
                    self.drive_service.files().create(body=file_metadata, media_body=media, fields='id').execute()
                    self.log(f"Uploaded: {clean_filename}", "INFO")
                    processed_count = 1
                else:
                    self.log(f"File already exists, skipping: {clean_filename}", "INFO")
            except Exception as e:
                self.log(f"Failed to process attachment '{filename}': {str(e)}", "ERROR")
        return processed_count

    def process_gmail_workflow(self, config: dict, progress_callback=None, status_callback=None):
        try:
            if status_callback: status_callback("Starting Gmail workflow...")
            self.log("Starting Gmail workflow", "INFO")
            if progress_callback: progress_callback(10)

            emails = self.search_emails(
                sender=config['sender'],
                search_term=config['search_term'],
                days_back=config['days_back'],
                max_results=config['max_results']
            )
            self.current_stats['gmail']['processed_emails'] = len(emails)

            if progress_callback: progress_callback(25)
            if not emails:
                self.log("No emails found matching criteria", "WARNING")
                return {'success': True, 'processed': 0, 'total_attachments': 0, 'failed': 0, 'emails_processed': 0}

            self.log(f"Found {len(emails)} emails. Processing attachments...", "INFO")

            base_folder_id = self._create_drive_folder("Gmail_Attachments", config.get('gdrive_folder_id'))
            if not base_folder_id:
                self.log("Failed to create base folder in Google Drive", "ERROR")
                return {'success': False, 'processed': 0, 'total_attachments': 0, 'failed': 0, 'emails_processed': 0}

            if progress_callback: progress_callback(50)

            processed_count = 0
            total_attachments = 0
            failed_count = 0

            for i, email in enumerate(emails):
                try:
                    if status_callback: status_callback(f"Processing email {i+1}/{len(emails)}")
                    email_details = self.get_email_details(email['id'])
                    subject = email_details.get('subject', 'No Subject')[:50]

                    inv_match = re.search(r'against Inv: (\S+)', subject)
                    if inv_match and '/' in inv_match.group(1):
                        self.log(f"Skipping email (contains '/'): {subject}", "INFO")
                        continue

                    self.log(f"Processing email: {subject}", "INFO")
                    message = self.gmail_service.users().messages().get(
                        userId='me', id=email['id'], format='full'
                    ).execute()
                    attachment_count = self._extract_attachments_from_email(
                        email['id'], message['payload'], config, base_folder_id
                    )

                    total_attachments += attachment_count
                    self.current_stats['gmail']['total_attachments'] += attachment_count
                    if attachment_count > 0:
                        self.current_stats['gmail']['successful_uploads'] += attachment_count
                        processed_count += 1
                        self.log(f"Found {attachment_count} attachments", "SUCCESS")
                    else:
                        failed_count += 1

                    if progress_callback:
                        progress_callback(int(50 + (i + 1) / len(emails) * 45))

                except Exception as e:
                    self.log(f"Failed to process email: {str(e)}", "ERROR")
                    self.current_stats['gmail']['failed_uploads'] += 1
                    failed_count += 1

            if progress_callback: progress_callback(100)
            if status_callback: status_callback(f"Gmail workflow completed! Processed {total_attachments} attachments")
            self.log(f"Gmail workflow completed! {total_attachments} attachments from {processed_count} emails", "SUCCESS")

            return {
                'success': True,
                'processed': total_attachments,
                'total_attachments': len(emails),
                'failed': failed_count,
                'emails_processed': len(emails)
            }
        except Exception as e:
            self.log(f"Gmail workflow failed: {str(e)}", "ERROR")
            return {'success': False, 'processed': 0, 'total_attachments': 0, 'failed': 0, 'emails_processed': 0}

    # ─────────────────────────── Drive → Sheet workflow ─────────────────────

    def list_drive_pdfs(self, folder_id: str, days_back: int = 1, all_time: bool = False) -> List[Dict]:
        try:
            if all_time:
                query = f"'{folder_id}' in parents and mimeType='application/pdf' and trashed=false"
            else:
                start_datetime = datetime.now(timezone.utc) - timedelta(days=days_back - 1)
                start_str = start_datetime.strftime('%Y-%m-%dT00:00:00Z')
                query = (
                    f"'{folder_id}' in parents and mimeType='application/pdf' "
                    f"and trashed=false and createdTime >= '{start_str}'"
                )
            all_files = []
            page_token = None
            while True:
                results = self.drive_service.files().list(
                    q=query,
                    fields="nextPageToken, files(id, name, mimeType, createdTime, modifiedTime)",
                    orderBy="createdTime desc",
                    pageSize=1000,
                    pageToken=page_token
                ).execute()
                all_files.extend(results.get('files', []))
                page_token = results.get('nextPageToken')
                if not page_token:
                    break
            label = 'all time' if all_time else f'last {days_back} days'
            self.log(f"Found {len(all_files)} PDF files in folder ({label})", "INFO")
            return all_files
        except Exception as e:
            self.log(f"Failed to list PDFs: {str(e)}", "ERROR")
            return []

    def download_from_drive(self, file_id: str, file_name: str) -> bytes:
        try:
            self.log(f"Downloading: {file_name}", "INFO")
            data = self.drive_service.files().get_media(fileId=file_id).execute()
            self.log(f"Downloaded: {file_name}", "SUCCESS")
            return data
        except Exception as e:
            self.log(f"Failed to download {file_name}: {str(e)}", "ERROR")
            return b""

    def get_existing_drive_ids(self, spreadsheet_id: str, sheet_range: str) -> set:
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id, range=sheet_range, majorDimension="ROWS"
            ).execute()
            values = result.get('values', [])
            if not values:
                return set()
            headers = values[0]
            if "drive_file_id" not in headers:
                self.log("No 'drive_file_id' column found in sheet", "WARNING")
                return set()
            id_index = headers.index("drive_file_id")
            ids = {row[id_index] for row in values[1:] if len(row) > id_index and row[id_index]}
            self.log(f"Found {len(ids)} existing file IDs in sheet", "INFO")
            return ids
        except Exception as e:
            self.log(f"Failed to get existing file IDs: {str(e)}", "ERROR")
            return set()

    def get_existing_filenames(self, spreadsheet_id: str, sheet_range: str) -> set:
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id, range=sheet_range, majorDimension="ROWS"
            ).execute()
            values = result.get('values', [])
            if not values:
                return set()
            headers = values[0]
            if "source_file" not in headers:
                return set()
            id_index = headers.index("source_file")
            names = {row[id_index] for row in values[1:] if len(row) > id_index and row[id_index]}
            self.log(f"Found {len(names)} existing file names in sheet", "INFO")
            return names
        except Exception as e:
            self.log(f"Failed to get existing file names: {str(e)}", "ERROR")
            return set()

    def safe_extract(self, client, tmp_path: str, file_info: Dict, config: dict, retries: int = 5):
        """Upload PDF to LlamaCloud and poll until extraction completes."""
        for attempt in range(1, retries + 1):
            try:
                self.log(f"Extracting data (attempt {attempt}/{retries})...", "INFO")
                with open(tmp_path, "rb") as f:
                    file_obj = client.files.create(file=f, purpose="extract")
                file_id = file_obj.id

                job = client.extract.create(
                    file_input=file_id,
                    configuration={
                        "agent_name": config['llama_agent'],
                        "extraction_target": "per_doc",
                        "tier": "agentic",
                        "data_schema": {
                            "type": "object",
                            "properties": {
                                "purchase_order_number": {"type": "string"},
                                "purchase_order_date": {"type": "string"},
                                "expected_delivery_date": {"type": "string"},
                                "vendor_id": {"type": "string"},
                                "account_number": {"type": "string"},
                                "delivery_charge": {"type": "string"},
                                "total_amount": {"type": "string"},
                                "total_taxable_value_overall": {"type": "string"},
                                "total_tax_amount_overall": {"type": "string"},
                                "amount_chargeable_in_words": {"type": "string"},
                                "buyer": {
                                    "type": "object",
                                    "properties": {
                                        "name": {"type": "string"},
                                        "gstin": {"type": "string"},
                                        "state": {"type": "string"},
                                        "address": {"type": "string"}
                                    }
                                },
                                "supplier": {
                                    "type": "object",
                                    "properties": {
                                        "name": {"type": "string"},
                                        "gstin": {"type": "string"},
                                        "phone": {"type": "string"},
                                        "address": {"type": "string"}
                                    }
                                },
                                "line_items": {
                                    "type": "array",
                                    "items": {
                                        "type": "object",
                                        "properties": {
                                            "product_number": {"type": "string"},
                                            "product_name": {"type": "string"},
                                            "description": {"type": "string"},
                                            "hsn": {"type": "string"},
                                            "unit_of_measure": {"type": "string"},
                                            "quantity_ordered": {"type": "string"},
                                            "mrp": {"type": "string"},
                                            "price_per_unit": {"type": "string"},
                                            "margin_percentage": {"type": "string"},
                                            "gst_rate": {"type": "string"},
                                            "total_amount_item": {"type": "string"},
                                            "total_tax_amount_item": {"type": "string"}
                                        }
                                    }
                                }
                            }
                        }
                    }
                )

                # Poll for completion
                max_wait, wait_interval, elapsed = 300, 10, 0
                while elapsed < max_wait:
                    job = client.extract.get(job.id)
                    status = (job.status or "").upper()
                    if status in ("SUCCESS", "COMPLETED"):
                        self.log(f"Extraction completed (status={job.status})", "SUCCESS")
                        extracted_data = (
                            getattr(job, 'extract_result', None) or
                            getattr(job, 'data', None) or
                            getattr(job, 'result', None) or
                            getattr(getattr(job, 'extraction', None), 'data', None) or
                            getattr(getattr(job, 'output', None), 'data', None) or
                            getattr(job, 'extracted_data', None) or
                            getattr(job, 'json_result', None)
                        )
                        if extracted_data is None:
                            self.log(f"WARNING: Could not find data in job. Full job: {vars(job)}", "WARNING")
                        return extracted_data
                    elif status in ("FAILED", "ERROR", "CANCELLED"):
                        self.log(f"Extraction failed: status={job.status}", "ERROR")
                        break
                    self.log(f"Extraction status: {job.status} ({elapsed}s elapsed)", "INFO")
                    time.sleep(wait_interval)
                    elapsed += wait_interval
                else:
                    self.log(f"Extraction timed out after {max_wait}s", "ERROR")

            except Exception as e:
                self.log(f"Extraction attempt {attempt} failed: {str(e)}", "WARNING")
                time.sleep(RETRY_WAIT)

        self.log(f"Extraction failed after {retries} attempts", "ERROR")
        return None

    def process_extracted_data(self, extracted_data: Dict, file_info: Dict) -> List[Dict]:
        rows = []
        data = extracted_data.get("data", extracted_data)
        line_items = data.get("line_items") or data.get("items")
        if not line_items:
            self.log(f"Skipping (no line_items): {file_info['name']}", "WARNING")
            return rows

        def _get(d, keys, default=""):
            for k in keys:
                if k in d:
                    return d[k]
            return default

        po_number = _get(data, ["purchase_order_number", "po_number", "PO No"])
        po_date = _get(data, ["purchase_order_date", "po_date"])
        expected_delivery_date = _get(data, ["expected_delivery_date", "delivery_date"])
        vendor_id = _get(data, ["vendor_id"])
        account_number = _get(data, ["account_number"])
        delivery_charge = _get(data, ["delivery_charge"])
        total_amount = _get(data, ["total_amount"])
        total_taxable_value = _get(data, ["total_taxable_value_overall"])
        total_tax_amount = _get(data, ["total_tax_amount_overall"])
        amount_chargeable_in_words = _get(data, ["amount_chargeable_in_words"])
        buyer = data.get("buyer", {}) or {}
        supplier = data.get("supplier", {}) or {}

        for item in line_items:
            row = {
                "po_number": po_number,
                "po_date": po_date,
                "expected_delivery_date": expected_delivery_date,
                "vendor_id": vendor_id,
                "account_number": account_number,
                "delivery_charge": delivery_charge,
                "total_amount": total_amount,
                "total_taxable_value_overall": total_taxable_value,
                "total_tax_amount_overall": total_tax_amount,
                "amount_chargeable_in_words": amount_chargeable_in_words,
                "buyer_name": buyer.get("name", ""),
                "buyer_gstin": buyer.get("gstin", ""),
                "buyer_state": buyer.get("state", ""),
                "buyer_address": buyer.get("address", ""),
                "supplier_name": supplier.get("name", ""),
                "supplier_gstin": supplier.get("gstin", ""),
                "supplier_phone": supplier.get("phone", ""),
                "supplier_address": supplier.get("address", ""),
                "product_number": item.get("product_number", ""),
                "product_name": item.get("product_name", ""),
                "description": item.get("description", ""),
                "hsn": item.get("hsn", ""),
                "unit_of_measure": item.get("unit_of_measure", ""),
                "quantity_ordered": item.get("quantity_ordered", ""),
                "mrp": item.get("mrp", ""),
                "price_per_unit": item.get("price_per_unit", ""),
                "margin_percentage": item.get("margin_percentage", ""),
                "gst_rate": item.get("gst_rate", ""),
                "total_amount_item": item.get("total_amount_item", ""),
                "total_tax_amount_item": item.get("total_tax_amount_item", ""),
                "source_file": file_info['name'],
                "drive_file_id": file_info['id'],
                "processed_date": time.strftime("%Y-%m-%d %H:%M:%S"),
            }
            cleaned_row = {k: v for k, v in row.items() if v not in ["", None]}
            rows.append(cleaned_row)
        return rows

    def save_failed_extractions(self, spreadsheet_id: str, sheet_name: str, failed_files: List[Dict]):
        """Save failed/incomplete extraction details to a dedicated sheet."""
        try:
            self.sheets_service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id, range=sheet_name, body={}
            ).execute()
            headers = [['Timestamp', 'File Name', 'File ID', 'Status',
                        'Items Extracted', 'Completeness Score', 'Issues', 'Attempts', 'Strategy Used']]
            self.append_to_google_sheet(spreadsheet_id, sheet_name, headers)
            data = [[
                f.get('timestamp', ''), f.get('file_name', ''), f.get('file_id', ''),
                f.get('status', ''), f.get('items_extracted', 0),
                f"{f.get('completeness_score', 0):.2%}",
                '; '.join(f.get('issues', [])), f.get('attempts', 0), f.get('strategy_used', '')
            ] for f in failed_files]
            if data:
                self.append_to_google_sheet(spreadsheet_id, sheet_name, data)
                self.log(f"Saved {len(failed_files)} failed/incomplete extractions to {sheet_name}", "INFO")
        except Exception as e:
            self.log(f"Failed to save failed extractions: {str(e)}", "ERROR")

    def process_pdf_workflow(self, config: dict, progress_callback=None, status_callback=None):
        stats = {
            'total_pdfs': 0, 'processed_pdfs': 0, 'failed_pdfs': 0,
            'skipped_pdfs': 0, 'rows_added': 0, 'incomplete': 0
        }

        if not LLAMA_AVAILABLE:
            self.log("llama-cloud not installed. Run: pip install llama-cloud>=2.1", "ERROR")
            return {'success': False, **stats}

        try:
            if status_callback: status_callback("Starting PDF processing workflow...")
            self.log("Starting PDF processing workflow with LlamaCloud SDK...", "INFO")
            if progress_callback: progress_callback(20)

            os.environ.setdefault('LLAMA_CLOUD_API_KEY', config['llama_api_key'])
            client = LlamaCloud(api_key=config['llama_api_key'])

            if progress_callback: progress_callback(40)

            existing_file_ids = self.get_existing_drive_ids(config['spreadsheet_id'], config['sheet_range'])
            existing_file_names = self.get_existing_filenames(config['spreadsheet_id'], config['sheet_range'])
            pdf_files = self.list_drive_pdfs(config['drive_folder_id'], config['days_back'])
            stats['total_pdfs'] = len(pdf_files)
            self.current_stats['pdf']['total_pdfs'] = len(pdf_files)

            skipped, files_to_process = [], []
            for f in pdf_files:
                if f['id'] in existing_file_ids:
                    self.log(f"Skipping (drive_file_id already in sheet): {f['name']}", "INFO")
                    skipped.append(f)
                elif f['name'] in existing_file_names:
                    self.log(f"Skipping (filename already in sheet): {f['name']}", "INFO")
                    skipped.append(f)
                else:
                    files_to_process.append(f)

            stats['skipped_pdfs'] = len(skipped)
            self.current_stats['pdf']['skipped_pdfs'] += len(skipped)
            files_to_process = files_to_process[:config.get('max_files', len(files_to_process))]

            if not files_to_process:
                self.log("No PDF files to process", "WARNING")
                return {'success': True, **stats}

            self.log(f"Processing {len(files_to_process)} PDFs using agent '{config['llama_agent']}'", "INFO")

            sheet_name = config['sheet_range'].split('!')[0]
            sheet_id = self.get_sheet_id(config['spreadsheet_id'], sheet_name)
            headers = self.get_sheet_headers(config['spreadsheet_id'], sheet_name)
            headers_set = bool(headers)
            incomplete_extractions = []

            for i, file in enumerate(files_to_process):
                try:
                    if status_callback: status_callback(f"Processing PDF {i+1}/{len(files_to_process)}: {file['name']}")
                    self.log(f"Processing: {file['name']}", "INFO")

                    pdf_data = self.download_from_drive(file['id'], file['name'])
                    if not pdf_data:
                        stats['failed_pdfs'] += 1
                        self.current_stats['pdf']['failed_pdfs'] += 1
                        incomplete_extractions.append({
                            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'file_name': file['name'], 'file_id': file['id'],
                            'status': 'Download Failed', 'items_extracted': 0,
                            'completeness_score': 0, 'issues': ['Failed to download from Drive'],
                            'attempts': 0, 'strategy_used': 'N/A'
                        })
                        continue

                    with tempfile.NamedTemporaryFile(suffix='.pdf', delete=False) as tmp_file:
                        tmp_file.write(pdf_data)
                        tmp_path = tmp_file.name

                    extracted_data = None
                    try:
                        extracted_data = self.safe_extract(client, tmp_path, file, config, retries=5)
                    finally:
                        if os.path.exists(tmp_path):
                            os.unlink(tmp_path)

                    if not extracted_data or not isinstance(extracted_data, dict):
                        self.log(f"No valid extracted data for {file['name']}", "ERROR")
                        stats['failed_pdfs'] += 1
                        self.current_stats['pdf']['failed_pdfs'] += 1
                        incomplete_extractions.append({
                            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'file_name': file['name'], 'file_id': file['id'],
                            'status': 'Extraction Failed', 'items_extracted': 0,
                            'completeness_score': 0, 'issues': ['All extraction attempts failed'],
                            'attempts': 5, 'strategy_used': 'standard_retry'
                        })
                        continue

                    rows_data = self.process_extracted_data(extracted_data, file)

                    if not rows_data:
                        self.log(f"No rows extracted from {file['name']}", "WARNING")
                        stats['failed_pdfs'] += 1
                        self.current_stats['pdf']['failed_pdfs'] += 1
                        incomplete_extractions.append({
                            'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                            'file_name': file['name'], 'file_id': file['id'],
                            'status': 'No Rows Extracted', 'items_extracted': 0,
                            'completeness_score': 0, 'issues': ['No rows after processing'],
                            'attempts': 5, 'strategy_used': 'standard_retry'
                        })
                        continue

                    # Merge / update headers
                    if not headers_set:
                        all_keys = set()
                        for row in rows_data:
                            all_keys.update(row.keys())
                        new_headers = sorted(list(all_keys))
                        if headers:
                            combined = list(dict.fromkeys(headers + new_headers))
                            if combined != headers:
                                self.update_headers(config['spreadsheet_id'], sheet_name, combined)
                                headers = combined
                        else:
                            self.update_headers(config['spreadsheet_id'], sheet_name, new_headers)
                            headers = new_headers
                        headers_set = True

                    sheet_rows = [[row.get(h, "") for h in headers] for row in rows_data]

                    if self.replace_rows_for_file(
                        spreadsheet_id=config['spreadsheet_id'],
                        sheet_name=sheet_name,
                        file_id=file['id'],
                        headers=headers,
                        new_rows=sheet_rows,
                        sheet_id=sheet_id
                    ):
                        stats['processed_pdfs'] += 1
                        stats['rows_added'] += len(sheet_rows)
                        self.current_stats['pdf']['processed_pdfs'] += 1
                        self.current_stats['pdf']['rows_added'] += len(sheet_rows)
                        self.log(f"Successfully processed {file['name']}: {len(sheet_rows)} rows", "SUCCESS")
                    else:
                        stats['failed_pdfs'] += 1
                        self.current_stats['pdf']['failed_pdfs'] += 1
                        self.log(f"Failed to append data for {file['name']}", "ERROR")

                    if progress_callback:
                        progress_callback(int(40 + (i + 1) / len(files_to_process) * 55))

                except Exception as e:
                    self.log(f"Failed to process PDF {file.get('name', 'unknown')}: {str(e)}", "ERROR")
                    stats['failed_pdfs'] += 1
                    self.current_stats['pdf']['failed_pdfs'] += 1
                    incomplete_extractions.append({
                        'timestamp': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                        'file_name': file.get('name', 'unknown'), 'file_id': file.get('id', ''),
                        'status': 'Processing Error', 'items_extracted': 0,
                        'completeness_score': 0, 'issues': [str(e)],
                        'attempts': 0, 'strategy_used': 'N/A'
                    })

            if incomplete_extractions:
                failed_sheet = config.get('failed_extractions_sheet', 'failed_extractions')
                self.save_failed_extractions(config['spreadsheet_id'], failed_sheet, incomplete_extractions)
                stats['incomplete'] = len(incomplete_extractions)

            if progress_callback: progress_callback(100)
            if status_callback: status_callback(f"PDF workflow completed! Processed {stats['processed_pdfs']} PDFs")
            self.log(
                f"PDF workflow completed! Processed {stats['processed_pdfs']} PDFs, "
                f"added {stats['rows_added']} rows, failed {stats['failed_pdfs']}", "SUCCESS"
            )

            return {'success': True, **stats}

        except Exception as e:
            self.log(f"PDF workflow failed: {str(e)}", "ERROR")
            return {'success': False, **stats}

    # ─────────────────────── Remaining files tracker ────────────────────────

    def save_remaining_files(self, spreadsheet_id: str, sheet_name: str, files: List[Dict]):
        try:
            self.sheets_service.spreadsheets().values().clear(
                spreadsheetId=spreadsheet_id, range=sheet_name, body={}
            ).execute()
            self.log(f"Cleared existing data in {sheet_name}", "INFO")
            headers = [['File Name', 'File ID', 'Created Time']]
            self.append_to_google_sheet(spreadsheet_id, sheet_name, headers)
            data = [[f['name'], f['id'], f.get('createdTime', '')] for f in files]
            success = self.append_to_google_sheet(spreadsheet_id, sheet_name, data)
            if success:
                self.log(f"Saved {len(files)} remaining files to {sheet_name}", "SUCCESS")
            return success
        except Exception as e:
            self.log(f"Failed to save remaining files: {str(e)}", "ERROR")
            return False

    # ──────────────────────── Google Sheets helpers ─────────────────────────

    def get_sheet_headers(self, spreadsheet_id: str, sheet_name: str) -> List[str]:
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id, range=f"{sheet_name}!A1:Z1", majorDimension="ROWS"
            ).execute()
            values = result.get('values', [])
            return values[0] if values else []
        except Exception:
            return []

    def update_headers(self, spreadsheet_id: str, sheet_name: str, new_headers: List[str]) -> bool:
        try:
            body = {'values': [new_headers]}
            last_col = self._col_to_letter(len(new_headers))
            self.sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{sheet_name}!A1:{last_col}1",
                valueInputOption='USER_ENTERED',
                body=body
            ).execute()
            self.log(f"Updated headers with {len(new_headers)} columns", "INFO")
            return True
        except Exception as e:
            self.log(f"Failed to update headers: {str(e)}", "ERROR")
            return False

    def append_to_google_sheet(self, spreadsheet_id: str, range_name: str, values: List[List[Any]]) -> bool:
        for attempt in range(1, 4):
            try:
                body = {'values': values}
                self.sheets_service.spreadsheets().values().append(
                    spreadsheetId=spreadsheet_id, range=range_name,
                    valueInputOption='USER_ENTERED', body=body
                ).execute()
                return True
            except Exception as e:
                if attempt < 3:
                    time.sleep(RETRY_WAIT)
                else:
                    self.log(f"Failed to append after 3 attempts: {str(e)}", "ERROR")
                    return False
        return False

    def get_sheet_data(self, spreadsheet_id: str, sheet_name: str) -> List[List[str]]:
        try:
            result = self.sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id, range=sheet_name, majorDimension="ROWS"
            ).execute()
            return result.get('values', [])
        except Exception as e:
            self.log(f"Failed to get sheet data: {str(e)}", "ERROR")
            return []

    def get_sheet_id(self, spreadsheet_id: str, sheet_name: str) -> int:
        try:
            metadata = self.sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            for sheet in metadata.get('sheets', []):
                if sheet['properties']['title'] == sheet_name:
                    return sheet['properties']['sheetId']
            return 0
        except Exception as e:
            self.log(f"Failed to get sheet ID: {str(e)}", "ERROR")
            return 0

    def _ensure_sheet_exists(self, spreadsheet_id: str, sheet_name: str):
        try:
            metadata = self.sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
            existing = [s['properties']['title'] for s in metadata.get('sheets', [])]
            if sheet_name not in existing:
                body = {'requests': [{'addSheet': {'properties': {'title': sheet_name}}}]}
                self.sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id, body=body
                ).execute()
                self.log(f"Created sheet tab: {sheet_name}", "INFO")
        except Exception as e:
            self.log(f"Failed to ensure sheet exists: {str(e)}", "WARNING")

    def replace_rows_for_file(self, spreadsheet_id: str, sheet_name: str, file_id: str,
                               headers: List[str], new_rows: List[List[Any]], sheet_id: int) -> bool:
        try:
            values = self.get_sheet_data(spreadsheet_id, sheet_name)
            if not values:
                return self.append_to_google_sheet(spreadsheet_id, sheet_name, new_rows)
            current_headers = values[0]
            try:
                file_col = current_headers.index('drive_file_id')
            except ValueError:
                return self.append_to_google_sheet(spreadsheet_id, sheet_name, new_rows)

            data_rows = values[1:]
            rows_to_delete = [
                idx for idx, row in enumerate(data_rows, 2)
                if len(row) > file_col and row[file_col] == file_id
            ]
            if rows_to_delete:
                rows_to_delete.sort(reverse=True)
                requests = [{
                    'deleteDimension': {
                        'range': {'sheetId': sheet_id, 'dimension': 'ROWS', 'startIndex': r - 1, 'endIndex': r}
                    }
                } for r in rows_to_delete]
                self.sheets_service.spreadsheets().batchUpdate(
                    spreadsheetId=spreadsheet_id, body={'requests': requests}
                ).execute()
                self.log(f"Deleted {len(rows_to_delete)} existing rows for file {file_id}", "INFO")
            return self.append_to_google_sheet(spreadsheet_id, sheet_name, new_rows)
        except Exception as e:
            self.log(f"Failed to replace rows: {str(e)}", "ERROR")
            return False

    def _col_to_letter(self, n: int) -> str:
        result = ""
        while n > 0:
            n, remainder = divmod(n - 1, 26)
            result = chr(65 + remainder) + result
        return result

    # ─────────────────── Workflow logging to Google Sheet ───────────────────

    def log_workflow_to_sheet(self, workflow_name: str, start_time: datetime, end_time: datetime, stats: dict):
        try:
            duration = (end_time - start_time).total_seconds()
            duration_str = f"{duration:.2f}s"
            if duration >= 60:
                duration_str = f"{int(duration // 60)}m {int(duration % 60)}s"

            log_row = [
                start_time.strftime("%Y-%m-%d %H:%M:%S"),
                end_time.strftime("%Y-%m-%d %H:%M:%S"),
                duration_str,
                workflow_name,
                stats.get('processed', stats.get('processed_pdfs', 0)),
                stats.get('total_attachments', stats.get('rows_added', 0)),
                stats.get('failed', stats.get('failed_pdfs', 0)),
                stats.get('skipped_pdfs', 0),
                stats.get('incomplete', 0),
                "Success" if stats.get('success', stats.get('processed_pdfs', 0) > 0) else "Failed"
            ]

            log_config = CONFIG['workflow_log']
            self._ensure_sheet_exists(log_config['spreadsheet_id'], log_config['sheet_range'])
            existing_headers = self.get_sheet_headers(log_config['spreadsheet_id'], log_config['sheet_range'])
            if not existing_headers:
                header_row = ["Start Time", "End Time", "Duration", "Workflow",
                              "Processed", "Total Items", "Failed", "Skipped", "Incomplete", "Status"]
                self.append_to_google_sheet(log_config['spreadsheet_id'], log_config['sheet_range'], [header_row])

            self.append_to_google_sheet(log_config['spreadsheet_id'], log_config['sheet_range'], [log_row])
            self.log(f"Logged workflow '{workflow_name}' to sheet", "INFO")

        except Exception as e:
            self.log(f"Failed to log workflow: {str(e)}", "ERROR")

    # ────────────────────────── Scheduled entry point ───────────────────────

    def run_scheduled_workflow(self):
        """Run both workflows in sequence and log results. Called by GitHub Actions."""
        try:
            self.log("=" * 80)
            self.log("STARTING MILKBASKET SCHEDULED WORKFLOW RUN")
            self.log("=" * 80)

            overall_start = datetime.now(timezone.utc)
            self.reset_stats()

            # ── Workflow 1: Gmail → Drive ──
            self.log("\n[WORKFLOW 1/2] Starting Gmail → Drive workflow...")
            mail_start = datetime.now(timezone.utc)
            gmail_result = self.process_gmail_workflow(CONFIG['gmail'])
            mail_end = datetime.now(timezone.utc)
            self.log_workflow_to_sheet("Gmail to Drive", mail_start, mail_end, gmail_result)

            time.sleep(5)

            # ── Workflow 2: Drive → Sheet ──
            self.log("\n[WORKFLOW 2/2] Starting Drive → Sheet workflow...")
            sheet_start = datetime.now(timezone.utc)
            pdf_result = self.process_pdf_workflow(CONFIG['pdf'])
            sheet_end = datetime.now(timezone.utc)
            self.log_workflow_to_sheet("Drive to Sheet", sheet_start, sheet_end, {
                'processed_pdfs': pdf_result.get('processed_pdfs', 0),
                'rows_added': pdf_result.get('rows_added', 0),
                'failed_pdfs': pdf_result.get('failed_pdfs', 0),
                'skipped_pdfs': pdf_result.get('skipped_pdfs', 0),
                'incomplete': pdf_result.get('incomplete', 0),
                'success': pdf_result.get('processed_pdfs', 0) > 0
            })

            # ── Track remaining unprocessed files ──
            drive_files = self.list_drive_pdfs(CONFIG['pdf']['drive_folder_id'], all_time=True)
            existing_ids = self.get_existing_drive_ids(CONFIG['pdf']['spreadsheet_id'], CONFIG['pdf']['sheet_range'])
            if len(drive_files) > len(existing_ids):
                remaining_ids = {f['id'] for f in drive_files} - existing_ids
                remaining_files = [f for f in drive_files if f['id'] in remaining_ids]
                self.save_remaining_files(
                    CONFIG['remaining_files']['spreadsheet_id'],
                    CONFIG['remaining_files']['sheet_range'],
                    remaining_files
                )

            overall_end = datetime.now(timezone.utc)
            total_duration = (overall_end - overall_start).total_seconds()
            duration_str = (
                f"{int(total_duration // 60)}m {int(total_duration % 60)}s"
                if total_duration >= 60 else f"{total_duration:.2f}s"
            )

            self.log("\n" + "=" * 80)
            self.log("MILKBASKET SCHEDULED WORKFLOW RUN COMPLETED")
            self.log(f"Total Duration : {duration_str}")
            self.log(f"Gmail → Drive  : {gmail_result.get('processed', 0)} attachments from "
                     f"{gmail_result.get('emails_processed', 0)} emails")
            self.log(f"Drive → Sheet  : {pdf_result.get('processed_pdfs', 0)} PDFs processed, "
                     f"{pdf_result.get('rows_added', 0)} rows added")
            self.log("=" * 80 + "\n")

            return {
                'status': 'success',
                'duration': duration_str,
                'gmail_attachments': gmail_result.get('processed', 0),
                'pdf_pdfs': pdf_result.get('processed_pdfs', 0),
                'pdf_skipped': pdf_result.get('skipped_pdfs', 0),
                'pdf_rows': pdf_result.get('rows_added', 0),
            }

        except Exception as e:
            self.log(f"Scheduled workflow failed: {str(e)}", "ERROR")
            return None


# ──────────────────────────────── Entry points ──────────────────────────────

def main():
    """Run once immediately, then keep a 3-hour schedule (for local / persistent runner use)."""
    print("=" * 80)
    print("MILKBASKET GRN AUTOMATION SCHEDULER")
    print("Runs every 3 hours: Gmail → Drive → Sheet")
    print("=" * 80)

    automation = MilkbasketAutomation()

    print("\nAuthenticating...")
    if not automation.authenticate():
        print("ERROR: Authentication failed. Please check credentials.")
        return

    print("Authentication successful!")
    print("\nRunning initial workflow...")
    summary = automation.run_scheduled_workflow()

    if summary:
        print(f"\nWorkflow Summary:")
        print(f"  Gmail → Drive  : {summary['gmail_attachments']} attachments uploaded")
        print(f"  Drive → Sheet  : {summary['pdf_pdfs']} files processed, {summary['pdf_rows']} rows added")
        print(f"  Duration       : {summary['duration']}")

    # Schedule future runs (local mode only — GitHub Actions uses cron instead)
    schedule.every(3).hours.do(automation.run_scheduled_workflow)
    print(f"\nScheduler started. Next run in 3 hours.")
    print(f"Current time: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Press Ctrl+C to stop.\n")

    try:
        while True:
            schedule.run_pending()
            time.sleep(60)
    except KeyboardInterrupt:
        print("\n\nScheduler stopped by user.")
        print("=" * 80)


if __name__ == "__main__":
    main()
