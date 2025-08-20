import threading
import time
import imaplib
import email
import email.utils
from email.header import decode_header
from datetime import datetime, timezone
import logging
from collections import defaultdict
import queue
import select
import socket

class GmailConnectionManager:
    """Manages persistent IMAP connections with IDLE support for real-time email updates"""
    
    def __init__(self):
        self.connections = {}  # account_key -> connection_info
        self.user_subscriptions = defaultdict(set)  # account_key -> set of user_ids
        self.email_cache = {}  # account_key -> list of emails
        self.update_callbacks = defaultdict(list)  # account_key -> list of callback functions
        self.lock = threading.RLock()
        
    def subscribe_user(self, user_id, account_key, account_info):
        """Subscribe a user to an account and start connection if needed"""
        with self.lock:
            self.user_subscriptions[account_key].add(user_id)
            
            if account_key not in self.connections:
                # Start new connection
                self._start_connection(account_key, account_info)
            
            # Return current emails if available
            return self.email_cache.get(account_key, [])
    
    def unsubscribe_user(self, user_id, account_key):
        """Unsubscribe a user from an account and close connection if no users left"""
        with self.lock:
            if account_key in self.user_subscriptions:
                self.user_subscriptions[account_key].discard(user_id)
                
                # If no users left, close connection
                if not self.user_subscriptions[account_key]:
                    self._close_connection(account_key)
    
    def get_emails(self, account_key):
        """Get cached emails for an account"""
        with self.lock:
            return self.email_cache.get(account_key, [])
    
    def add_update_callback(self, account_key, callback):
        """Add a callback function to be called when emails are updated"""
        with self.lock:
            self.update_callbacks[account_key].append(callback)
    
    def _start_connection(self, account_key, account_info):
        """Start a new IMAP connection with IDLE monitoring"""
        try:
            # Create IMAP connection
            mail = imaplib.IMAP4_SSL('imap.gmail.com', 993)
            mail.login(account_info['email'], account_info['app_password'])
            
            # Select INBOX for IDLE
            mail.select('INBOX')
            
            connection_info = {
                'mail': mail,
                'account_info': account_info,
                'thread': None,
                'stop_event': threading.Event(),
                'last_update': time.time()
            }
            
            self.connections[account_key] = connection_info
            
            # Fetch initial emails
            self._fetch_emails(account_key)
            
            # Start IDLE monitoring thread
            idle_thread = threading.Thread(
                target=self._idle_monitor,
                args=(account_key,),
                daemon=True
            )
            idle_thread.start()
            connection_info['thread'] = idle_thread
            
            logging.info(f"Started connection for {account_key}")
            
        except Exception as e:
            logging.error(f"Failed to start connection for {account_key}: {e}")
            if account_key in self.connections:
                del self.connections[account_key]
    
    def _close_connection(self, account_key):
        """Close an IMAP connection"""
        if account_key in self.connections:
            connection_info = self.connections[account_key]
            
            # Signal thread to stop
            connection_info['stop_event'].set()
            
            # Close IMAP connection
            try:
                connection_info['mail'].close()
                connection_info['mail'].logout()
            except:
                pass
            
            # Clean up
            del self.connections[account_key]
            if account_key in self.email_cache:
                del self.email_cache[account_key]
            if account_key in self.update_callbacks:
                del self.update_callbacks[account_key]
            
            logging.info(f"Closed connection for {account_key}")
    
    def _idle_monitor(self, account_key):
        """Monitor IMAP connection using simplified polling (more reliable than IDLE)"""
        while account_key in self.connections:
            connection_info = self.connections[account_key]
            
            if connection_info['stop_event'].is_set():
                break
            
            try:
                # Use simple polling instead of IDLE to avoid SSL issues
                # Poll every 10 seconds for new emails
                time.sleep(10)
                
                if connection_info['stop_event'].is_set():
                    break
                
                # Fetch emails periodically
                self._fetch_emails(account_key)
                
            except Exception as e:
                logging.error(f"Monitor error for {account_key}: {e}")
                # Try to reconnect
                self._reconnect(account_key)
                time.sleep(30)
    
    def _reconnect(self, account_key):
        """Reconnect to Gmail account"""
        if account_key not in self.connections:
            return
        
        connection_info = self.connections[account_key]
        account_info = connection_info['account_info']
        
        try:
            # Close old connection
            try:
                connection_info['mail'].close()
                connection_info['mail'].logout()
            except:
                pass
            
            # Create new connection
            mail = imaplib.IMAP4_SSL('imap.gmail.com', 993)
            mail.login(account_info['email'], account_info['app_password'])
            mail.select('INBOX')
            
            connection_info['mail'] = mail
            logging.info(f"Reconnected to {account_key}")
            
            # Fetch latest emails
            self._fetch_emails(account_key)
            
        except Exception as e:
            logging.error(f"Failed to reconnect to {account_key}: {e}")
    
    def _fetch_emails(self, account_key):
        """Fetch the last 50 emails from all folders"""
        if account_key not in self.connections:
            return
        
        connection_info = self.connections[account_key]
        mail = connection_info['mail']
        
        try:
            all_emails = []
            
            # Get emails from key folders (focus on Inbox and Spam)
            folders_to_check = [
                ('INBOX', 'Inbox'),
                ('[Gmail]/Spam', 'Spam')
            ]
            
            # Pre-cache Gmail categories for inbox emails (ONLY when processing INBOX)
            category_uid_cache = {}
            
            for folder_path, folder_name in folders_to_check:
                try:
                    result = mail.select(folder_path)
                    if result[0] != 'OK':
                        continue
                    
                    # Cache Gmail categories only for Inbox folder
                    if folder_name == 'Inbox':
                        categories = ['social', 'promotions', 'updates', 'forums']
                        for cat_key in categories:
                            try:
                                result, data = mail.uid('SEARCH', 'X-GM-RAW', f'"category:{cat_key}"')
                                category_uid_cache[cat_key] = set(data[0].split()) if result == 'OK' and data[0] else set()
                            except Exception as e:
                                logging.debug(f"Error caching category {cat_key}: {e}")
                                category_uid_cache[cat_key] = set()
                    
                    # Search for all emails
                    result, data = mail.uid('SEARCH', None, 'ALL')
                    if result != 'OK' or not data[0]:
                        continue
                    
                    email_uids = data[0].split()
                    if not email_uids:
                        continue
                    
                    # Get the most recent emails from this folder
                    recent_uids = email_uids[-30:]  # Get 30 from each folder
                    
                    for uid in reversed(recent_uids):
                        try:
                            # Fetch email headers
                            result, msg_data = mail.uid('fetch', uid, '(BODY.PEEK[HEADER.FIELDS (FROM SUBJECT DATE)] FLAGS)')
                            if result != 'OK' or not msg_data[0]:
                                continue
                            
                            msg = email.message_from_bytes(msg_data[0][1])
                            
                            # Parse email data
                            from_header = msg.get('From', '')
                            from_name, from_email = email.utils.parseaddr(from_header)
                            from_name = self._decode_mime_words(from_name) if from_name else from_email
                            
                            subject = self._decode_mime_words(msg.get('Subject', 'No Subject'))
                            
                            date_header = msg.get('Date', '')
                            try:
                                date_obj = email.utils.parsedate_to_datetime(date_header)
                                date_timestamp = date_obj.timestamp()
                                date_formatted = self._format_time_ago(date_obj)
                            except:
                                date_timestamp = datetime.now().timestamp()
                                date_formatted = 'Unknown'
                            
                            # Determine folder type for inbox categorization
                            detected_folder = self._get_gmail_folder_type_cached(uid, folder_name, category_uid_cache)
                            
                            email_data = {
                                'uid': uid.decode() if isinstance(uid, bytes) else uid,
                                'folder': detected_folder,
                                'from_name': from_name,
                                'from_email': from_email,
                                'subject': subject,
                                'date': date_formatted,
                                'date_timestamp': date_timestamp,
                                'folder_name': folder_name
                            }
                            
                            all_emails.append(email_data)
                            
                        except Exception as e:
                            logging.debug(f"Error processing email UID {uid}: {e}")
                            continue
                
                except Exception as e:
                    logging.debug(f"Error processing folder {folder_path}: {e}")
                    continue
            
            # Sort by date and keep only the 50 most recent
            all_emails.sort(key=lambda x: x['date_timestamp'], reverse=True)
            recent_emails = all_emails[:50]
            
            # Update cache
            with self.lock:
                self.email_cache[account_key] = recent_emails
                connection_info['last_update'] = time.time()
            
            # Call update callbacks
            for callback in self.update_callbacks[account_key]:
                try:
                    callback(account_key, recent_emails)
                except Exception as e:
                    logging.error(f"Error in update callback: {e}")
            
            logging.debug(f"Fetched {len(recent_emails)} emails for {account_key}")
            
        except Exception as e:
            logging.error(f"Error fetching emails for {account_key}: {e}")
    
    def _decode_mime_words(self, s):
        """Decode MIME encoded words"""
        if s is None:
            return ''
        
        decoded_parts = []
        for part, encoding in decode_header(s):
            if isinstance(part, bytes):
                if encoding:
                    try:
                        decoded_parts.append(part.decode(encoding))
                    except:
                        decoded_parts.append(part.decode('utf-8', errors='ignore'))
                else:
                    decoded_parts.append(part.decode('utf-8', errors='ignore'))
            else:
                decoded_parts.append(str(part))
        
        return ''.join(decoded_parts)
    
    def _format_time_ago(self, dt):
        """Convert datetime to 'X sec/min/hour/day' format"""
        now = datetime.now(timezone.utc)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        
        diff = now - dt
        seconds = int(diff.total_seconds())
        
        if seconds < 60:
            return f"{seconds} sec"
        elif seconds < 3600:
            minutes = seconds // 60
            return f"{minutes} min"
        elif seconds < 86400:
            hours = seconds // 3600
            return f"{hours} h"
        else:
            days = seconds // 86400
            return f"{days} day{'s' if days > 1 else ''}"
    
    def _get_gmail_folder_type_cached(self, uid, folder_name, category_uid_cache):
        """Determine Gmail folder type using cached category UIDs"""
        if folder_name.lower() != 'inbox':
            return folder_name
        
        # Check cached categories
        categories = [
            ('social', 'Inbox/Social'),
            ('promotions', 'Inbox/Promotions'), 
            ('updates', 'Inbox/Updates'),
            ('forums', 'Inbox/Forums')
        ]
        
        for cat_key, folder_type in categories:
            if uid in category_uid_cache.get(cat_key, set()):
                return folder_type
        
        # Default to Primary if no category found
        return 'Inbox/Primary'

# Global connection manager instance
gmail_manager = GmailConnectionManager()