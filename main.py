#!/usr/bin/env python3
"""
Discord Media Downloader CLI Tool

A command-line tool to download media files from Discord servers.
Uses requests + threading for reliable parallel downloads.

Requirements:
    pip install requests tqdm

Usage:
    python discord_downloader.py

Environment Variables:
    DISCORD_TOKEN - Your Discord authentication token (optional)

Author: Claude AI Assistant
Version: 3.0.0 - Requests + Threading
"""

import os
import sys
import time
import requests
import re
import threading
from urllib.parse import urlparse
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple
import json
import logging
from queue import Queue, Empty
from concurrent.futures import ThreadPoolExecutor, as_completed
from tqdm import tqdm
import traceback
import hashlib
from dataclasses import dataclass, asdict
import pickle


# Setup logging
def setup_logging():
    """Setup comprehensive logging"""
    log_format = '%(asctime)s - %(levelname)s - %(message)s'
    logging.basicConfig(
        level=logging.INFO,
        format=log_format,
        handlers=[
            logging.FileHandler('discord_downloader.log'),
            logging.StreamHandler(sys.stdout)
        ]
    )
    return logging.getLogger(__name__)


@dataclass
class DownloadCheckpoint:
    """Checkpoint data for resuming downloads"""
    channel_id: str
    channel_name: str
    guild_id: str
    guild_name: str
    last_processed_message_id: Optional[str]
    total_found: int
    downloaded: int
    skipped: int
    errors: int
    processed_message_ids: List[str]
    download_path: str
    timestamp: str


class CacheManager:
    """Manages caching for tokens, checkpoints, and other data"""

    def __init__(self, cache_dir: str = ".discord_downloader_cache"):
        self.cache_dir = Path(cache_dir)
        self.cache_dir.mkdir(exist_ok=True)

        self.token_file = self.cache_dir / "token.cache"
        self.guilds_file = self.cache_dir / "guilds.cache"
        self.channels_file = self.cache_dir / "channels.cache"
        self.checkpoints_dir = self.cache_dir / "checkpoints"
        self.checkpoints_dir.mkdir(exist_ok=True)

    def save_token(self, token: str) -> None:
        """Save Discord token to cache"""
        try:
            # Simple encryption using base64 (not secure, just obfuscation)
            import base64
            encoded = base64.b64encode(token.encode()).decode()
            with open(self.token_file, 'w') as f:
                f.write(encoded)
        except Exception as e:
            logging.error(f"Failed to save token: {e}")

    def load_token(self) -> Optional[str]:
        """Load Discord token from cache"""
        try:
            if self.token_file.exists():
                import base64
                with open(self.token_file, 'r') as f:
                    encoded = f.read().strip()
                return base64.b64decode(encoded.encode()).decode()
        except Exception as e:
            logging.error(f"Failed to load token: {e}")
        return None

    def save_guilds(self, guilds: List[Dict]) -> None:
        """Cache guilds data"""
        try:
            cache_data = {
                'timestamp': time.time(),
                'guilds': guilds
            }
            with open(self.guilds_file, 'w') as f:
                json.dump(cache_data, f)
        except Exception as e:
            logging.error(f"Failed to save guilds cache: {e}")

    def load_guilds(self, max_age: int = 3600) -> Optional[List[Dict]]:
        """Load cached guilds data if not too old"""
        try:
            if self.guilds_file.exists():
                with open(self.guilds_file, 'r') as f:
                    cache_data = json.load(f)

                age = time.time() - cache_data.get('timestamp', 0)
                if age < max_age:
                    return cache_data.get('guilds')
        except Exception as e:
            logging.error(f"Failed to load guilds cache: {e}")
        return None

    def save_channels(self, guild_id: str, channels: List[Dict]) -> None:
        """Cache channels data for a guild"""
        try:
            cache_data = {
                'timestamp': time.time(),
                'channels': channels
            }
            channels_file = self.cache_dir / f"channels_{guild_id}.cache"
            with open(channels_file, 'w') as f:
                json.dump(cache_data, f)
        except Exception as e:
            logging.error(f"Failed to save channels cache: {e}")

    def load_channels(self, guild_id: str, max_age: int = 3600) -> Optional[List[Dict]]:
        """Load cached channels data for a guild"""
        try:
            channels_file = self.cache_dir / f"channels_{guild_id}.cache"
            if channels_file.exists():
                with open(channels_file, 'r') as f:
                    cache_data = json.load(f)

                age = time.time() - cache_data.get('timestamp', 0)
                if age < max_age:
                    return cache_data.get('channels')
        except Exception as e:
            logging.error(f"Failed to load channels cache: {e}")
        return None

    def save_checkpoint(self, checkpoint: DownloadCheckpoint) -> None:
        """Save download checkpoint"""
        try:
            checkpoint_id = f"{checkpoint.guild_id}_{checkpoint.channel_id}"
            checkpoint_file = self.checkpoints_dir / \
                f"{checkpoint_id}.checkpoint"

            with open(checkpoint_file, 'wb') as f:
                pickle.dump(asdict(checkpoint), f)

            logging.info(f"Saved checkpoint for {checkpoint.channel_name}")
        except Exception as e:
            logging.error(f"Failed to save checkpoint: {e}")

    def load_checkpoint(self, guild_id: str, channel_id: str) -> Optional[DownloadCheckpoint]:
        """Load download checkpoint"""
        try:
            checkpoint_id = f"{guild_id}_{channel_id}"
            checkpoint_file = self.checkpoints_dir / \
                f"{checkpoint_id}.checkpoint"

            if checkpoint_file.exists():
                with open(checkpoint_file, 'rb') as f:
                    data = pickle.load(f)
                return DownloadCheckpoint(**data)
        except Exception as e:
            logging.error(f"Failed to load checkpoint: {e}")
        return None

    def delete_checkpoint(self, guild_id: str, channel_id: str) -> None:
        """Delete checkpoint after successful completion"""
        try:
            checkpoint_id = f"{guild_id}_{channel_id}"
            checkpoint_file = self.checkpoints_dir / \
                f"{checkpoint_id}.checkpoint"

            if checkpoint_file.exists():
                checkpoint_file.unlink()
                logging.info(f"Deleted checkpoint for {checkpoint_id}")
        except Exception as e:
            logging.error(f"Failed to delete checkpoint: {e}")

    def list_checkpoints(self) -> List[DownloadCheckpoint]:
        """List all available checkpoints"""
        checkpoints = []
        try:
            for checkpoint_file in self.checkpoints_dir.glob("*.checkpoint"):
                try:
                    with open(checkpoint_file, 'rb') as f:
                        data = pickle.load(f)
                    checkpoints.append(DownloadCheckpoint(**data))
                except Exception as e:
                    logging.warning(
                        f"Failed to load checkpoint {checkpoint_file}: {e}")
        except Exception as e:
            logging.error(f"Failed to list checkpoints: {e}")
        return checkpoints

    def clear_cache(self) -> None:
        """Clear all cached data"""
        try:
            import shutil
            if self.cache_dir.exists():
                shutil.rmtree(self.cache_dir)
                self.cache_dir.mkdir(exist_ok=True)
                self.checkpoints_dir.mkdir(exist_ok=True)
            print("✅ Cache cleared successfully")
        except Exception as e:
            print(f"❌ Failed to clear cache: {e}")


class DiscordDownloader:
    """Discord Media Downloader with requests + threading"""

    def __init__(self, logger, cache_manager: CacheManager):
        self.logger = logger
        self.cache_manager = cache_manager
        self.session = requests.Session()
        self.token = None
        self.user_id = None
        self.base_url = "https://discord.com/api/v10"
        self.download_stats = {"total": 0,
                               "downloaded": 0, "skipped": 0, "errors": 0}
        self.stats_lock = threading.Lock()

        # Setup session with proper headers and connection pooling
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'application/json',
            'Accept-Language': 'en-US,en;q=0.5',
            'Accept-Encoding': 'gzip, deflate',
            'DNT': '1',
            'Connection': 'keep-alive',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin'
        })

        # Configure session for better connection handling
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=20,
            pool_maxsize=20,
            max_retries=3,
            pool_block=False
        )
        self.session.mount('http://', adapter)
        self.session.mount('https://', adapter)

        self.logger.info("Discord downloader initialized")

    def setup_auth(self, token: str, save_token: bool = True) -> bool:
        """Setup authentication with Discord token"""
        self.logger.info("Setting up Discord authentication...")
        self.token = token
        self.session.headers.update({
            'Authorization': token
        })

        try:
            self.logger.info("Testing authentication...")
            response = self._make_request('GET', '/users/@me')

            if response and response.status_code == 200:
                user_data = response.json()
                self.user_id = user_data['id']
                username = user_data.get('username', 'Unknown')
                discriminator = user_data.get('discriminator', '0000')

                print(
                    f"✅ Successfully authenticated as: {username}#{discriminator}")
                self.logger.info(
                    f"Authentication successful for user: {username}#{discriminator}")

                # Save token to cache if requested
                if save_token:
                    self.cache_manager.save_token(token)
                    self.logger.info("Token saved to cache")

                return True
            else:
                print("❌ Authentication failed. Please check your token.")
                self.logger.error(
                    f"Authentication failed. Status: {response.status_code if response else 'No response'}")
                return False

        except Exception as e:
            print(f"❌ Authentication error: {str(e)}")
            self.logger.error(f"Authentication error: {str(e)}", exc_info=True)
            return False

    def _checkpoint_saver_worker(self, checkpoint: DownloadCheckpoint, stop_event: threading.Event):
        """Periodically save checkpoint for resumable downloads."""
        while not stop_event.is_set():
            try:
                # Update checkpoint with current stats
                with self.stats_lock:
                    checkpoint.total_found = self.download_stats["total"]
                    checkpoint.downloaded = self.download_stats["downloaded"]
                    checkpoint.skipped = self.download_stats["skipped"]
                    checkpoint.errors = self.download_stats["errors"]
                    checkpoint.timestamp = datetime.now().isoformat()

                # Save checkpoint
                self.cache_manager.save_checkpoint(checkpoint)
                self.logger.debug(
                    f"Checkpoint saved for {checkpoint.channel_name}")

            except Exception as e:
                self.logger.error(
                    f"Failed to save checkpoint: {e}", exc_info=True)

            stop_event.wait(30)  # wait 30 seconds between saves

    def _make_request(self, method: str, endpoint: str, **kwargs) -> Optional[requests.Response]:
        """Make a rate-limited request to Discord API"""
        url = f"{self.base_url}{endpoint}"
        max_retries = 5
        base_delay = 1

        self.logger.debug(f"Making {method} request to {endpoint}")

        for attempt in range(max_retries):
            try:
                response = self.session.request(
                    method, url, timeout=(10, 30), **kwargs)

                # Handle rate limiting
                if response.status_code == 429:
                    retry_after = float(response.headers.get('Retry-After', 1))
                    print(f"⏳ Rate limited. Waiting {retry_after} seconds...")
                    self.logger.warning(
                        f"Rate limited. Waiting {retry_after} seconds...")
                    time.sleep(retry_after)
                    continue

                # Handle other errors
                if response.status_code >= 400:
                    error_msg = f"HTTP {response.status_code}: {response.text[:200]}"
                    self.logger.warning(f"Request failed: {error_msg}")

                    if response.status_code == 403:
                        self.logger.error(
                            "403 Forbidden - Check token permissions")
                        return response  # Return to handle in calling function

                    if attempt == max_retries - 1:
                        self.logger.error(
                            f"Request failed after {max_retries} attempts")
                        return response

                    delay = base_delay * (2 ** attempt)
                    print(
                        f"🔄 Retrying in {delay} seconds... (Attempt {attempt + 2}/{max_retries})")
                    time.sleep(delay)
                    continue

                self.logger.debug(
                    f"Request successful: {response.status_code}")
                return response

            except (requests.exceptions.ConnectionError, requests.exceptions.Timeout) as e:
                error_msg = f"{type(e).__name__}: {str(e)}"
                self.logger.warning(f"Connection error: {error_msg}")

                if attempt == max_retries - 1:
                    print(f"❌ Connection failed after {max_retries} attempts")
                    self.logger.error(
                        f"Connection failed after {max_retries} attempts")
                    return None

                delay = base_delay * (2 ** attempt)
                print(
                    f"🔄 Connection issue. Retrying in {delay} seconds... (Attempt {attempt + 2}/{max_retries})")
                time.sleep(delay)
                continue

            except Exception as e:
                self.logger.error(
                    f"Unexpected error: {type(e).__name__}: {str(e)}", exc_info=True)
                if attempt == max_retries - 1:
                    return None
                time.sleep(base_delay * (2 ** attempt))

        return None

    def get_user_guilds(self, use_cache: bool = True) -> List[Dict]:
        """Fetch all guilds (servers) the user is in"""
        # Try cache first
        if use_cache:
            cached_guilds = self.cache_manager.load_guilds()
            if cached_guilds:
                print(
                    f"🔄 Using cached server list ({len(cached_guilds)} servers)")
                self.logger.info(
                    f"Using cached guilds: {len(cached_guilds)} servers")
                return cached_guilds

        print("🔍 Fetching your Discord servers...")
        self.logger.info("Fetching user guilds...")

        try:
            response = self._make_request('GET', '/users/@me/guilds')

            if response and response.status_code == 200:
                guilds = response.json()
                print(f"✅ Found {len(guilds)} servers")
                self.logger.info(f"Successfully fetched {len(guilds)} guilds")

                # Cache the results
                self.cache_manager.save_guilds(guilds)

                return guilds
            elif response and response.status_code == 403:
                print("❌ Permission denied. Token may not have required permissions.")
                self.logger.error("403 Forbidden when fetching guilds")
                return []
            else:
                print("❌ Failed to fetch servers")
                self.logger.error(
                    f"Failed to fetch guilds. Status: {response.status_code if response else 'No response'}")
                return []

        except Exception as e:
            print(f"❌ Error fetching servers: {str(e)}")
            self.logger.error(
                f"Error fetching guilds: {str(e)}", exc_info=True)
            return []

    def get_guild_channels(self, guild_id: str, use_cache: bool = True) -> List[Dict]:
        """Fetch all text channels in a guild"""
        # Try cache first
        if use_cache:
            cached_channels = self.cache_manager.load_channels(guild_id)
            if cached_channels:
                print(
                    f"🔄 Using cached channel list ({len(cached_channels)} channels)")
                self.logger.info(
                    f"Using cached channels for guild {guild_id}: {len(cached_channels)} channels")
                return cached_channels

        print(f"🔍 Fetching channels for guild {guild_id}...")
        self.logger.info(f"Fetching channels for guild {guild_id}")

        try:
            response = self._make_request(
                'GET', f'/guilds/{guild_id}/channels')

            if response and response.status_code == 200:
                all_channels = response.json()
                # Filter for text channels (type 0) and news channels (type 5)
                text_channels = [ch for ch in all_channels if ch.get('type') in [
                    0, 5]]
                print(f"✅ Found {len(text_channels)} text channels")
                self.logger.info(
                    f"Successfully fetched {len(text_channels)} text channels")

                # Cache the results
                self.cache_manager.save_channels(guild_id, text_channels)

                return text_channels
            elif response and response.status_code == 403:
                print("❌ Permission denied for this server.")
                self.logger.error(
                    f"403 Forbidden when fetching channels for guild {guild_id}")
                return []
            else:
                print("❌ Failed to fetch channels")
                self.logger.error(
                    f"Failed to fetch channels. Status: {response.status_code if response else 'No response'}")
                return []

        except Exception as e:
            print(f"❌ Error fetching channels: {str(e)}")
            self.logger.error(
                f"Error fetching channels: {str(e)}", exc_info=True)
            return []

    def scan_and_download_channel(self, channel_id: str, channel_name: str, guild_id: str, guild_name: str, download_path: Path, max_workers: int = 5):
        """Scan channel and download media with threading and checkpointing"""
        download_path.mkdir(parents=True, exist_ok=True)

        # Check for existing checkpoint
        checkpoint = self.cache_manager.load_checkpoint(guild_id, channel_id)

        if checkpoint:
            print(f"📁 Found existing checkpoint for #{channel_name}")
            print(
                f"   Last processed: {checkpoint.total_found} files found, {checkpoint.downloaded} downloaded")
            print(f"   Checkpoint from: {checkpoint.timestamp}")

            resume = input("Resume from checkpoint? (y/n): ").strip().lower()
            if resume != 'y':
                # Delete checkpoint if not resuming
                self.cache_manager.delete_checkpoint(guild_id, channel_id)
                checkpoint = None

        print(
            f"🚀 Starting {'resumed ' if checkpoint else ''}parallel scan and download for #{channel_name}")
        print(f"📁 Download path: {download_path}")
        print("=" * 70)

        self.logger.info(
            f"Starting {'resumed ' if checkpoint else ''}download for channel {channel_name} ({channel_id})")

        # Initialize or restore stats
        if checkpoint:
            with self.stats_lock:
                self.download_stats = {
                    "total": checkpoint.total_found,
                    "downloaded": checkpoint.downloaded,
                    "skipped": checkpoint.skipped,
                    "errors": checkpoint.errors
                }
        else:
            with self.stats_lock:
                self.download_stats = {
                    "total": 0, "downloaded": 0, "skipped": 0, "errors": 0}

        # Create download queue and progress tracking
        download_queue = Queue()
        scanning_complete = threading.Event()

        # Create checkpoint object
        current_checkpoint = DownloadCheckpoint(
            channel_id=channel_id,
            channel_name=channel_name,
            guild_id=guild_id,
            guild_name=guild_name,
            last_processed_message_id=checkpoint.last_processed_message_id if checkpoint else None,
            total_found=self.download_stats["total"],
            downloaded=self.download_stats["downloaded"],
            skipped=self.download_stats["skipped"],
            errors=self.download_stats["errors"],
            processed_message_ids=checkpoint.processed_message_ids if checkpoint else [],
            download_path=str(download_path),
            timestamp=datetime.now().isoformat()
        )

        # Start scanning thread
        scanner_thread = threading.Thread(
            target=self._scan_messages_worker,
            args=(channel_id, channel_name, download_queue, scanning_complete)
        )
        scanner_thread.start()

        # Start download workers
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            download_futures = []

            for i in range(max_workers):
                future = executor.submit(
                    self._download_worker,
                    download_queue, download_path, scanning_complete
                )
                download_futures.append(future)

            # Start checkpoint saving thread
            checkpoint_thread = threading.Thread(
                target=self._checkpoint_saver_worker,
                args=(current_checkpoint, scanning_complete)
            )
            checkpoint_thread.start()

            try:
                # Wait for scanner to complete
                scanner_thread.join()

                # Signal workers to stop
                for _ in range(max_workers):
                    download_queue.put(None)

                # Wait for all downloads to complete
                for future in as_completed(download_futures):
                    try:
                        future.result()
                    except Exception as e:
                        self.logger.error(
                            f"Download worker error: {str(e)}", exc_info=True)

                # Stop checkpoint thread
                checkpoint_thread.join(timeout=5)

                # Print final statistics
                self._print_final_stats()
                self.logger.info(
                    f"Completed download for channel {channel_name}")

                # Delete checkpoint on successful completion
                self.cache_manager.delete_checkpoint(guild_id, channel_id)

            except KeyboardInterrupt:
                print("\n⏹️  Download interrupted! Saving checkpoint...")
                self.logger.info(
                    "Download interrupted by user, saving checkpoint")

                # Update final checkpoint stats
                with self.stats_lock:
                    current_checkpoint.total_found = self.download_stats["total"]
                    current_checkpoint.downloaded = self.download_stats["downloaded"]
                    current_checkpoint.skipped = self.download_stats["skipped"]
                    current_checkpoint.errors = self.download_stats["errors"]
                    current_checkpoint.timestamp = datetime.now().isoformat()

                self.cache_manager.save_checkpoint(current_checkpoint)
                print("✅ Checkpoint saved! You can resume this download later.")
                raise

    def _scan_messages_worker(self, channel_id: str, channel_name: str, download_queue: Queue, scanning_complete: threading.Event):
        """Worker thread to scan messages and queue media"""
        self.logger.info(f"Starting message scan for channel {channel_id}")
        before = None
        page_count = 0
        total_messages = 0

        print(f"📥 Scanning messages in #{channel_name}...")

        try:
            while True:
                params = {'limit': 100}
                if before:
                    params['before'] = before

                response = self._make_request(
                    'GET', f'/channels/{channel_id}/messages', params=params)

                if not response:
                    self.logger.error(
                        f"Failed to fetch messages from channel {channel_id}")
                    break

                if response.status_code == 403:
                    print(f"❌ No permission to read #{channel_name}")
                    self.logger.error(
                        f"403 Forbidden for channel {channel_id}")
                    break

                if response.status_code != 200:
                    self.logger.error(
                        f"Error fetching messages: {response.status_code}")
                    break

                batch = response.json()
                if not batch:
                    break

                total_messages += len(batch)
                before = batch[-1]['id']
                page_count += 1

                # Process messages for media
                media_items = self._extract_media_from_messages(batch)

                if media_items:
                    print(
                        f"📄 Page {page_count}: Found {len(media_items)} media files (Total messages: {total_messages})")
                    self.logger.info(
                        f"Page {page_count}: Found {len(media_items)} media files")

                    # Queue media items
                    for media_url, media_info in media_items:
                        with self.stats_lock:
                            self.download_stats["total"] += 1
                        download_queue.put((media_url, media_info))

                # Small delay
                time.sleep(0.5)

            print(
                f"✅ Scanning complete! Total messages scanned: {total_messages}")
            self.logger.info(
                f"Scanning complete. Total messages: {total_messages}, Total media: {self.download_stats['total']}")

        except Exception as e:
            self.logger.error(
                f"Error in message scanning: {str(e)}", exc_info=True)
        finally:
            scanning_complete.set()

    def _download_worker(self, download_queue: Queue, download_path: Path, scanning_complete: threading.Event):
        """Worker thread to down_download_workeroad media files"""
        session = requests.Session()
        session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

        while True:
            try:
                # Get item from queue
                item = download_queue.get(timeout=5)

                # Check for shutdown signal
                if item is None:
                    break

                url, info = item
                self._download_single_file(session, url, info, download_path)
                download_queue.task_done()

            except Empty:
                if scanning_complete.is_set() and download_queue.empty():
                    break
                continue
            except Exception as e:
                self.logger.error(
                    f"Download worker error: {str(e)}", exc_info=True)
                with self.stats_lock:
                    self.download_stats["errors"] += 1

        session.close()

    def _download_single_file(self, session: requests.Session, url: str, info: Dict, download_path: Path):
        """Download a single media file with improved error handling"""
        original_filename = info['filename']
        sanitized_filename = self._sanitize_filename(original_filename)
        file_path = download_path / sanitized_filename

        # Handle duplicate filenames by adding counter
        counter = 1
        base_name, ext = os.path.splitext(sanitized_filename)
        while file_path.exists():
            # If file exists and has same size, consider it already downloaded
            try:
                if file_path.stat().st_size > 0:
                    with self.stats_lock:
                        self.download_stats["skipped"] += 1
                    self.logger.debug(
                        f"Skipped (already exists): {original_filename}")
                    return
            except:
                pass

            # Create new filename with counter
            new_filename = f"{base_name}_{counter}{ext}"
            file_path = download_path / new_filename
            counter += 1

            # Prevent infinite loop
            if counter > 1000:
                self.logger.error(
                    f"Too many duplicate files for {original_filename}")
                with self.stats_lock:
                    self.download_stats["errors"] += 1
                return

        try:
            # Ensure parent directory exists
            file_path.parent.mkdir(parents=True, exist_ok=True)

            response = session.get(url, stream=True, timeout=(10, 30))
            response.raise_for_status()

            # Download to temporary file first, then rename
            temp_file = file_path.with_suffix(file_path.suffix + '.tmp')

            try:
                with open(temp_file, 'wb') as f:
                    for chunk in response.iter_content(chunk_size=8192):
                        if chunk:
                            f.write(chunk)

                # Verify file was written and has content
                if temp_file.stat().st_size > 0:
                    temp_file.rename(file_path)
                    with self.stats_lock:
                        self.download_stats["downloaded"] += 1
                    self.logger.debug(
                        f"Downloaded: {original_filename} -> {sanitized_filename}")
                else:
                    temp_file.unlink()  # Remove empty temp file
                    raise Exception("Downloaded file is empty")

            except Exception as e:
                # Clean up temp file on error
                if temp_file.exists():
                    temp_file.unlink()
                raise e

        except Exception as e:
            with self.stats_lock:
                self.download_stats["errors"] += 1

            error_msg = str(e)
            if "No such file or directory" in error_msg or "cannot find the path" in error_msg.lower():
                self.logger.error(
                    f"Path too long or invalid for {original_filename}. Original length: {len(str(download_path / original_filename))}")
            else:
                self.logger.error(
                    f"Error downloading {original_filename}: {error_msg}")

    def _extract_media_from_messages(self, messages: List[Dict]) -> List[Tuple[str, Dict]]:
        """Extract media URLs from messages"""
        media_urls = []

        for message in messages:
            try:
                message_info = {
                    'id': message.get('id', 'unknown'),
                    'timestamp': message.get('timestamp', ''),
                    'author': message.get('author', {}).get('username', 'Unknown') if 'author' in message else 'Unknown'
                }

                # Check attachments
                if 'attachments' in message:
                    for attachment in message['attachments']:
                        if self._is_media_file(attachment.get('url', '')):
                            media_urls.append((attachment['url'], {
                                **message_info,
                                'filename': attachment.get('filename', 'unknown_file'),
                                'size': attachment.get('size', 0),
                                'type': 'attachment'
                            }))

                # Check embeds
                if 'embeds' in message:
                    for embed in message['embeds']:
                        # Image embeds
                        if 'image' in embed and 'url' in embed['image']:
                            url = embed['image']['url']
                            if self._is_media_file(url):
                                media_urls.append((url, {
                                    **message_info,
                                    'filename': os.path.basename(urlparse(url).path),
                                    'size': 0,
                                    'type': 'embed_image'
                                }))

                        # Video embeds
                        if 'video' in embed and 'url' in embed['video']:
                            url = embed['video']['url']
                            if self._is_media_file(url):
                                media_urls.append((url, {
                                    **message_info,
                                    'filename': os.path.basename(urlparse(url).path),
                                    'size': 0,
                                    'type': 'embed_video'
                                }))

                        # Thumbnail embeds
                        if 'thumbnail' in embed and 'url' in embed['thumbnail']:
                            url = embed['thumbnail']['url']
                            if self._is_media_file(url):
                                media_urls.append((url, {
                                    **message_info,
                                    'filename': os.path.basename(urlparse(url).path),
                                    'size': 0,
                                    'type': 'embed_thumbnail'
                                }))

            except Exception as e:
                self.logger.error(
                    f"Error extracting media from message: {str(e)}", exc_info=True)

        return media_urls

    def _is_media_file(self, url: str) -> bool:
        """Check if URL points to a media file"""
        if not url:
            return False

        media_extensions = {
            '.jpg', '.jpeg', '.png', '.gif', '.webp', '.bmp', '.svg', '.ico',
            '.mp4', '.avi', '.mov', '.wmv', '.flv', '.webm', '.mkv', '.m4v',
            '.mp3', '.wav', '.ogg', '.flac', '.aac', '.m4a', '.wma',
            '.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx',
            '.zip', '.rar', '.7z', '.tar', '.gz'
        }

        try:
            parsed_url = urlparse(url.lower())
            path = parsed_url.path
            return any(path.endswith(ext) for ext in media_extensions)
        except:
            return False

    def _extract_filename_from_url(self, url: str) -> str:
        """Extract filename from URL"""
        try:
            parsed_url = urlparse(url)
            path = parsed_url.path
            if path:
                filename = os.path.basename(path)
                if filename:
                    return self._sanitize_filename(filename)
        except:
            pass
        return f"media_{int(time.time())}"

    def _sanitize_filename(self, filename: str) -> str:
        """Sanitize filename for filesystem with improved length handling"""
        if not filename:
            return f"media_{int(time.time())}"

        # Remove invalid characters
        filename = re.sub(r'[<>:"/\\|?*]', '_', filename)
        # Remove control characters
        filename = re.sub(r'[\x00-\x1f\x7f-\x9f]', '', filename)
        # Replace multiple spaces/underscores with single ones
        filename = re.sub(r'[_\s]+', '_', filename)
        # Remove leading/trailing spaces and dots
        filename = filename.strip(' .')

        # Handle Windows reserved names
        windows_reserved = {
            'CON', 'PRN', 'AUX', 'NUL',
            'COM1', 'COM2', 'COM3', 'COM4', 'COM5', 'COM6', 'COM7', 'COM8', 'COM9',
            'LPT1', 'LPT2', 'LPT3', 'LPT4', 'LPT5', 'LPT6', 'LPT7', 'LPT8', 'LPT9'
        }

        name, ext = os.path.splitext(filename)
        if name.upper() in windows_reserved:
            name = f"file_{name}"

        # Smart length limitation (Windows max path is ~260, leave room for path)
        max_filename_length = 100  # Conservative limit

        if len(filename) > max_filename_length:
            # Keep extension, truncate name intelligently
            if ext:
                available_length = max_filename_length - \
                    len(ext) - 1  # -1 for the dot
                if available_length > 10:  # Ensure minimum reasonable name length
                    # Try to truncate at word boundaries or underscores
                    truncated = name[:available_length]
                    # Find last word/underscore boundary
                    last_boundary = max(
                        truncated.rfind('_'),
                        truncated.rfind(' '),
                        truncated.rfind('-')
                    )
                    if last_boundary > 10:  # Only use boundary if it leaves reasonable length
                        truncated = truncated[:last_boundary]

                    filename = truncated + ext
                else:
                    # Fallback to hash-based naming if extension is too long
                    import hashlib
                    hash_name = hashlib.md5(name.encode()).hexdigest()[:8]
                    filename = f"file_{hash_name}{ext}"
            else:
                # No extension, just truncate
                filename = name[:max_filename_length]

        # Final check - if still problematic, use hash-based name
        if not filename or len(filename) < 1:
            import hashlib
            hash_name = hashlib.md5(str(time.time()).encode()).hexdigest()[:8]
            filename = f"media_{hash_name}.bin"

        return filename

    def _print_final_stats(self):
        """Print final download statistics"""
        with self.stats_lock:
            stats = self.download_stats.copy()

        print("\n" + "=" * 70)
        print(f"🎉 Download Complete!")
        print(f"📊 Final Statistics:")
        print(f"   Total files found: {stats['total']}")
        print(f"   Downloaded: {stats['downloaded']}")
        print(f"   Skipped (existing): {stats['skipped']}")
        print(f"   Errors: {stats['errors']}")

        if stats['total'] > 0:
            success_rate = (
                (stats['downloaded'] + stats['skipped']) / stats['total']) * 100
            print(f"   Success rate: {success_rate:.1f}%")


def get_discord_token(cache_manager: CacheManager) -> Tuple[str, bool]:
    """Get Discord token from cache, environment variable, or user input"""
    # Try cache first
    cached_token = cache_manager.load_token()
    if cached_token:
        print("🔑 Found cached Discord token")
        use_cached = input("Use cached token? (y/n): ").strip().lower()
        if use_cached == 'y':
            return cached_token, False  # Don't save again

    # Try environment variable
    token = os.getenv('DISCORD_TOKEN')
    if token:
        print("🔑 Using token from environment variable")
        return token, True  # Save to cache

    # Ask user for token
    print("🔑 Discord Token Required")
    print("You can get your token by:")
    print("1. Open Discord in your browser")
    print("2. Press F12 to open Developer Tools")
    print("3. Go to Application/Storage tab")
    print("4. Find 'Local Storage' -> 'https://discord.com'")
    print("5. Look for 'token' key")
    print()

    while True:
        token = input("Enter your Discord token: ").strip()
        if token:
            return token, True  # Save to cache
        print("❌ Token cannot be empty!")


def select_from_list(items: List[Dict], name_key: str, title: str) -> List[Dict]:
    """Generic function to select items from a list"""
    print(f"\n{title}")
    print("=" * len(title))

    for i, item in enumerate(items, 1):
        print(f"[{i}] {item.get(name_key, 'Unknown')}")

    print(
        f"\nEnter selection(s) (1-{len(items)}, comma-separated for multiple):")

    while True:
        try:
            selection = input("Selection: ").strip()
            if not selection:
                print("❌ Please enter a selection!")
                continue

            selected_indices = []
            for part in selection.split(','):
                part = part.strip()
                if '-' in part:
                    start, end = map(int, part.split('-'))
                    selected_indices.extend(range(start, end + 1))
                else:
                    selected_indices.append(int(part))

            selected_items = []
            for idx in selected_indices:
                if 1 <= idx <= len(items):
                    selected_items.append(items[idx - 1])
                else:
                    print(f"❌ Invalid selection: {idx}")
                    break
            else:
                return selected_items

        except ValueError:
            print("❌ Invalid input format! Use numbers separated by commas (e.g., 1,2,3)")


def main():
    logger = setup_logging()
    cache_manager = CacheManager()
    downloader = DiscordDownloader(logger, cache_manager)

    print("=== Discord Media Downloader ===")

    # Load token from env var or cache
    token = os.environ.get("DISCORD_TOKEN") or cache_manager.load_token()
    if not token:
        token = input("Enter your Discord token: ").strip()

    if not downloader.setup_auth(token):
        sys.exit(1)

    # Fetch guilds
    guilds = downloader.get_user_guilds()
    if not guilds:
        print("❌ No servers found or access denied.")
        sys.exit(1)

    # Let user choose guild
    print("\nAvailable Servers:")
    for idx, g in enumerate(guilds, start=1):
        print(f"{idx}. {g.get('name')} ({g.get('id')})")

    guild_choice = input("Select server by number: ").strip()
    if not guild_choice.isdigit() or int(guild_choice) < 1 or int(guild_choice) > len(guilds):
        print("❌ Invalid choice")
        sys.exit(1)

    guild = guilds[int(guild_choice) - 1]
    guild_id = guild["id"]
    guild_name = guild["name"]

    # Fetch channels
    channels = downloader.get_guild_channels(guild_id)
    if not channels:
        print("❌ No accessible channels found.")
        sys.exit(1)

    print("\nAvailable Channels:")
    for idx, ch in enumerate(channels, start=1):
        print(f"{idx}. #{ch.get('name')} ({ch.get('id')})")

    channel_choice = input("Select channel by number: ").strip()
    if not channel_choice.isdigit() or int(channel_choice) < 1 or int(channel_choice) > len(channels):
        print("❌ Invalid choice")
        sys.exit(1)

    channel = channels[int(channel_choice) - 1]
    channel_id = channel["id"]
    channel_name = channel["name"]

    # Ask download path
    default_path = Path("downloads") / guild_name / channel_name
    path_in = input(f"Enter download path (default: {default_path}): ").strip()
    download_path = Path(path_in) if path_in else default_path

    # Ask number of workers
    workers_in = input("Number of parallel downloads (default 5): ").strip()
    max_workers = int(workers_in) if workers_in.isdigit() else 5

    try:
        downloader.scan_and_download_channel(
            channel_id=channel_id,
            channel_name=channel_name,
            guild_id=guild_id,
            guild_name=guild_name,
            download_path=download_path,
            max_workers=max_workers
        )
    except KeyboardInterrupt:
        print("\n⏹️ Stopped by user.")
    except Exception as e:
        logger.error(f"Fatal error: {e}", exc_info=True)
        print(f"❌ Fatal error: {e}")


if __name__ == "__main__":
    main()
