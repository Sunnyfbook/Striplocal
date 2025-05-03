import os
import sys
import json
import time
import logging
import asyncio
import re
import datetime
import aiohttp
import aiofiles
import m3u8
from typing import Dict, List, Optional, Any, Tuple
from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup, ReplyKeyboardMarkup, ReplyKeyboardRemove
from telegram.ext import Application, CommandHandler, CallbackQueryHandler, ContextTypes, MessageHandler, filters, ConversationHandler

# Configure logging
logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("bot.log")
    ]
)
logger = logging.getLogger(__name__)

# Default configuration
DEFAULT_CONFIG = {
    "models": [],
    "save_dir": "./recordings",
    "proxy": {
        "enable": True,
        "proxies": [],
        "current_index": 0,
        "last_update": 0,
        "update_interval": 3600,  # Update proxy list every hour
        "geonode_url": "https://proxylist.geonode.com/api/proxy-list?limit=500&page=1&sort_by=lastChecked&sort_type=desc"
    },
    "telegram": {
        "token": "",
        "authorized_users": []
    }
}

# Conversation states
(
    ADD_MODEL,
    REMOVE_MODEL,
    SET_DIR, 
    PROXY_TOGGLE,
    PROXY_URL,
    WAITING_FOR_MODEL_NAME,
    WAITING_FOR_DIRECTORY,
    WAITING_FOR_PROXY_URL,
) = range(8)

class ModelOfflineError(Exception):
    def __init__(self, model_name, *args) -> None:
        self.model_name = model_name
        super().__init__(*args)

class ProxyManager:
    def __init__(self):
        self.proxies = []
        self.current_proxy_index = 0
        self.load_proxies()
        
    def load_proxies(self):
        try:
            with open('Free_Proxy_List.json', 'r') as f:
                data = json.load(f)
                self.proxies = data.get('proxies', [])
                if not self.proxies:
                    logger.warning("No proxies found in Free_Proxy_List.json")
        except Exception as e:
            logger.error(f"Error loading proxies: {e}")
            self.proxies = []
            
    def get_next_proxy(self):
        if not self.proxies:
            return None
            
        proxy = self.proxies[self.current_proxy_index]
        self.current_proxy_index = (self.current_proxy_index + 1) % len(self.proxies)
        return proxy
        
    def get_proxy_dict(self):
        proxy = self.get_next_proxy()
        if proxy:
            return {
                'http': proxy,
                'https': proxy
            }
        return None

class RecorderTask:
    def __init__(self, username, config, proxy_manager):
        self.username = username
        self.config = config
        self.proxy_manager = proxy_manager
        self.logger = logging.getLogger(f"RecorderTask-{username}")
        self.header = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
            'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
            'Accept-Language': 'en-US,en;q=0.5',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Cache-Control': 'max-age=0'
        }
        self.max_retries = 3
        self.retry_delay = 5  # seconds
        self.proxy_retry_count = 0
        self.max_proxy_retries = len(config["proxy"]["proxies"]) if config["proxy"]["enable"] and config["proxy"]["proxies"] else 1
        self.stop_flag = False
        self.has_started = False
        self.save_dir = os.path.join(config["save_dir"], username, datetime.datetime.now().strftime("%Y-%m-%d"))
        
        # Stream data
        self.ext_x_map = None
        self.online_m3u8_uri = None
        self.current_segment_sequence = 0
        self.stream_name = None
        self.part_to_download = []
        self.part_download_finished = []
        self.data_map = {}
        self.current_save_path = None

    async def is_online(self) -> Tuple[Optional[str], Optional[str]]:
        """Check if the model is online and get stream info"""
        try:
            proxy = self.proxy_manager.get_proxy_dict()
            async with aiohttp.ClientSession(trust_env=True, headers=self.header, proxy=proxy) as session:
                async with session.get(f"https://stripchat.com/api/front/v2/models/username/{self.username}/cam") as resp:
                    if resp.status != 200:
                        logger.error(f"({self.username}) Failed to get model info, status: {resp.status}")
                        return None, None
                        
                    data = await resp.json()
                    if not data.get("isOnline"):
                        logger.info(f"({self.username}) Model is offline")
                        return None, None
                        
                    stream_name = data.get("streamName")
                    if not stream_name:
                        logger.error(f"({self.username}) No stream name found")
                        return None, None
                        
                    m3u8_uri = f"https://edge-hls.dopplertoken.com/hls/{stream_name}/master.m3u8"
                    return m3u8_uri, stream_name
        except Exception as e:
            logger.error(f"({self.username}) Error checking online status: {str(e)}")
            return None, None

    async def get_playlist(self) -> Any:
        """Get and process M3U8 playlist"""
        if not self.online_m3u8_uri:
            logger.error(f"({self.username}) No M3U8 URI available")
            return None
            
        try:
            logger.info(f"({self.username}) Attempting to get playlist from: {self.online_m3u8_uri}")
            proxy = self.proxy_manager.get_proxy_dict()
            async with aiohttp.ClientSession(trust_env=True, headers=self.header, proxy=proxy) as session:
                async with session.get(self.online_m3u8_uri) as resp:
                    if resp.status != 200:
                        logger.error(f"({self.username}) Failed to get playlist, status: {resp.status}")
                        logger.error(f"({self.username}) Response headers: {resp.headers}")
                        self.stop_flag = True
                        return None
                        
                    m3u8_content = await resp.text()
                    logger.info(f"({self.username}) Successfully retrieved playlist content")
                    m3u8_obj = m3u8.loads(m3u8_content)
                    
                    if m3u8_obj.media_sequence > self.current_segment_sequence:
                        # New segments available
                        logger.info(f"({self.username}) New segments available, current sequence: {self.current_segment_sequence}, new sequence: {m3u8_obj.media_sequence}")
                        for segment in m3u8_obj.segments:
                            if segment.uri not in self.part_to_download and segment.uri not in self.part_download_finished:
                                logger.info(f"({self.username}) Adding new segment: {segment.uri}")
                                self.part_to_download.append(segment.uri)
                            
                            # Get initialization segment if not already set
                            if not self.ext_x_map and hasattr(segment, 'init_section') and segment.init_section:
                                self.ext_x_map = segment.init_section.uri
                                logger.info(f"({self.username}) Set initialization segment: {self.ext_x_map}")
                                
                        self.current_segment_sequence = m3u8_obj.media_sequence
                    else:
                        # Check if model is still online
                        logger.info(f"({self.username}) No new segments, checking if model is still online")
                        m3u8_uri, stream_name = await self.is_online()
                        if not m3u8_uri or not stream_name:
                            self.stop_flag = True
                            logger.info(f"({self.username}) Model is now offline")
                            raise ModelOfflineError(self.username, f"({self.username}) is not online")
                    
                    return m3u8_obj
        except ModelOfflineError:
            raise
        except Exception as e:
            logger.error(f"({self.username}) Error getting playlist: {str(e)}", exc_info=True)
            self.stop_flag = True
            return None

    def _get_sequence(self, part_uri: str) -> Optional[int]:
        """Extract sequence number from segment URI"""
        pattern = re.compile(r'_(\d+)_')
        match = pattern.search(part_uri)
        if match:
            return int(match.group(1))
        return None

    async def download_part_file(self, part_uri: str) -> None:
        """Download an individual segment file"""
        sequence = self._get_sequence(part_uri)
        if not sequence:
            logger.error(f"({self.username}) Can't get sequence from URI: {part_uri}")
            return
            
        try:
            proxy = self.proxy_manager.get_proxy_dict()
            async with aiohttp.ClientSession(trust_env=True, headers=self.header, proxy=proxy) as session:
                async with session.get(part_uri) as resp:
                    if resp.status == 200:
                        self.data_map[sequence] = await resp.read()
                        logger.info(f"({self.username}) Downloaded segment {sequence}")
                    else:
                        logger.error(f"({self.username}) Failed to download {part_uri}, status: {resp.status}")
        except Exception as e:
            logger.error(f"({self.username}) Error downloading segment: {str(e)}", exc_info=True)
        finally:
            # Keep only the most recent 100 records
            if len(self.part_download_finished) > 100:
                self.part_download_finished = self.part_download_finished[-100:]

    async def _downloader(self) -> None:
        """Download manager task"""
        try:
            # Ensure directory exists
            logger.info(f"({self.username}) Ensuring save directory exists: {self.save_dir}")
            os.makedirs(self.save_dir, exist_ok=True)
            
            # Download initialization file
            if self.ext_x_map:
                self.current_save_path = os.path.join(self.save_dir, self.ext_x_map.rsplit('/')[-1])
                logger.info(f"({self.username}) Initialization file path: {self.current_save_path}")
                
                if not os.path.exists(self.current_save_path):
                    logger.info(f"({self.username}) Downloading initialization file")
                    proxy = self.proxy_manager.get_proxy_dict()
                    async with aiohttp.ClientSession(trust_env=True, headers=self.header, proxy=proxy) as session:
                        async with session.get(self.ext_x_map) as resp:
                            if resp.status == 200:
                                logger.info(f"({self.username}) Successfully downloaded init file")
                                async with aiofiles.open(self.current_save_path, "wb") as f:
                                    await f.write(await resp.read())
                            else:
                                logger.error(f"({self.username}) Failed to download init file, status: {resp.status}")
                                self.stop_flag = True
                                return
        except Exception as e:
            logger.error(f"({self.username}) Error in downloader setup: {str(e)}", exc_info=True)
            self.stop_flag = True
            return
            
        # Main download loop
        logger.info(f"({self.username}) Starting main download loop")
        while not self.stop_flag:
            if not self.part_to_download:
                await asyncio.sleep(1)
                continue
                
            part_uri = self.part_to_download.pop(0)
            self.part_download_finished.append(part_uri)
            logger.info(f"({self.username}) Starting download of segment: {part_uri}")
            asyncio.create_task(self.download_part_file(part_uri))
            await asyncio.sleep(0.1)  # Prevent overwhelming the server with requests

    async def _writer(self) -> None:
        """File writer task"""
        start_sequence = self.current_segment_sequence
        
        while not self.stop_flag:
            if start_sequence in self.data_map:
                if not self.current_save_path:
                    logger.warning(f"({self.username}) Save path not set, waiting...")
                    await asyncio.sleep(5)
                    continue
                    
                try:
                    async with aiofiles.open(self.current_save_path, 'ab') as f:
                        await f.write(self.data_map[start_sequence])
                    logger.info(f"({self.username}) Wrote sequence {start_sequence} to file")
                    del self.data_map[start_sequence]
                    start_sequence += 1
                except Exception as e:
                    logger.error(f"({self.username}) Error writing to file: {str(e)}", exc_info=True)
                    start_sequence += 1  # Skip problematic sequence
            else:
                await asyncio.sleep(5)
                # If still not available after waiting, skip this sequence
                if start_sequence not in self.data_map:
                    start_sequence += 1
                    # Clean up any older sequences we may have missed
                    for key in list(self.data_map.keys()):
                        if key < start_sequence:
                            logger.info(f"({self.username}) Skipping old sequence {key}")
                            del self.data_map[key]
        
        # Write remaining data when stopping
        if self.data_map:
            logger.info(f"({self.username}) Writing remaining data to file")
            keys = sorted(self.data_map.keys())
            for key in keys:
                try:
                    async with aiofiles.open(self.current_save_path, 'ab') as f:
                        await f.write(self.data_map[key])
                    logger.info(f"({self.username}) Wrote final sequence {key}")
                except Exception as e:
                    logger.error(f"({self.username}) Error writing final data: {str(e)}", exc_info=True)

    async def start(self) -> 'RecorderTask':
        """Start the recording task"""
        logger.info(f"({self.username}) Starting recording task")
        
        try:
            # Check if model is online
            online = await self.is_online()
            if not online:
                logger.info(f"({self.username}) is not online, not starting task")
                self.stop_flag = True
                return self
                
            # Initialize stream data
            logger.info(f"({self.username}) Model is online, initializing stream data")
            m3u8_uri, stream_name = await self.is_online()
            self.online_m3u8_uri = m3u8_uri
            self.has_started = True
            
            # Get initial playlist
            try:
                logger.info(f"({self.username}) Getting initial playlist")
                playlist = await self.get_playlist()
                if not playlist:
                    logger.error(f"({self.username}) Failed to get initial playlist")
                    self.stop_flag = True
                    return self
            except Exception as e:
                logger.error(f"({self.username}) Error getting initial playlist: {str(e)}", exc_info=True)
                self.stop_flag = True
                return self
                
            # Start worker tasks
            logger.info(f"({self.username}) Starting downloader and writer tasks")
            downloader_task = asyncio.create_task(self._downloader())
            writer_task = asyncio.create_task(self._writer())
            
            # Main task loop - periodically refresh playlist
            while not self.stop_flag:
                try:
                    logger.info(f"({self.username}) Refreshing playlist")
                    playlist = await self.get_playlist()
                    if not playlist:
                        logger.error(f"({self.username}) Failed to refresh playlist")
                        self.stop_flag = True
                        break
                    await asyncio.sleep(5)  # Check for updates every 5 seconds
                except ModelOfflineError:
                    logger.info(f"({self.username}) Model went offline, stopping task")
                    self.stop_flag = True
                except Exception as e:
                    logger.error(f"({self.username}) Error in main task loop: {str(e)}", exc_info=True)
                    self.stop_flag = True
            
            # Wait for worker tasks to complete
            logger.info(f"({self.username}) Waiting for worker tasks to complete")
            await asyncio.gather(downloader_task, writer_task, return_exceptions=True)
            logger.info(f"({self.username}) Recording task completed")
            
        except Exception as e:
            logger.error(f"({self.username}) Fatal error in recording task: {str(e)}", exc_info=True)
            self.stop_flag = True
            
        return self

    def stop(self) -> None:
        """Stop the recording task"""
        logger.info(f"({self.username}) Stopping recording task")
        self.stop_flag = True


class TaskManager:
    def __init__(self, config_file: str) -> None:
        self.config_file = config_file
        self.config = self._load_config()
        self.tasks: Dict[str, RecorderTask] = {}
        self.proxy_manager = ProxyManager()
        
        # Start proxy updater task
        if self.config["proxy"]["enable"]:
            asyncio.create_task(self._proxy_updater())
        else:
            logger.info("Proxy is disabled")
    
    async def _proxy_updater(self):
        """Background task to update proxy list periodically"""
        while True:
            try:
                self.proxy_manager.load_proxies()
                self.save_config()
            except Exception as e:
                logger.error(f"Error updating proxies: {str(e)}")
            await asyncio.sleep(60)  # Check every minute if update is needed

    def get_next_proxy(self) -> Optional[str]:
        """Get next proxy from proxy manager"""
        return self.proxy_manager.get_next_proxy()

    def get_current_proxy(self) -> Optional[str]:
        """Get current proxy from proxy manager"""
        return self.proxy_manager.get_proxy_dict()

    def _load_config(self) -> dict:
        """Load configuration from file or create default"""
        try:
            with open(self.config_file, "r") as f:
                config = json.load(f)
                # Ensure all required fields exist
                for key, value in DEFAULT_CONFIG.items():
                    if key not in config:
                        config[key] = value
                return config
        except (FileNotFoundError, json.JSONDecodeError):
            # Create default config
            with open(self.config_file, "w") as f:
                json.dump(DEFAULT_CONFIG, f, indent=4)
            return DEFAULT_CONFIG.copy()
    
    def save_config(self) -> None:
        """Save current configuration to file"""
        with open(self.config_file, "w") as f:
            json.dump(self.config, indent=4, fp=f)
    
    async def add_task(self, model_name: str) -> bool:
        """Add a new recording task"""
        if model_name in self.tasks and not self.tasks[model_name].stop_flag:
            logger.info(f"Model {model_name} is already being recorded")
            return False
            
        # Create and start new task
        logger.info(f"Creating new recording task for model {model_name}")
        task = RecorderTask(model_name, self.config, self.proxy_manager)
        self.tasks[model_name] = task
        
        # Add to config if not already there
        model_exists = any(model["name"] == model_name for model in self.config["models"])
        if not model_exists:
            logger.info(f"Adding model {model_name} to config")
            self.config["models"].append({"name": model_name, "type": "stripchat"})
            self.save_config()
        
        # Start the task
        logger.info(f"Starting recording task for model {model_name}")
        asyncio.create_task(task.start())
        return True
    
    def stop_task(self, model_name: str) -> bool:
        """Stop a recording task"""
        if model_name not in self.tasks:
            return False
            
        self.tasks[model_name].stop()
        return True
    
    def remove_model(self, model_name: str) -> bool:
        """Remove a model from the configuration"""
        self.stop_task(model_name)
        
        # Remove from config
        self.config["models"] = [model for model in self.config["models"] if model["name"] != model_name]
        self.save_config()
        return True
    
    def get_active_models(self) -> List[str]:
        """Get list of active recording tasks"""
        return [name for name, task in self.tasks.items() if task.has_started and not task.stop_flag]
    
    def get_configured_models(self) -> List[str]:
        """Get list of all configured models"""
        return [model["name"] for model in self.config["models"]]
    
    async def check_and_start_configured_models(self) -> None:
        """Check and start recording for all configured models"""
        logger.info("Checking configured models")
        for model in self.config["models"]:
            model_name = model["name"]
            logger.info(f"Checking model {model_name}")
            if model_name not in self.tasks or self.tasks[model_name].stop_flag:
                logger.info(f"Starting recording for model {model_name}")
                await self.add_task(model_name)
            else:
                logger.info(f"Model {model_name} is already being recorded")


class StripchatRecorderBot:
    def __init__(self, config_file: str = "config.json") -> None:
        self.config_file = config_file
        self.task_manager = TaskManager(config_file)
        
        # Initialize bot if token is available
        token = self.task_manager.config["telegram"]["token"]
        if not token:
            logger.error("Telegram bot token not configured")
            print("Please configure your Telegram bot token in config.json")
            sys.exit(1)
            
        self.application = Application.builder().token(token).build()
        self._setup_handlers()
        
        # Background task for checking models
        self._model_checker_task = None
    
    def _setup_handlers(self) -> None:
        """Set up command and conversation handlers for the bot"""
        # Basic command handlers
        self.application.add_handler(CommandHandler("start", self.cmd_start))
        self.application.add_handler(CommandHandler("help", self.cmd_help))
        self.application.add_handler(CommandHandler("cancel", self.cancel))
        
        # Keep traditional command handlers as fallback
        self.application.add_handler(CommandHandler("add", self.cmd_add_model))
        self.application.add_handler(CommandHandler("remove", self.cmd_remove_model))
        self.application.add_handler(CommandHandler("list", self.cmd_list_models))
        self.application.add_handler(CommandHandler("status", self.cmd_status))
        self.application.add_handler(CommandHandler("setdir", self.cmd_set_directory))
        self.application.add_handler(CommandHandler("proxy", self.cmd_set_proxy))
        
        # Add model conversation handler
        add_model_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.handle_add_model_start, pattern='^add_model$')],
            states={
                WAITING_FOR_MODEL_NAME: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_add_model_input)]
            },
            fallbacks=[CommandHandler("cancel", self.cancel)]
        )
        self.application.add_handler(add_model_conv)
        
        # Set directory conversation handler
        set_dir_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.handle_set_dir_start, pattern='^set_dir$')],
            states={
                WAITING_FOR_DIRECTORY: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_set_dir_input)]
            },
            fallbacks=[CommandHandler("cancel", self.cancel)]
        )
        self.application.add_handler(set_dir_conv)
        
        # Proxy conversation handler
        proxy_conv = ConversationHandler(
            entry_points=[CallbackQueryHandler(self.handle_proxy_start, pattern='^proxy_settings$')],
            states={
                PROXY_TOGGLE: [
                    CallbackQueryHandler(self.handle_proxy_toggle_on, pattern='^proxy_on$'),
                    CallbackQueryHandler(self.handle_proxy_toggle_off, pattern='^proxy_off$')
                ],
                WAITING_FOR_PROXY_URL: [MessageHandler(filters.TEXT & ~filters.COMMAND, self.handle_proxy_url_input)]
            },
            fallbacks=[CommandHandler("cancel", self.cancel)]
        )
        self.application.add_handler(proxy_conv)
        
        # Main menu callback handler
        self.application.add_handler(CallbackQueryHandler(self.handle_main_menu, pattern='^main_menu$'))
        
        # Models callback handler
        self.application.add_handler(CallbackQueryHandler(self.handle_models_menu, pattern='^models_menu$'))
        
        # Status callback handler
        self.application.add_handler(CallbackQueryHandler(self.handle_status, pattern='^status$'))
        
        # Remove model callback handler
        self.application.add_handler(CallbackQueryHandler(self.handle_remove_model, pattern='^remove_model$'))
        self.application.add_handler(CallbackQueryHandler(self.handle_remove_model_confirm, pattern='^remove_'))
        
        # Settings callback handler
        self.application.add_handler(CallbackQueryHandler(self.handle_settings_menu, pattern='^settings$'))
        
        # Catch all other callback queries
        self.application.add_handler(CallbackQueryHandler(self.button_callback))
        
        # Handle unknown commands
        self.application.add_handler(MessageHandler(filters.COMMAND, self.unknown_command))
    
    async def _check_authorized(self, update: Update) -> bool:
        """Check if user is authorized to use the bot"""
        user_id = update.effective_user.id
        authorized_users = self.task_manager.config["telegram"]["authorized_users"]
        
        if not authorized_users:
            # If no authorized users defined, allow first user to become admin
            self.task_manager.config["telegram"]["authorized_users"] = [user_id]
            self.task_manager.save_config()
            return True
            
        if user_id not in authorized_users:
            await update.message.reply_text("You are not authorized to use this bot.")
            return False
            
        return True
    
    async def cmd_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /start command - shows main menu"""
        if not await self._check_authorized(update):
            return
        
        await self.show_main_menu(update, context)
    
    async def show_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Display the main menu with buttons"""
        keyboard = [
            [InlineKeyboardButton("📋 Models", callback_data="models_menu")],
            [InlineKeyboardButton("📊 Status", callback_data="status")],
            [InlineKeyboardButton("⚙️ Settings", callback_data="settings")],
            [InlineKeyboardButton("❓ Help", callback_data="help")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        # Check if this is called from a callback query or a command
        if update.callback_query:
            await update.callback_query.answer()
            await update.callback_query.edit_message_text(
                text="Welcome to Stripchat Recorder Bot!\n\nWhat would you like to do?",
                reply_markup=reply_markup
            )
        else:
            await update.message.reply_text(
                "Welcome to Stripchat Recorder Bot!\n\nWhat would you like to do?",
                reply_markup=reply_markup
            )
    
    async def handle_main_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle main menu button callback"""
        if not await self._check_authorized(update):
            return
        
        await self.show_main_menu(update, context)
    
    async def handle_models_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle models menu button callback"""
        if not await self._check_authorized(update):
            return
        
        await update.callback_query.answer()
        
        keyboard = [
            [InlineKeyboardButton("➕ Add Model", callback_data="add_model")],
            [InlineKeyboardButton("❌ Remove Model", callback_data="remove_model")],
            [InlineKeyboardButton("📋 List Models", callback_data="list_models")],
            [InlineKeyboardButton("🔙 Back to Main Menu", callback_data="main_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.callback_query.edit_message_text(
            text="Models Menu:\nManage models to record",
            reply_markup=reply_markup
        )
    
    async def handle_add_model_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Start the add model conversation"""
        if not await self._check_authorized(update):
            return ConversationHandler.END
        
        await update.callback_query.answer()
        await update.callback_query.edit_message_text(
            "Please enter the model name you want to add:"
        )
        
        return WAITING_FOR_MODEL_NAME
    
    async def handle_add_model_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Process the model name input"""
        model_name = update.message.text.strip().lower()
        
        result = await self.task_manager.add_task(model_name)
        
        if result:
            await update.message.reply_text(f"Added model {model_name} to recording list")
        else:
            await update.message.reply_text(f"Model {model_name} is already in the recording list")
        
        # Return to main menu
        await self.show_main_menu(update, context)
        return ConversationHandler.END
    
    async def handle_remove_model(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle remove model button callback"""
        if not await self._check_authorized(update):
            return
        
        await update.callback_query.answer()
        
        models = self.task_manager.get_configured_models()
        if not models:
            await update.callback_query.edit_message_text(
                "No models configured",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="models_menu")]])
            )
            return
        
        keyboard = []
        for model in models:
            keyboard.append([InlineKeyboardButton(model, callback_data=f"remove_{model}")])
        
        keyboard.append([InlineKeyboardButton("🔙 Back", callback_data="models_menu")])
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.callback_query.edit_message_text(
            "Select a model to remove:",
            reply_markup=reply_markup
        )
    
    async def handle_remove_model_confirm(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle model removal confirmation"""
        if not await self._check_authorized(update):
            return
        
        await update.callback_query.answer()
        
        model_name = update.callback_query.data[7:]  # Remove "remove_" prefix
        result = self.task_manager.remove_model(model_name)
        
        if result:
            await update.callback_query.edit_message_text(
                f"Removed model {model_name} from recording list",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Models", callback_data="models_menu")]])
            )
        else:
            await update.callback_query.edit_message_text(
                f"Model {model_name} not found in the recording list",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Models", callback_data="models_menu")]])
            )
    
    async def handle_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle status button callback"""
        if not await self._check_authorized(update):
            return
        
        await update.callback_query.answer()
        
        active_models = self.task_manager.get_active_models()
        configured_models = self.task_manager.get_configured_models()
        
        status = (
            f"📊 Status:\n\n"
            f"Models configured: {len(configured_models)}\n"
            f"Models recording: {len(active_models)}\n"
            f"Save directory: {self.task_manager.config['save_dir']}\n"
            f"Proxy: {'Enabled' if self.task_manager.config['proxy']['enable'] else 'Disabled'}"
        )
        
        if active_models:
            status += "\n\nCurrently recording:\n"
            for model in active_models:
                status += f"• {model}\n"
        
        await update.callback_query.edit_message_text(
            status,
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back to Main Menu", callback_data="main_menu")]])
        )
    
    async def button_callback(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle all other button callbacks"""
        if not await self._check_authorized(update):
            return
        
        query = update.callback_query
        await query.answer()
        
        # Process callback data
        data = query.data
        
        if data == "help":
            help_text = (
                "Stripchat Recorder Bot Help:\n\n"
                "• Use the Models menu to add/remove models\n"
                "• Check Status to see recording status\n"
                "• Use Settings to configure save directory and proxy\n\n"
                "You can also use these commands:\n"
                "/start - Show main menu\n"
                "/help - Show this help\n"
                "/cancel - Cancel current operation"
            )
            await query.edit_message_text(
                help_text,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="main_menu")]])
            )
        
        elif data == "list_models":
            models = self.task_manager.get_configured_models()
            active_models = self.task_manager.get_active_models()
            
            if not models:
                await query.edit_message_text(
                    "No models configured",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="models_menu")]])
                )
                return
            
            message = "Configured models:\n\n"
            for model in models:
                status = "🟢 Recording" if model in active_models else "⚪ Not recording"
                message += f"• {model} - {status}\n"
            
            await query.edit_message_text(
                message,
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="models_menu")]])
            )
    
    async def handle_settings_menu(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle settings menu button callback"""
        if not await self._check_authorized(update):
            return
        
        await update.callback_query.answer()
        
        keyboard = [
            [InlineKeyboardButton("📁 Set Save Directory", callback_data="set_dir")],
            [InlineKeyboardButton("🌐 Proxy Settings", callback_data="proxy_settings")],
            [InlineKeyboardButton("🔙 Back to Main Menu", callback_data="main_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.callback_query.edit_message_text(
            "Settings Menu:\nConfigure bot settings",
            reply_markup=reply_markup
        )
    
    async def handle_set_dir_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Start the set directory conversation"""
        if not await self._check_authorized(update):
            return ConversationHandler.END
        
        await update.callback_query.answer()
        
        current_dir = self.task_manager.config["save_dir"]
        await update.callback_query.edit_message_text(
            f"Current directory: {current_dir}\n\n"
            f"Please enter the new directory path:"
        )
        
        return WAITING_FOR_DIRECTORY
    
    async def handle_set_dir_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Process the directory input"""
        new_dir = update.message.text.strip()
        
        try:
            os.makedirs(new_dir, exist_ok=True)
            self.task_manager.config["save_dir"] = new_dir
            self.task_manager.save_config()
            await update.message.reply_text(f"Recording directory set to: {new_dir}")
        except Exception as e:
            await update.message.reply_text(f"Error setting directory: {str(e)}")
        
        # Show settings menu again
        keyboard = [
            [InlineKeyboardButton("📁 Set Save Directory", callback_data="set_dir")],
            [InlineKeyboardButton("🌐 Proxy Settings", callback_data="proxy_settings")],
            [InlineKeyboardButton("🔙 Back to Main Menu", callback_data="main_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "Settings Menu:\nConfigure bot settings",
            reply_markup=reply_markup
        )
        
        return ConversationHandler.END
    
    async def handle_proxy_start(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Start the proxy settings conversation"""
        if not await self._check_authorized(update):
            return ConversationHandler.END
        
        await update.callback_query.answer()
        
        proxy_status = "enabled" if self.task_manager.config["proxy"]["enable"] else "disabled"
        proxy_url = self.task_manager.config["proxy"]["uri"] or "not set"
        
        keyboard = [
            [InlineKeyboardButton("Enable Proxy", callback_data="proxy_on")],
            [InlineKeyboardButton("Disable Proxy", callback_data="proxy_off")],
            [InlineKeyboardButton("🔙 Back", callback_data="settings")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.callback_query.edit_message_text(
            f"Proxy is currently {proxy_status}\n"
            f"Proxy URL: {proxy_url}\n\n"
            f"What would you like to do?",
            reply_markup=reply_markup
        )
        
        return PROXY_TOGGLE
    
    async def handle_proxy_toggle_on(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle enabling proxy"""
        if not await self._check_authorized(update):
            return ConversationHandler.END
        
        await update.callback_query.answer()
        
        # Check if we already have a URL
        if self.task_manager.config["proxy"]["uri"]:
            # Enable with existing URL
            self.task_manager.config["proxy"]["enable"] = True
            os.environ["HTTP_PROXY"] = self.task_manager.config["proxy"]["uri"]
            os.environ["HTTPS_PROXY"] = self.task_manager.config["proxy"]["uri"]
            self.task_manager.save_config()
            
            await update.callback_query.edit_message_text(
                f"Proxy enabled with URL: {self.task_manager.config['proxy']['uri']}",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="settings")]])
            )
            return ConversationHandler.END
        else:
            # Need to get URL
            await update.callback_query.edit_message_text(
                "Please enter the proxy URL (e.g. http://proxy.example.com:8080):"
            )
            return WAITING_FOR_PROXY_URL
    
    async def handle_proxy_toggle_off(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Handle disabling proxy"""
        if not await self._check_authorized(update):
            return ConversationHandler.END
        
        await update.callback_query.answer()
        
        self.task_manager.config["proxy"]["enable"] = False
        os.environ.pop("HTTP_PROXY", None)
        os.environ.pop("HTTPS_PROXY", None)
        self.task_manager.save_config()
        
        await update.callback_query.edit_message_text(
            "Proxy disabled",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Back", callback_data="settings")]])
        )
        
        return ConversationHandler.END
    
    async def handle_proxy_url_input(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Process the proxy URL input"""
        proxy_url = update.message.text.strip()
        
        self.task_manager.config["proxy"]["enable"] = True
        self.task_manager.config["proxy"]["uri"] = proxy_url
        os.environ["HTTP_PROXY"] = proxy_url
        os.environ["HTTPS_PROXY"] = proxy_url
        self.task_manager.save_config()
        
        await update.message.reply_text(f"Proxy enabled with URL: {proxy_url}")
        
        # Show settings menu again
        keyboard = [
            [InlineKeyboardButton("📁 Set Save Directory", callback_data="set_dir")],
            [InlineKeyboardButton("🌐 Proxy Settings", callback_data="proxy_settings")],
            [InlineKeyboardButton("🔙 Back to Main Menu", callback_data="main_menu")]
        ]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "Settings Menu:\nConfigure bot settings",
            reply_markup=reply_markup
        )
        
        return ConversationHandler.END
    
    async def cancel(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> int:
        """Cancel the current conversation"""
        await update.message.reply_text(
            "Operation cancelled.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]])
        )
        return ConversationHandler.END
    
    async def cmd_help(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /help command"""
        if not await self._check_authorized(update):
            return
            
        help_text = (
            "📱 *Stripchat Recorder Bot* 📱\n\n"
            "Use the buttons in the main menu to navigate and control the bot.\n\n"
            "*Available commands:*\n"
            "/start - Show main menu\n"
            "/help - Show this help message\n"
            "/cancel - Cancel current operation\n"
            "/add <model> - Add a model to record\n"
            "/remove <model> - Remove a model\n"
            "/list - List all configured models\n"
            "/status - Show recording status\n"
            "/setdir <path> - Set recording directory\n"
            "/proxy <on/off> <url> - Configure proxy settings"
        )
        
        keyboard = [[InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(help_text, reply_markup=reply_markup, parse_mode="Markdown")
    
    async def cmd_add_model(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /add command (kept for compatibility)"""
        if not await self._check_authorized(update):
            return
            
        if not context.args or len(context.args) < 1:
            keyboard = [[InlineKeyboardButton("➕ Add Model", callback_data="add_model")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            await update.message.reply_text(
                "Usage: /add <model_name> or use the button below:",
                reply_markup=reply_markup
            )
            return
            
        model_name = context.args[0].lower()
        result = await self.task_manager.add_task(model_name)
        
        keyboard = [[InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if result:
            await update.message.reply_text(
                f"Added model {model_name} to recording list",
                reply_markup=reply_markup
            )
        else:
            await update.message.reply_text(
                f"Model {model_name} is already in the recording list",
                reply_markup=reply_markup
            )
    
    async def cmd_remove_model(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /remove command (kept for compatibility)"""
        if not await self._check_authorized(update):
            return
            
        if not context.args or len(context.args) < 1:
            # Show keyboard with models to remove
            models = self.task_manager.get_configured_models()
            if not models:
                await update.message.reply_text(
                    "No models configured",
                    reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]])
                )
                return
                
            keyboard = []
            for model in models:
                keyboard.append([InlineKeyboardButton(model, callback_data=f"remove_{model}")])
                
            keyboard.append([InlineKeyboardButton("🔙 Cancel", callback_data="models_menu")])
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                "Select a model to remove:",
                reply_markup=reply_markup
            )
            return
            
        model_name = context.args[0].lower()
        result = self.task_manager.remove_model(model_name)
        
        keyboard = [[InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        if result:
            await update.message.reply_text(
                f"Removed model {model_name} from recording list",
                reply_markup=reply_markup
            )
        else:
            await update.message.reply_text(
                f"Model {model_name} not found in the recording list",
                reply_markup=reply_markup
            )
    
    async def cmd_list_models(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /list command (kept for compatibility)"""
        if not await self._check_authorized(update):
            return
            
        models = self.task_manager.get_configured_models()
        active_models = self.task_manager.get_active_models()
        
        if not models:
            await update.message.reply_text(
                "No models configured",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]])
            )
            return
            
        message = "Configured models:\n\n"
        for model in models:
            status = "🟢 Recording" if model in active_models else "⚪ Not recording"
            message += f"• {model} - {status}\n"
            
        keyboard = [[InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(message, reply_markup=reply_markup)
    
    async def cmd_status(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /status command (kept for compatibility)"""
        if not await self._check_authorized(update):
            return
            
        active_models = self.task_manager.get_active_models()
        configured_models = self.task_manager.get_configured_models()
        
        status = (
            f"📊 Status:\n\n"
            f"Models configured: {len(configured_models)}\n"
            f"Models recording: {len(active_models)}\n"
            f"Save directory: {self.task_manager.config['save_dir']}\n"
            f"Proxy: {'Enabled' if self.task_manager.config['proxy']['enable'] else 'Disabled'}"
        )
        
        if active_models:
            status += "\n\nCurrently recording:\n"
            for model in active_models:
                status += f"• {model}\n"
        
        keyboard = [[InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(status, reply_markup=reply_markup)
    
    async def cmd_set_directory(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /setdir command (kept for compatibility)"""
        if not await self._check_authorized(update):
            return
            
        if not context.args or len(context.args) < 1:
            await update.message.reply_text(
                f"Current directory: {self.task_manager.config['save_dir']}\n\n"
                f"Usage: /setdir <path>",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("📁 Set Directory", callback_data="set_dir")]])
            )
            return
            
        new_dir = context.args[0]
        
        # Validate and create directory if needed
        try:
            os.makedirs(new_dir, exist_ok=True)
            self.task_manager.config["save_dir"] = new_dir
            self.task_manager.save_config()
            
            keyboard = [[InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                f"Recording directory set to: {new_dir}",
                reply_markup=reply_markup
            )
        except Exception as e:
            await update.message.reply_text(f"Error setting directory: {str(e)}")
    
    async def cmd_set_proxy(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle /proxy command (kept for compatibility)"""
        if not await self._check_authorized(update):
            return
            
        if not context.args:
            proxy_status = "enabled" if self.task_manager.config["proxy"]["enable"] else "disabled"
            proxy_url = self.task_manager.config["proxy"]["uri"] or "not set"
            
            keyboard = [[InlineKeyboardButton("🌐 Proxy Settings", callback_data="proxy_settings")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                f"Proxy is currently {proxy_status}\n"
                f"Proxy URL: {proxy_url}\n\n"
                f"Usage:\n"
                f"/proxy on <url> - Enable proxy\n"
                f"/proxy off - Disable proxy\n\n"
                f"Or use the button below:",
                reply_markup=reply_markup
            )
            return
            
        action = context.args[0].lower()
        
        if action == "on" or action == "enable":
            if len(context.args) < 2:
                await update.message.reply_text("Please provide a proxy URL")
                return
                
            proxy_url = context.args[1]
            self.task_manager.config["proxy"]["enable"] = True
            self.task_manager.config["proxy"]["uri"] = proxy_url
            os.environ["HTTP_PROXY"] = proxy_url
            os.environ["HTTPS_PROXY"] = proxy_url
            
            self.task_manager.save_config()
            
            keyboard = [[InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                f"Proxy enabled with URL: {proxy_url}",
                reply_markup=reply_markup
            )
            
        elif action == "off" or action == "disable":
            self.task_manager.config["proxy"]["enable"] = False
            os.environ.pop("HTTP_PROXY", None)
            os.environ.pop("HTTPS_PROXY", None)
            
            self.task_manager.save_config()
            
            keyboard = [[InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]]
            reply_markup = InlineKeyboardMarkup(keyboard)
            
            await update.message.reply_text(
                "Proxy disabled",
                reply_markup=reply_markup
            )
            
        else:
            await update.message.reply_text("Unknown action. Use 'on' or 'off'")
    
    async def unknown_command(self, update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
        """Handle unknown commands"""
        if not await self._check_authorized(update):
            return
            
        keyboard = [[InlineKeyboardButton("🔙 Main Menu", callback_data="main_menu")]]
        reply_markup = InlineKeyboardMarkup(keyboard)
        
        await update.message.reply_text(
            "Unknown command. Use /help to see available commands or use the buttons in the main menu.",
            reply_markup=reply_markup
        )
    
    async def model_checker(self) -> None:
        """Background task to periodically check and start configured models"""
        while True:
            try:
                await self.task_manager.check_and_start_configured_models()
            except Exception as e:
                logger.error(f"Error in model checker: {str(e)}", exc_info=True)
            
            await asyncio.sleep(60)  # Check every minute
    
    async def start(self) -> None:
        """Start the bot and background tasks"""
        # Start model checker
        self._model_checker_task = asyncio.create_task(self.model_checker())
        
        # Start the bot
        await self.application.initialize()
        await self.application.start()
        await self.application.updater.start_polling()
        
        logger.info("Bot started")
        
        try:
            # Keep the bot running
            while True:
                await asyncio.sleep(1)
        except (KeyboardInterrupt, SystemExit):
            logger.info("Bot stopping...")
        finally:
            # Clean shutdown
            await self.stop()
    
    async def stop(self) -> None:
        """Stop the bot and all tasks"""
        # Stop all recording tasks
        for model_name in list(self.task_manager.tasks.keys()):
            self.task_manager.stop_task(model_name)
        
        # Cancel model checker
        if self._model_checker_task:
            self._model_checker_task.cancel()
            
        # Stop the bot
        await self.application.stop()
        await self.application.shutdown()
        logger.info("Bot stopped")


async def main():
    """Main function"""
    bot = StripchatRecorderBot()
    await bot.start()


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        pass