"""
Logging Module for OTP Bot
Sends purchase logs, OTP receipts, and recharge approvals to Telegram channel
"""

import logging
import time
from datetime import datetime
from typing import Optional, Dict, Any
import threading

logger = logging.getLogger(__name__)

class TelegramLogger:
    """Sends logs to Telegram channel"""
    
    def __init__(self, bot_token: str, log_channel_id: str, support_link: str = "https://t.me", buy_link: str = "https://t.me/T "):
        """
        Initialize Telegram logger
        
        Args:
            bot_token: Telegram bot token
            log_channel_id: Channel ID where logs will be sent (e.g., "@your_log_channel")
            support_link: Link for support button
            buy_link: Link for buy button
        """
        self.bot_token = bot_token
        self.log_channel_id = log_channel_id
        self.support_link = support_link
        self.buy_link = buy_link
        self._bot = None
        self._init_bot()
        
    def _init_bot(self):
        """Initialize Telegram bot for logging"""
        try:
            import telebot
            from telebot.types import InlineKeyboardMarkup, InlineKeyboardButton
            self._bot = telebot.TeleBot(self.bot_token)
            self.InlineKeyboardMarkup = InlineKeyboardMarkup
            self.InlineKeyboardButton = InlineKeyboardButton
            logger.info(f"✅ Telegram logger initialized for channel: {self.log_channel_id}")
        except ImportError:
            logger.error("❌ Failed to import telebot. Install with: pip install pyTelegramBotAPI")
            self._bot = None
        except Exception as e:
            logger.error(f"❌ Failed to initialize Telegram logger: {e}")
            self._bot = None
    
    def _get_inline_buttons(self):
        """Create inline keyboard with Support and Buy buttons"""
        markup = self.InlineKeyboardMarkup(row_width=2)
        support_btn = self.InlineKeyboardButton("🆘 Support", url=self.support_link)
        buy_btn = self.InlineKeyboardButton("🛒 Buy", url=self.buy_link)
        markup.add(support_btn, buy_btn)
        return markup
    
    def _wrap_in_quote(self, message: str) -> str:
        """Wrap message in quote/code block"""
        return f"<blockquote>{message}</blockquote>"
    
    def send_log(self, message: str, parse_mode: str = "HTML") -> bool:
        """
        Send log message to Telegram channel with buttons
        
        Args:
            message: The message to send
            parse_mode: HTML or Markdown
            
        Returns:
            bool: True if successful, False otherwise
        """
        if not self._bot:
            logger.error("Telegram bot not initialized")
            return False
        
        try:
            # Wrap message in quote
            quoted_message = self._wrap_in_quote(message)
            
            # Get inline keyboard
            markup = self._get_inline_buttons()
            
            # Send message to channel with buttons
            self._bot.send_message(
                self.log_channel_id,
                quoted_message,
                parse_mode=parse_mode,
                disable_web_page_preview=True,
                reply_markup=markup
            )
            return True
        except Exception as e:
            logger.error(f"Failed to send log to Telegram: {e}")
            return False
    
    def log_purchase(self, user_id: int, country: str, price: float, phone: str) -> bool:
        """
        Log when user buys an account with exact format from image
        
        Args:
            user_id: User ID who made purchase
            country: Country of account
            price: Price paid
            phone: Phone number purchased
            
        Returns:
            bool: Success status
        """
        # Format phone number (show first 2 digits and mask rest)
        formatted_phone = ""
        if phone:
            try:
                # Remove any non-digit characters
                digits = ''.join(filter(str.isdigit, phone))
                if len(digits) >= 4:
                    # Show first 2 digits, mask middle, show last 1 digit
                    hidden = "*" * (len(digits) - 3)
                    formatted_phone = digits[:2] + hidden + digits[-1:]
                elif len(digits) > 0:
                    formatted_phone = digits[:2] + "***"
                else:
                    formatted_phone = "N/A"
            except:
                formatted_phone = phone[:2] + "****" if len(phone) > 2 else "N/A"
        
        # Format user ID (show first digits and mask rest)
        formatted_user = ""
        try:
            user_str = str(user_id)
            if len(user_str) > 4:
                formatted_user = user_str[:3] + "***" + user_str[-2:]
            else:
                formatted_user = user_str[:2] + "***"
        except:
            formatted_user = str(user_id)[:3] + "***"
        
        # Get current time
        current_time = datetime.now().strftime("%I:%M %p").lstrip("0")  # 9:54 PM format
        
        message = (
            f"<b>🔥 NEW ACCOUNT SOLD! 🔥</b>\n\n"
            f"<b>✚ Category:</b> {country} \n"
            f"<b>✚ Region:</b> {country}\n"
            f"<b>✚ Number:</b> {formatted_phone}📞\n"
            f"<b>✚ User:</b> {formatted_user}👤\n"
            f"<b>✚ Status:</b> Verified & Delivered ✅\n\n"
            f"<b>🤖 @TmOtpSell_bot</b>\n"
        )
        
        return self.send_log(message)
    
    def log_otp_received(self, user_id: int, phone: str, otp_code: str, 
                         country: str, price: float) -> bool:
        """
        Log when OTP is received for a purchase
        
        Args:
            user_id: User ID who purchased
            phone: Phone number
            otp_code: OTP code received
            country: Country of account
            price: Price paid
            
        Returns:
            bool: Success status
        """
        # Format phone number (show first 2 digits and mask rest)
        formatted_phone = ""
        if phone:
            try:
                digits = ''.join(filter(str.isdigit, phone))
                if len(digits) >= 4:
                    hidden = "*" * (len(digits) - 3)
                    formatted_phone = digits[:2] + hidden + digits[-1:]
                else:
                    formatted_phone = digits[:2] + "***"
            except:
                formatted_phone = phone[:2] + "****" if len(phone) > 2 else "N/A"
        
        # Format user ID
        formatted_user = ""
        try:
            user_str = str(user_id)
            if len(user_str) > 4:
                formatted_user = user_str[:3] + "***" + user_str[-2:]
            else:
                formatted_user = user_str[:2] + "***"
        except:
            formatted_user = str(user_id)[:3] + "***"
        
        # Get current time
        current_time = datetime.now().strftime("%I:%M %p").lstrip("0")
        
        message = (
            f"<b>🔐 OTP RECEIVED! 🔐</b>\n\n"
            f"<b>━ Category:</b> {country} \n"
            f"<b>━ Region:</b> {country}\n"
            f"<b>━ Number:</b> {formatted_phone}📞\n"
            f"<b>━ OTP:</b> <code>{otp_code}</code>💬\n"
            f"<b>━ User:</b> {formatted_user}👤\n"
            f"<b>━ Status:</b> OTP Delivered ✅\n\n"
            f"<b> 🤖 @TmOtpSell_bot</b>\n"
        )
        
        return self.send_log(message)
    
    def log_recharge_approved(self, user_id: int, amount: float, 
                             method: str = "UPI", utr: str = None) -> bool:
        """
        Log when recharge is approved
        
        Args:
            user_id: User ID who recharged
            amount: Amount recharged
            method: Payment method (UPI/Crypto)
            utr: UTR number (for UPI payments)
            
        Returns:
            bool: Success status
        """
        # Format user ID
        formatted_user = ""
        try:
            user_str = str(user_id)
            if len(user_str) > 4:
                formatted_user = user_str[:3] + "***" + user_str[-2:]
            else:
                formatted_user = user_str[:2] + "***"
        except:
            formatted_user = str(user_id)[:3] + "***"
        
        # Format UTR (mask middle digits)
        formatted_utr = ""
        if utr:
            try:
                if len(utr) >= 8:
                    formatted_utr = utr[:4] + "****" + utr[-2:]
                else:
                    formatted_utr = utr[:3] + "***"
            except:
                formatted_utr = utr
        
        # Get current time
        current_time = datetime.now().strftime("%I:%M %p").lstrip("0")
        
        utr_display = f" | UTR: {formatted_utr}" if utr else ""
        
        message = (
            f"<b>💰 RECHARGE APPROVED! 💰</b>\n\n"
            f"<b>User:</b> {formatted_user}\n"
            f"<b>Amount:</b> ₹{amount:,.0f}\n"
            f"<b>Method:</b> {method}{utr_display}\n"
            f"<b>Status:</b> Balance Updated ✅\n\n"
        )
        
        return self.send_log(message)
    
    def log_custom(self, title: str, **kwargs) -> bool:
        """
        Send custom log with key-value pairs
        
        Args:
            title: Log title
            **kwargs: Key-value pairs to display
            
        Returns:
            bool: Success status
        """
        # Format user ID if present
        formatted_data = []
        for key, value in kwargs.items():
            if key.lower() == "user_id" and value:
                try:
                    user_str = str(value)
                    if len(user_str) > 4:
                        value = user_str[:3] + "***" + user_str[-2:]
                    else:
                        value = user_str[:2] + "***"
                except:
                    pass
            elif key.lower() == "phone" and value:
                try:
                    digits = ''.join(filter(str.isdigit, str(value)))
                    if len(digits) >= 10:
                        value = digits[:3] + "****" + digits[-2:]
                    else:
                        value = digits[:3] + "****"
                except:
                    pass
            
            formatted_data.append(f"<b>{key}:</b> {value}")
        
        # Get current time
        current_time = datetime.now().strftime("%I:%M %p").lstrip("0")
        
        message = (
            f"<b>{title}</b>\n\n"
            f"{chr(10).join(formatted_data)}\n\n"
            f"<b>Always use</b>\n"
            f"{current_time}"
        )
        
        return self.send_log(message)

# Create global instance
telegram_logger = None

def init_logger(bot_token: str, log_channel_id: str, support_link: str = "https://t.me/rchiex", buy_link: str = "https://t.me/rchiex"):
    """Initialize the global telegram logger"""
    global telegram_logger
    telegram_logger = TelegramLogger(bot_token, log_channel_id, support_link, buy_link)
    return telegram_logger

def get_logger() -> TelegramLogger:
    """Get the global telegram logger instance"""
    global telegram_logger
    if telegram_logger is None:
        raise ValueError("Telegram logger not initialized. Call init_logger() first.")
    return telegram_logger

# Helper functions for common logging scenarios
def log_purchase_async(user_id: int, country: str, price: float, phone: str):
    """Log purchase in background thread"""
    def _log():
        try:
            logger = get_logger()
            logger.log_purchase(user_id, country, price, phone)
        except Exception as e:
            logging.error(f"Failed to log purchase: {e}")
    
    thread = threading.Thread(target=_log, daemon=True)
    thread.start()

def log_otp_received_async(user_id: int, phone: str, otp_code: str, country: str, price: float):
    """Log OTP receipt in background thread"""
    def _log():
        try:
            logger = get_logger()
            logger.log_otp_received(user_id, phone, otp_code, country, price)
        except Exception as e:
            logging.error(f"Failed to log OTP: {e}")
    
    thread = threading.Thread(target=_log, daemon=True)
    thread.start()

def log_recharge_approved_async(user_id: int, amount: float, method: str = "UPI", utr: str = None):
    """Log recharge approval in background thread"""
    def _log():
        try:
            logger = get_logger()
            logger.log_recharge_approved(user_id, amount, method, utr)
        except Exception as e:
            logging.error(f"Failed to log recharge: {e}")
    
    thread = threading.Thread(target=_log, daemon=True)
    thread.start()

def log_custom_async(title: str, **kwargs):
    """Send custom log in background thread"""
    def _log():
        try:
            logger = get_logger()
            logger.log_custom(title, **kwargs)
        except Exception as e:
            logging.error(f"Failed to send custom log: {e}")
    
    thread = threading.Thread(target=_log, daemon=True)
    thread.start()

# Export everything
__all__ = [
    'TelegramLogger',
    'init_logger',
    'get_logger',
    'log_purchase_async',
    'log_otp_received_async',
    'log_recharge_approved_async',
    'log_custom_async',
    'telegram_logger'
]
