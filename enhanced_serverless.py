import json
import os
import asyncio
import time
from typing import Dict, Any, Optional
import requests
import langdetect
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.services.functions import Functions
from appwrite.exception import AppwriteException

# تنظیمات Appwrite
APPWRITE_ENDPOINT = ""
APPWRITE_PROJECT_ID = ""
APPWRITE_API_KEY = ""
APPWRITE_DATABASE_ID = ""
APPWRITE_COLLECTION_ID = ""
APPWRITE_MESSAGES_COLLECTION_ID = ""
APPWRITE_FAILED_MESSAGES_COLLECTION_ID = ""

# تنظیمات Telegram و Gemini
TELEGRAM_TOKEN = ""
GEMINI_API_KEY = ""
GEMINI_API_URL = ""

# پرامپت سیستمی
SYSTEM_PROMPT = """سلام! من PyTech هستم، یک دستیار برنامه‌نویسی هوشمند که توسط تیم HiTech ساخته شده‌ام. وظایف من عبارتند از:
- کمک به نوشتن کد تمیز، کارآمد و مستندسازی شده
- رفع اشکال و پیشنهاد بهبودها
- توضیح واضح مفاهیم برنامه‌نویسی
- پیروی از بهترین شیوه‌ها و الگوهای طراحی
- ارائه توضیحات فنی دقیق در صورت نیاز
- کمک در بررسی و بهینه‌سازی کد
- دریافت و ارسال فایل‌های .py برای کمک بهتر

You must ONLY respond in the same language as the user's message:
- If user writes in Persian/Farsi, respond ONLY in Persian/Farsi
- If user writes in English, respond ONLY in English
Send code files as .py files to users.
Only introduce yourself as PyTech when specifically asked about your name or identity."""

# کلاس مدیریت اتصال بهبود یافته
class EnhancedDatabaseConnection:
    """کلاس مدیریت اتصال بهبود یافته با قابلیت‌های real-time"""
    
    _instance = None
    _connection_pool = {}
    _max_connections = 10
    _connection_timeout = 30
    _retry_attempts = 3
    _last_health_check = 0
    _health_check_interval = 300  # 5 دقیقه
    
    def __new__(cls):
        if cls._instance is None:
            cls._instance = super(EnhancedDatabaseConnection, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    def _initialize(self):
        """راه‌اندازی اولیه"""
        self._create_connection_pool()
    
    def _create_connection_pool(self):
        """ایجاد pool اتصالات"""
        try:
            for i in range(self._max_connections):
                client = Client()
                client.set_endpoint(APPWRITE_ENDPOINT)
                client.set_project(APPWRITE_PROJECT_ID)
                client.set_key(APPWRITE_API_KEY)
                
                self._connection_pool[f"conn_{i}"] = {
                    'client': client,
                    'databases': Databases(client),
                    'in_use': False,
                    'created_at': time.time(),
                    'last_used': time.time()
                }
            print(f"Connection pool ایجاد شد با {self._max_connections} اتصال")
        except Exception as e:
            print(f"خطا در ایجاد connection pool: {e}")
            raise
    
    def _get_available_connection(self):
        """دریافت اتصال آزاد از pool"""
        current_time = time.time()
        
        # جستجو برای اتصال آزاد
        for conn_id, conn_info in self._connection_pool.items():
            if not conn_info['in_use']:
                # بررسی timeout اتصال
                if current_time - conn_info['last_used'] > self._connection_timeout:
                    self._refresh_connection(conn_id)
                
                conn_info['in_use'] = True
                conn_info['last_used'] = current_time
                return conn_id, conn_info['databases']
        
        # اگر اتصال آزاد نبود، منتظر می‌مانیم
        return self._wait_for_connection()
    
    def _refresh_connection(self, conn_id):
        """تازه‌سازی اتصال منقضی شده"""
        try:
            client = Client()
            client.set_endpoint(APPWRITE_ENDPOINT)
            client.set_project(APPWRITE_PROJECT_ID)
            client.set_key(APPWRITE_API_KEY)
            
            self._connection_pool[conn_id].update({
                'client': client,
                'databases': Databases(client),
                'created_at': time.time()
            })
            print(f"اتصال {conn_id} تازه‌سازی شد")
        except Exception as e:
            print(f"خطا در تازه‌سازی اتصال {conn_id}: {e}")
    
    def _wait_for_connection(self):
        """انتظار برای آزاد شدن اتصال"""
        max_wait = 10  # حداکثر 10 ثانیه انتظار
        wait_time = 0.1
        total_wait = 0
        
        while total_wait < max_wait:
            for conn_id, conn_info in self._connection_pool.items():
                if not conn_info['in_use']:
                    conn_info['in_use'] = True
                    conn_info['last_used'] = time.time()
                    return conn_id, conn_info['databases']
            
            time.sleep(wait_time)
            total_wait += wait_time
        
        raise Exception("تمام اتصالات مشغول هستند")
    
    def _release_connection(self, conn_id):
        """آزاد کردن اتصال"""
        if conn_id in self._connection_pool:
            self._connection_pool[conn_id]['in_use'] = False
    
    def execute_with_retry(self, operation, *args, **kwargs):
        """اجرای عملیات با retry logic"""
        last_exception = None
        
        for attempt in range(self._retry_attempts):
            conn_id = None
            try:
                conn_id, databases = self._get_available_connection()
                
                # اجرای عملیات
                if operation == 'create_document':
                    result = databases.create_document(*args, **kwargs)
                elif operation == 'get_document':
                    result = databases.get_document(*args, **kwargs)
                elif operation == 'update_document':
                    result = databases.update_document(*args, **kwargs)
                elif operation == 'delete_document':
                    result = databases.delete_document(*args, **kwargs)
                elif operation == 'list_documents':
                    result = databases.list_documents(*args, **kwargs)
                else:
                    raise ValueError(f"عملیات نامشخص: {operation}")
                
                return result
                
            except AppwriteException as e:
                last_exception = e
                print(f"تلاش {attempt + 1} ناموفق: {e}")
                if attempt < self._retry_attempts - 1:
                    time.sleep(0.5 * (attempt + 1))  # Exponential backoff
            except Exception as e:
                last_exception = e
                print(f"خطای غیرمنتظره در تلاش {attempt + 1}: {e}")
                break
            finally:
                if conn_id:
                    self._release_connection(conn_id)
        
        raise last_exception
    
    def health_check(self):
        """بررسی سلامت اتصالات"""
        current_time = time.time()
        
        if current_time - self._last_health_check < self._health_check_interval:
            return True
        
        healthy_connections = 0
        for conn_id, conn_info in self._connection_pool.items():
            try:
                # تست ساده اتصال
                databases = conn_info['databases']
                databases.list_documents(
                    database_id=APPWRITE_DATABASE_ID,
                    collection_id=APPWRITE_COLLECTION_ID,
                    queries=["limit(1)"]
                )
                healthy_connections += 1
            except Exception as e:
                print(f"اتصال {conn_id} ناسالم: {e}")
                self._refresh_connection(conn_id)
        
        self._last_health_check = current_time
        health_ratio = healthy_connections / self._max_connections
        
        print(f"Health check: {healthy_connections}/{self._max_connections} اتصال سالم")
        return health_ratio > 0.5  # حداقل 50% اتصالات باید سالم باشند

# نمونه سراسری از کلاس اتصال
db_manager = EnhancedDatabaseConnection()

# توابع کمکی برای پردازش فوری
async def validate_telegram_update(req) -> Dict[str, Any]:
    """اعتبارسنجی و استخراج داده‌های تلگرام"""
    try:
        update = req.json
        if not update or 'message' not in update:
            raise ValueError("داده‌های نامعتبر از تلگرام")
        return update
    except Exception as e:
        raise ValueError(f"خطا در پردازش داده‌های تلگرام: {e}")

async def extract_message_data(update: Dict[str, Any]) -> Dict[str, Any]:
    """استخراج اطلاعات پیام"""
    message = update['message']
    return {
        'chat_id': str(message['chat']['id']),
        'message_id': message['message_id'],
        'text': message.get('text', ''),
        'document': message.get('document'),
        'caption': message.get('caption', ''),
        'user_id': message['from']['id'],
        'username': message['from'].get('username', ''),
        'timestamp': time.time()
    }

async def detect_user_language(text: str) -> str:
    """تشخیص زبان کاربر"""
    try:
        return langdetect.detect(text) if text else 'en'
    except:
        return 'en'

async def get_gemini_response_async(prompt: str, user_lang: str) -> str:
    """دریافت پاسخ از API هوش مصنوعی Gemini به صورت async"""
    headers = {'Content-Type': 'application/json'}
    
    lang_instruction = "\nYou MUST respond ONLY in Persian/Farsi." if user_lang == 'fa' else "\nYou MUST respond ONLY in English."
    modified_prompt = SYSTEM_PROMPT + lang_instruction
    
    data = {
        "contents": [{
            "parts": [
                {"text": modified_prompt},
                {"text": prompt}
            ]
        }]
    }
    
    try:
        # استفاده از requests برای درخواست HTTP
        response = requests.post(
            f"{GEMINI_API_URL}?key={GEMINI_API_KEY}",
            headers=headers,
            json=data,
            timeout=15  # timeout برای جلوگیری از انتظار طولانی
        )
        response.raise_for_status()
        result = response.json()
        
        if 'candidates' in result and len(result['candidates']) > 0:
            return result['candidates'][0]['content']['parts'][0]['text']
        return "متأسفم، نتوانستم پاسخی تولید کنم." if user_lang == 'fa' else "Sorry, I couldn't generate a response."
    except Exception as e:
        error_msg = f"خطا در دریافت پاسخ: {str(e)}" if user_lang == 'fa' else f"Error getting response: {str(e)}"
        return error_msg

async def send_telegram_message_async(chat_id: str, text: str) -> bool:
    """ارسال پیام به تلگرام به صورت async"""
    url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
    
    try:
        # تقسیم پیام‌های طولانی
        max_length = 4096
        if len(text) > max_length:
            chunks = [text[i:i + max_length] for i in range(0, len(text), max_length)]
            for chunk in chunks:
                response = requests.post(
                    url, 
                    json={"chat_id": chat_id, "text": chunk},
                    timeout=10
                )
                if not response.ok:
                    print(f"خطا در ارسال بخش پیام: {response.text}")
        else:
            response = requests.post(
                url, 
                json={"chat_id": chat_id, "text": text},
                timeout=10
            )
            if not response.ok:
                print(f"خطا در ارسال پیام: {response.text}")
                return False
        
        return True
    except Exception as e:
        print(f"خطا در ارسال پیام به تلگرام: {e}")
        return False

async def save_conversation_async(chat_id: str, message: str, response: str) -> bool:
    """ذخیره مکالمه به صورت async"""
    try:
        result = db_manager.execute_with_retry(
            'create_document',
            database_id=APPWRITE_DATABASE_ID,
            collection_id=APPWRITE_COLLECTION_ID,
            document_id='unique()',
            data={
                'user_id': chat_id,
                'message': message,
                'response': response,
                'timestamp': {'$createdAt': True}
            }
        )
        return True
    except Exception as e:
        print(f"خطا در ذخیره مکالمه: {e}")
        return False

async def save_failed_message(chat_id: str, message_data: Dict[str, Any], error: str) -> bool:
    """ذخیره پیام ناموفق برای پردازش بعدی"""
    try:
        result = db_manager.execute_with_retry(
            'create_document',
            database_id=APPWRITE_DATABASE_ID,
            collection_id=APPWRITE_FAILED_MESSAGES_COLLECTION_ID,
            document_id='unique()',
            data={
                'chat_id': chat_id,
                'message_data': json.dumps(message_data),
                'error': error,
                'retry_count': 0,
                'timestamp': {'$createdAt': True}
            }
        )
        return True
    except Exception as e:
        print(f"خطا در ذخیره پیام ناموفق: {e}")
        return False

# توابع پردازش پیام‌ها
async def handle_start_command_async(chat_id: str, text: str) -> str:
    """پردازش دستور /start"""
    user_lang = await detect_user_language(text)
    
    if user_lang == 'fa':
        message = "سلام! من PyTech هستم، دستیار برنامه‌نویسی حرفه‌ای شما که توسط تیم HiTech ساخته شده‌ام.\n" \
                  "می‌توانید فایل‌های .py خود را برای من ارسال کنید تا در بررسی و بهبود آنها به شما کمک کنم."
    else:
        message = "👋 Hello! I'm Pytech, your professional programming assistant powered by HiTech.\n" \
                  "You can send me your .py files and I'll help you review and improve them."
    
    return message

async def handle_help_command_async(chat_id: str, text: str) -> str:
    """پردازش دستور /help"""
    user_lang = await detect_user_language(text)
    
    if user_lang == 'fa':
        message = "🤖 دستیار برنامه نویسی شما برای:\n" \
                  "- نوشتن و بررسی کد\n" \
                  "- رفع اشکال\n" \
                  "- توضیح مفاهیم برنامه نویسی\n" \
                  "- پیشنهاد بهینه سازی\n" \
                  "- بهترین شیوه‌ها و الگوها\n" \
                  "- دریافت و بررسی فایل‌های .py"
    else:
        message = "🤖 I can help you with:\n" \
                  "- Writing and reviewing code\n" \
                  "- Debugging problems\n" \
                  "- Explaining programming concepts\n" \
                  "- Suggesting optimizations\n" \
                  "- Best practices and patterns\n" \
                  "- Reviewing .py files"
    
    return message

async def handle_document_async(chat_id: str, file_id: str, mime_type: str, caption: str = None) -> str:
    """پردازش فایل‌های دریافتی"""
    if mime_type == 'text/x-python':
        try:
            # دریافت فایل از تلگرام
            file_info_url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getFile?file_id={file_id}"
            file_info_response = requests.get(file_info_url, timeout=10)
            file_info = file_info_response.json()
            
            if 'result' in file_info and 'file_path' in file_info['result']:
                file_path = file_info['result']['file_path']
                file_url = f"https://api.telegram.org/file/bot{TELEGRAM_TOKEN}/{file_path}"
                file_response = requests.get(file_url, timeout=15)
                code = file_response.content.decode('utf-8')
                
                # تشخیص زبان کاربر
                user_lang = await detect_user_language(caption) if caption else 'en'
                
                # دریافت پاسخ از Gemini
                ai_response = await get_gemini_response_async(f"Review this Python code:\n{code}", user_lang)
                return ai_response
            else:
                return "خطا در دریافت فایل از تلگرام" if await detect_user_language(caption or '') == 'fa' else "Error downloading file from Telegram"
        except Exception as e:
            error_msg = f"خطا در پردازش فایل: {str(e)}" if await detect_user_language(caption or '') == 'fa' else f"Error processing file: {str(e)}"
            return error_msg
    else:
        user_lang = await detect_user_language(caption or '')
        return "لطفاً فقط فایل‌های Python (.py) ارسال کنید" if user_lang == 'fa' else "Please send only Python (.py) files"

async def handle_text_message_async(chat_id: str, text: str) -> str:
    """پردازش پیام‌های متنی"""
    user_lang = await detect_user_language(text)
    ai_response = await get_gemini_response_async(text, user_lang)
    return ai_response

async def process_message_immediately(update: Dict[str, Any]) -> Dict[str, Any]:
    """پردازش فوری پیام"""
    message_data = await extract_message_data(update)
    chat_id = message_data['chat_id']
    
    try:
        # پردازش بر اساس نوع پیام
        if message_data['text']:
            text = message_data['text']
            
            if text.startswith('/start'):
                response = await handle_start_command_async(chat_id, text)
            elif text.startswith('/help'):
                response = await handle_help_command_async(chat_id, text)
            else:
                response = await handle_text_message_async(chat_id, text)
                
        elif message_data['document']:
            document = message_data['document']
            response = await handle_document_async(
                chat_id, 
                document['file_id'], 
                document.get('mime_type', ''), 
                message_data['caption']
            )
        else:
            user_lang = await detect_user_language(message_data.get('caption', ''))
            response = "نوع پیام پشتیبانی نمی‌شود" if user_lang == 'fa' else "Unsupported message type"
        
        return {
            'chat_id': chat_id,
            'response': response,
            'message_data': message_data,
            'success': True
        }
        
    except Exception as e:
        return {
            'chat_id': chat_id,
            'response': f"خطا در پردازش: {str(e)}",
            'message_data': message_data,
            'success': False,
            'error': str(e)
        }

# تابع اصلی webhook با قابلیت real-time
async def main_webhook_realtime(req, res):
    """تابع اصلی webhook با پردازش real-time"""
    start_time = time.time()
    
    try:
        # اعتبارسنجی داده‌های ورودی
        update = await validate_telegram_update(req)
        
        # پردازش فوری پیام
        result = await process_message_immediately(update)
        
        # ارسال پاسخ به کاربر
        if result['success']:
            send_success = await send_telegram_message_async(result['chat_id'], result['response'])
            
            if send_success:
                # ذخیره مکالمه در background (non-blocking)
                asyncio.create_task(save_conversation_async(
                    result['chat_id'],
                    result['message_data'].get('text', '[File/Document]'),
                    result['response']
                ))
            else:
                # در صورت خطا در ارسال، ذخیره در failed messages
                await save_failed_message(
                    result['chat_id'],
                    result['message_data'],
                    "Failed to send response to Telegram"
                )
        else:
            # ارسال پیام خطا به کاربر
            error_response = "متأسفم، خطایی رخ داده است. لطفاً دوباره تلاش کنید."
            await send_telegram_message_async(result['chat_id'], error_response)
            
            # ذخیره در failed messages
            await save_failed_message(
                result['chat_id'],
                result['message_data'],
                result.get('error', 'Unknown error')
            )
        
        processing_time = time.time() - start_time
        print(f"پیام در {processing_time:.2f} ثانیه پردازش شد")
        
        return res.json({
            "success": True,
            "processing_time": processing_time,
            "message": "پیام با موفقیت پردازش شد"
        })
        
    except Exception as e:
        processing_time = time.time() - start_time
        print(f"خطا در پردازش webhook: {str(e)}")
        
        return res.json({
            "success": False,
            "error": str(e),
            "processing_time": processing_time
        }, 500)

# تابع بازیابی پیام‌های ناموفق
async def recovery_function(req, res):
    """پردازش پیام‌های ناموفق"""
    try:
        # دریافت پیام‌های ناموفق
        result = db_manager.execute_with_retry(
            'list_documents',
            database_id=APPWRITE_DATABASE_ID,
            collection_id=APPWRITE_FAILED_MESSAGES_COLLECTION_ID,
            queries=["retry_count<3", "orderAsc('timestamp')", "limit(10)"]
        )
        
        failed_messages = result.get('documents', [])
        processed_count = 0
        
        for message_doc in failed_messages:
            try:
                message_id = message_doc['$id']
                chat_id = message_doc['chat_id']
                message_data = json.loads(message_doc['message_data'])
                retry_count = message_doc.get('retry_count', 0)
                
                # تلاش مجدد برای پردازش
                fake_update = {'message': message_data}
                result = await process_message_immediately(fake_update)
                
                if result['success']:
                    # ارسال پاسخ
                    send_success = await send_telegram_message_async(result['chat_id'], result['response'])
                    
                    if send_success:
                        # ذخیره مکالمه
                        await save_conversation_async(
                            result['chat_id'],
                            result['message_data'].get('text', '[Recovered Message]'),
                            result['response']
                        )
                        
                        # حذف از failed messages
                        db_manager.execute_with_retry(
                            'delete_document',
                            database_id=APPWRITE_DATABASE_ID,
                            collection_id=APPWRITE_FAILED_MESSAGES_COLLECTION_ID,
                            document_id=message_id
                        )
                        
                        processed_count += 1
                    else:
                        # افزایش تعداد تلاش
                        db_manager.execute_with_retry(
                            'update_document',
                            database_id=APPWRITE_DATABASE_ID,
                            collection_id=APPWRITE_FAILED_MESSAGES_COLLECTION_ID,
                            document_id=message_id,
                            data={'retry_count': retry_count + 1}
                        )
                else:
                    # افزایش تعداد تلاش
                    db_manager.execute_with_retry(
                        'update_document',
                        database_id=APPWRITE_DATABASE_ID,
                        collection_id=APPWRITE_FAILED_MESSAGES_COLLECTION_ID,
                        document_id=message_id,
                        data={'retry_count': retry_count + 1}
                    )
                    
            except Exception as e:
                print(f"خطا در بازیابی پیام {message_doc.get('$id', 'unknown')}: {e}")
        
        return res.json({
            "success": True,
            "processed_count": processed_count,
            "total_failed_messages": len(failed_messages)
        })
        
    except Exception as e:
        print(f"خطا در تابع بازیابی: {str(e)}")
        return res.json({"success": False, "error": str(e)})

# تابع بررسی سلامت سیستم
def health_check_function(req, res):
    """بررسی سلامت سیستم"""
    try:
        # بررسی سلامت اتصالات دیتابیس
        db_health = db_manager.health_check()
        
        # بررسی اتصال به تلگرام
        telegram_health = True
        try:
            telegram_response = requests.get(
                f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/getMe",
                timeout=5
            )
            telegram_health = telegram_response.ok
        except:
            telegram_health = False
        
        # بررسی اتصال به Gemini
        gemini_health = True
        try:
            test_response = requests.post(
                f"{GEMINI_API_URL}?key={GEMINI_API_KEY}",
                json={"contents": [{"parts": [{"text": "test"}]}]},
                timeout=5
            )
            gemini_health = test_response.status_code in [200, 400]  # 400 هم قابل قبول است
        except:
            gemini_health = False
        
        overall_health = db_health and telegram_health and gemini_health
        
        return res.json({
            "overall_health": overall_health,
            "database_health": db_health,
            "telegram_health": telegram_health,
            "gemini_health": gemini_health,
            "timestamp": time.time()
        })
        
    except Exception as e:
        return res.json({
            "overall_health": False,
            "error": str(e),
            "timestamp": time.time()
        })

# تابع اصلی برای routing
def main(req, res):
    """نقطه ورودی اصلی برای فانکشن Appwrite"""
    
    # تعیین نوع عملیات بر اساس path و method
    if req.method == 'POST' and req.path in ['/', '/webhook']:
        # پردازش webhook تلگرام
        return asyncio.run(main_webhook_realtime(req, res))
    
    elif req.method == 'GET' and req.path == '/recovery':
        # پردازش پیام‌های ناموفق
        return asyncio.run(recovery_function(req, res))
    
    elif req.method == 'GET' and req.path == '/health':
        # بررسی سلامت سیستم
        return health_check_function(req, res)
    
    else:
        return res.json({
            "error": "درخواست نامعتبر",
            "supported_endpoints": {
                "POST /webhook": "پردازش پیام‌های تلگرام",
                "GET /recovery": "بازیابی پیام‌های ناموفق",
                "GET /health": "بررسی سلامت سیستم"
            }
        }, 400)

