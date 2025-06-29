import time
import asyncio
from typing import Dict, List, Any, Optional, Union
from appwrite.client import Client
from appwrite.services.databases import Databases
from appwrite.exception import AppwriteException

# تنظیمات Appwrite
APPWRITE_ENDPOINT = "https://cloud.appwrite.io/v1"
APPWRITE_PROJECT_ID = "pytechid"
APPWRITE_API_KEY = "standard_0486bbb3e6c1e5d3efccdb4f6a81030ab94b0fb01c0a8004d849f0559efa973d93929068b59e91702095a7348f1406fbe78c099c4e94fd2436fad457d99b2d1f1004b6804b6f185c9ed719a55f592aa43e45e0a1d10a647c92b8c58072713878b02634f2168f5e139178deea059b4c8b22e80aeac98c2ebf17ba6511fe5f7697"
APPWRITE_DATABASE_ID = "pytechdb"
APPWRITE_COLLECTION_ID = "685e8e8e002b7f7fe1bb"

class EnhancedDatabaseConnection:
    """کلاس مدیریت اتصال بهبود یافته به دیتابیس Appwrite با قابلیت‌های real-time"""
    
    _instance = None
    _connection_pool = {}
    _max_connections = 10
    _connection_timeout = 30
    _retry_attempts = 3
    _last_health_check = 0
    _health_check_interval = 300  # 5 دقیقه
    
    def __new__(cls):
        """پیاده‌سازی الگوی Singleton برای مدیریت اتصالات"""
        if cls._instance is None:
            cls._instance = super(EnhancedDatabaseConnection, cls).__new__(cls)
            cls._instance._initialize()
        return cls._instance
    
    def _initialize(self):
        """راه‌اندازی اولیه کلاس"""
        self._create_connection_pool()
        print("Enhanced Database Connection initialized successfully")
    
    def _create_connection_pool(self):
        """ایجاد pool اتصالات برای بهبود عملکرد"""
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
                    'last_used': time.time(),
                    'error_count': 0
                }
            
            print(f"Connection pool created with {self._max_connections} connections")
            
        except Exception as e:
            print(f"Error creating connection pool: {e}")
            raise AppwriteException(f"Failed to initialize database connections: {e}")
    
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
                'created_at': time.time(),
                'error_count': 0
            })
            
            print(f"Connection {conn_id} refreshed successfully")
            
        except Exception as e:
            print(f"Error refreshing connection {conn_id}: {e}")
            # در صورت خطا، اتصال را غیرفعال می‌کنیم
            self._connection_pool[conn_id]['error_count'] += 1
    
    def _wait_for_connection(self):
        """انتظار برای آزاد شدن اتصال"""
        max_wait = 10  # حداکثر 10 ثانیه انتظار
        wait_time = 0.1
        total_wait = 0
        
        while total_wait < max_wait:
            for conn_id, conn_info in self._connection_pool.items():
                if not conn_info['in_use'] and conn_info['error_count'] < 5:
                    conn_info['in_use'] = True
                    conn_info['last_used'] = time.time()
                    return conn_id, conn_info['databases']
            
            time.sleep(wait_time)
            total_wait += wait_time
        
        raise AppwriteException("All database connections are busy or failed")
    
    def _release_connection(self, conn_id):
        """آزاد کردن اتصال برای استفاده مجدد"""
        if conn_id in self._connection_pool:
            self._connection_pool[conn_id]['in_use'] = False
    
    def _handle_connection_error(self, conn_id, error):
        """مدیریت خطاهای اتصال"""
        if conn_id in self._connection_pool:
            self._connection_pool[conn_id]['error_count'] += 1
            self._connection_pool[conn_id]['in_use'] = False
            
            # اگر خطاها زیاد شد، اتصال را تازه‌سازی می‌کنیم
            if self._connection_pool[conn_id]['error_count'] >= 3:
                print(f"Connection {conn_id} has too many errors, refreshing...")
                self._refresh_connection(conn_id)
    
    def execute_with_retry(self, operation, *args, **kwargs):
        """اجرای عملیات با retry logic و مدیریت خطا"""
        last_exception = None
        
        for attempt in range(self._retry_attempts):
            conn_id = None
            try:
                conn_id, databases = self._get_available_connection()
                
                # اجرای عملیات مورد نظر
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
                    raise ValueError(f"Unknown operation: {operation}")
                
                # در صورت موفقیت، خطاهای اتصال را ریست می‌کنیم
                if conn_id in self._connection_pool:
                    self._connection_pool[conn_id]['error_count'] = 0
                
                return result
                
            except AppwriteException as e:
                last_exception = e
                if conn_id:
                    self._handle_connection_error(conn_id, e)
                
                print(f"Attempt {attempt + 1} failed: {e}")
                
                # برای برخی خطاها، retry نمی‌کنیم
                if "404" in str(e) or "401" in str(e) or "403" in str(e):
                    break
                
                if attempt < self._retry_attempts - 1:
                    # Exponential backoff
                    wait_time = 0.5 * (2 ** attempt)
                    time.sleep(wait_time)
                    
            except Exception as e:
                last_exception = e
                if conn_id:
                    self._handle_connection_error(conn_id, e)
                print(f"Unexpected error in attempt {attempt + 1}: {e}")
                break
                
            finally:
                if conn_id:
                    self._release_connection(conn_id)
        
        raise last_exception or AppwriteException("All retry attempts failed")
    
    async def execute_async(self, operation, *args, **kwargs):
        """اجرای عملیات به صورت async"""
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, 
            self.execute_with_retry, 
            operation, 
            *args, 
            **kwargs
        )
    
    def health_check(self):
        """بررسی سلامت اتصالات و عملکرد دیتابیس"""
        current_time = time.time()
        
        # اگر health check اخیراً انجام شده، نتیجه قبلی را برمی‌گردانیم
        if current_time - self._last_health_check < self._health_check_interval:
            return True
        
        healthy_connections = 0
        total_connections = len(self._connection_pool)
        
        for conn_id, conn_info in self._connection_pool.items():
            try:
                # تست ساده اتصال با یک query کوچک
                databases = conn_info['databases']
                test_result = databases.list_documents(
                    database_id=APPWRITE_DATABASE_ID,
                    collection_id=APPWRITE_COLLECTION_ID,
                    queries=["limit(1)"]
                )
                
                # اگر موفق بود، اتصال سالم است
                healthy_connections += 1
                conn_info['error_count'] = 0
                
            except Exception as e:
                print(f"Health check failed for connection {conn_id}: {e}")
                conn_info['error_count'] += 1
                
                # اگر خطاها زیاد شد، اتصال را تازه‌سازی می‌کنیم
                if conn_info['error_count'] >= 3:
                    self._refresh_connection(conn_id)
        
        self._last_health_check = current_time
        health_ratio = healthy_connections / total_connections
        
        print(f"Health check completed: {healthy_connections}/{total_connections} connections healthy")
        
        # حداقل 50% اتصالات باید سالم باشند
        return health_ratio >= 0.5
    
    def get_connection_stats(self):
        """دریافت آمار اتصالات برای monitoring"""
        stats = {
            'total_connections': len(self._connection_pool),
            'active_connections': 0,
            'idle_connections': 0,
            'error_connections': 0,
            'average_age': 0
        }
        
        current_time = time.time()
        total_age = 0
        
        for conn_info in self._connection_pool.values():
            if conn_info['in_use']:
                stats['active_connections'] += 1
            else:
                stats['idle_connections'] += 1
            
            if conn_info['error_count'] > 0:
                stats['error_connections'] += 1
            
            total_age += current_time - conn_info['created_at']
        
        if stats['total_connections'] > 0:
            stats['average_age'] = total_age / stats['total_connections']
        
        return stats
    
    def cleanup_connections(self):
        """پاکسازی اتصالات قدیمی و خراب"""
        current_time = time.time()
        cleaned_count = 0
        
        for conn_id, conn_info in list(self._connection_pool.items()):
            # اگر اتصال خیلی قدیمی یا خراب است، آن را تازه‌سازی می‌کنیم
            age = current_time - conn_info['created_at']
            if age > 3600 or conn_info['error_count'] > 5:  # 1 ساعت یا بیش از 5 خطا
                if not conn_info['in_use']:
                    self._refresh_connection(conn_id)
                    cleaned_count += 1
        
        if cleaned_count > 0:
            print(f"Cleaned up {cleaned_count} old/problematic connections")
        
        return cleaned_count

# توابع کمکی برای سازگاری با کد قبلی
def save_conversation(user_id: str, message: str, response: str):
    """ذخیره مکالمه در دیتابیس Appwrite"""
    try:
        db = EnhancedDatabaseConnection()
        result = db.execute_with_retry(
            'create_document',
            database_id=APPWRITE_DATABASE_ID,
            collection_id=APPWRITE_COLLECTION_ID,
            document_id='unique()',
            data={
                'user_id': user_id,
                'message': message,
                'response': response,
                'timestamp': {'$createdAt': True}
            }
        )
        return True
    except Exception as e:
        print(f"Error saving conversation: {e}")
        return False

async def save_conversation_async(user_id: str, message: str, response: str):
    """ذخیره مکالمه به صورت async"""
    try:
        db = EnhancedDatabaseConnection()
        result = await db.execute_async(
            'create_document',
            database_id=APPWRITE_DATABASE_ID,
            collection_id=APPWRITE_COLLECTION_ID,
            document_id='unique()',
            data={
                'user_id': user_id,
                'message': message,
                'response': response,
                'timestamp': {'$createdAt': True}
            }
        )
        return True
    except Exception as e:
        print(f"Error saving conversation async: {e}")
        return False

def get_user_history(chat_id: str, limit: int = 5):
    """دریافت تاریخچه مکالمات کاربر"""
    try:
        db = EnhancedDatabaseConnection()
        result = db.execute_with_retry(
            'list_documents',
            database_id=APPWRITE_DATABASE_ID,
            collection_id=APPWRITE_COLLECTION_ID,
            queries=[f"user_id={chat_id}", "orderDesc('timestamp')", f"limit({limit})"]
        )
        
        conversations = result.get('documents', [])
        
        if not conversations:
            return "تاریخچه‌ای یافت نشد."
        
        history = "\n\n".join([
            f"پیام: {conv['message']}\nپاسخ: {conv['response']}" 
            for conv in conversations
        ])
        return history
        
    except Exception as e:
        print(f"Error getting user history: {e}")
        return f"خطا در دریافت تاریخچه: {str(e)}"

def delete_user_history(chat_id: str):
    """حذف تاریخچه مکالمات کاربر"""
    try:
        db = EnhancedDatabaseConnection()
        
        # دریافت تمام مکالمات کاربر
        result = db.execute_with_retry(
            'list_documents',
            database_id=APPWRITE_DATABASE_ID,
            collection_id=APPWRITE_COLLECTION_ID,
            queries=[f"user_id={chat_id}", "limit(100)"]
        )
        
        conversations = result.get('documents', [])
        
        if not conversations:
            return "تاریخچه‌ای برای حذف یافت نشد."
        
        # حذف تک تک مکالمات
        deleted_count = 0
        for conv in conversations:
            try:
                db.execute_with_retry(
                    'delete_document',
                    database_id=APPWRITE_DATABASE_ID,
                    collection_id=APPWRITE_COLLECTION_ID,
                    document_id=conv['$id']
                )
                deleted_count += 1
            except Exception as e:
                print(f"Error deleting conversation {conv['$id']}: {e}")
        
        return f"تاریخچه مکالمات حذف شد. {deleted_count} مکالمه حذف شد."
        
    except Exception as e:
        print(f"Error deleting user history: {e}")
        return f"خطا در حذف تاریخچه: {str(e)}"

# تابع ساده برای اتصال به دیتابیس (برای سازگاری با کد قبلی)
def init_appwrite():
    """راه‌اندازی کلاینت Appwrite"""
    client = Client()
    client.set_endpoint(APPWRITE_ENDPOINT)
    client.set_project(APPWRITE_PROJECT_ID)
    client.set_key(APPWRITE_API_KEY)
    return client

# کلاس اصلی برای export (سازگاری با کد قبلی)
DatabaseConnection = EnhancedDatabaseConnection

