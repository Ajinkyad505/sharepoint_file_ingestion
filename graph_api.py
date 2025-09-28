import aiohttp
from typing import List, Dict, Optional
import logging

logger = logging.getLogger(__name__)

class SharePointGraphAPI:
    # SharePoint Graph API 客户端
    def __init__(self, tenant_id: str, client_id: str, client_secret: str, site_url: str):
        # 这些参数都是从Supabase获取的，不是预定义的
        self.site_url = site_url  # 从Supabase获取的SharePoint站点URL
        self.tenant_id = tenant_id # Azure AD 租户 ID
        self.client_id = client_id # Azure AD 应用程序客户端 ID
        self.client_secret = client_secret # Azure AD 应用程序客户端密钥
        self.access_token = None # 访问令牌
        self.base_url = "https://graph.microsoft.com/v1.0" # SharePoint Graph API 基础 URL
        
        logger.info(f"SharePoint Graph API client initialized for site: {site_url}")
    
    async def get_access_token(self) -> Optional[str]:
        # 从 Azure AD 获取访问令牌
        try:
            token_url = f"https://login.microsoftonline.com/{self.tenant_id}/oauth2/v2.0/token"
            
            data = {
                'grant_type': 'client_credentials',
                'client_id': self.client_id,
                'client_secret': self.client_secret,
                'scope': 'https://graph.microsoft.com/.default'
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.post(token_url, data=data) as response:
                    if response.status == 200: #如果返回 200 OK，解析 JSON 响应
                        token_data = await response.json()
                        self.access_token = token_data.get('access_token')
                        logger.info("Access token obtained successfully")
                        return self.access_token
                    else:
                        logger.error(f"Failed to get access token: {response.status}")
                        return None
                        
        except Exception as e:
            logger.error(f"Error getting access token: {e}")
            return None
    
    async def list_files(self, folder_path: str = "/") -> List[Dict]:
        # 调用 Microsoft Graph API 来列出指定 SharePoint 文件夹中的文件
        try:
            if not self.access_token:
                await self.get_access_token()
            
            if not self.access_token:
                logger.error("No access token available")
                return []
            
            # 从site URL中提取site ID
            site_id = self._extract_site_id_from_url()
            if not site_id:
                logger.error("Could not extract site ID from URL")
                return []
            
            # 构建API请求URL
            if folder_path == "/":
            # 根目录
                url = f"{self.base_url}/sites/{site_id}/drive/root/children"
            else:
            # 子目录
                url = f"{self.base_url}/sites/{site_id}/drive/root:/{folder_path}:/children"
            
            # 设置HTTP请求头
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Content-Type': 'application/json'
            }
            
            # async with：确保请求完成后自动关闭连接，避免资源泄露。
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        data = await response.json()
                        files = []
                        for item in data.get('value', []):
                            if item.get('file'):  # 只返回文件，不返回文件夹
                                files.append({
                                    'id': item['id'],
                                    'name': item['name'],
                                    'size': item.get('size', 0),
                                    'last_modified': item.get('lastModifiedDateTime'),
                                    'web_url': item.get('webUrl')
                                })
                        logger.info(f"Found {len(files)} files in SharePoint")
                        return files
                    else:
                        logger.error(f"Failed to list files: {response.status}")
                        return []
                        
        except Exception as e:
            logger.error(f"Error listing files: {e}")
            return []
    
    async def download_file(self, file_id: str) -> Optional[bytes]:
        # 从 SharePoint 下载文件
        try:
            if not self.access_token:
                await self.get_access_token()
            
            if not self.access_token:
                logger.error("No access token available")
                return None
            
            # 构建下载URL
            # 从site URL中提取site ID
            site_id = self._extract_site_id_from_url()
            if not site_id:
                logger.error("Could not extract site ID from URL")
                return None
            
            url = f"{self.base_url}/sites/{site_id}/drive/items/{file_id}/content"
            
            # 设置HTTP请求头
            headers = {
                'Authorization': f'Bearer {self.access_token}'
            }
            
            async with aiohttp.ClientSession() as session:
                async with session.get(url, headers=headers) as response:
                    if response.status == 200:
                        file_data = await response.read()
                        logger.info(f"File downloaded successfully, size: {len(file_data)} bytes")
                        return file_data
                    else:
                        logger.error(f"Failed to download file: {response.status}")
                        return None
                        
        except Exception as e:
            logger.error(f"Error downloading file: {e}")
            return None
    
    def _extract_site_id_from_url(self) -> Optional[str]:
        # 从 SharePoint 站点 URL 中提取站点 ID
        try:
            # 这里需要根据实际的URL格式来提取site ID
            # 示例URL: https://company.sharepoint.com/sites/sitename
            # 实际实现需要根据具体的URL格式来调整
            parts = self.site_url.split('/')
            if 'sites' in parts:
                site_index = parts.index('sites')
                if site_index + 1 < len(parts):
                    return parts[site_index + 1]
            return None
        except Exception as e:
            logger.error(f"Error extracting site ID: {e}")
            return None
    
    async def test_connection(self) -> bool:
        # 测试与 SharePoint Graph API 的连接
        try:
            token = await self.get_access_token()
            return token is not None
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
