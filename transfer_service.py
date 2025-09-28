import asyncio
import logging
from typing import List, Dict, Optional
from datetime import datetime
import io

from .graph_api import SharePointGraphAPI
from .blob_upload import BlobUploadService
from ..utils.encryption import decrypt_string

logger = logging.getLogger(__name__)

class FileTransferService:
    # 文件传输服务 协调SharePoint和Azure Blob之间的文件传输
    
    def __init__(self, config_data: Dict):
        # 初始化文件传输服务
        # config_data: 包含SharePoint和Azure配置的字典
        self.config = config_data
        self.graph_api = None
        self.blob_service = None
        self._validate_config()
        self._initialize_services()
    
    def _validate_config(self):
        # 验证必需的配置字段
        required_fields = ['tenant_id', 'client_id', 'client_secret', 'sharepoint_site_url', 
                          'connection_string', 'blob_container']
        missing_fields = [field for field in required_fields if field not in self.config]
        if missing_fields:
            raise ValueError(f"Missing required configuration fields: {missing_fields}")
    
    def _initialize_services(self):
        # 初始化SharePoint Graph API和Azure Blob服务
        try:
            # 解密敏感信息
            decrypted_client_secret = decrypt_string(self.config['client_secret'])
            decrypted_connection_string = decrypt_string(self.config['connection_string'])
            
            # 初始化SharePoint Graph API服务
            self.graph_api = SharePointGraphAPI(
                tenant_id=self.config['tenant_id'],
                client_id=self.config['client_id'],
                client_secret=decrypted_client_secret,
                site_url=self.config['sharepoint_site_url']
            )
            
            # 初始化Azure Blob服务
            self.blob_service = BlobUploadService(decrypted_connection_string)
            
            logger.info("File transfer service initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize file transfer service: {e}")
            raise
    
    async def transfer_files(self, folder_path: str = "/") -> Dict:
        # 执行文件传输
        try:
            logger.info(f"Starting file transfer, path: {folder_path}")
            
            # 获取文件列表
            files = await self.graph_api.list_files(folder_path)
            
            if not files:
                return {"success": True, "message": "No files found for transfer", "transferred_count": 0}
            
            # 执行传输
            transferred_count = 0
            failed_files = []
            
            for file_info in files:
                try:
                    # 下载文件
                    file_data = await self.graph_api.download_file(file_info['id'])
                    if not file_data:
                        logger.error(f"Failed to download file: {file_info['name']}")
                        failed_files.append(file_info['name'])
                        continue
                    
                    # 上传到Azure Blob
                    blob_name = f"sharepoint/{file_info['name']}"
                    
                    # 准备元数据
                    metadata = {
                        "source": "sharepoint",
                        "original_name": file_info['name'],
                        "upload_time": datetime.now().isoformat(),
                        "file_size": str(len(file_data)),
                        "transfer_service": "FileTransferService",
                        "sharepoint_modified_time": file_info.get('lastModifiedDateTime', '')
                    }
                    
                    # 同时保存源文件和元数据到同一个Blob
                    success = await self.blob_service.upload_with_metadata(
                        container_name=self.config['blob_container'],
                        blob_name=blob_name,
                        file_data=file_data,
                        metadata=metadata
                    )
                    
                    if success:
                        transferred_count += 1
                        logger.info(f"File transferred successfully: {file_info['name']}")
                    else:
                        failed_files.append(file_info['name'])
                        logger.error(f"File upload failed: {file_info['name']}")
                    
                except Exception as e:
                    failed_files.append(file_info['name'])
                    logger.error(f"File transfer failed {file_info['name']}: {e}")
            
            result = {
                "success": True,
                "message": f"Transfer completed, success: {transferred_count}, failed: {len(failed_files)}",
                "transferred_count": transferred_count,
                "failed_files": failed_files,
                "timestamp": datetime.now().isoformat()
            }
            
            logger.info(f"File transfer completed: {result}")
            return result
            
        except Exception as e:
            logger.error(f"Error occurred during file transfer: {e}")
            return {
                "success": False,
                "message": f"Transfer failed: {str(e)}",
                "transferred_count": 0,
                "failed_files": [],
                "timestamp": datetime.now().isoformat()
            }
    
    async def list_blob_files(self) -> List[str]:
        # 列出Azure Blob中的文件
        try:
            return await self.blob_service.list_blob_files(self.config['blob_container'])
        except Exception as e:
            logger.error(f"Failed to get Blob file list: {e}")
            return []
    
    async def test_connection(self) -> Dict:
        # 测试SharePoint和Azure Blob连接
        try:
            # 测试SharePoint连接
            sharepoint_status = await self.graph_api.test_connection()
            
            # 测试Azure Blob连接（使用异步线程池）
            blob_status = await asyncio.to_thread(self.blob_service.test_connection)
            
            return {
                "sharepoint": sharepoint_status,
                "azure_blob": blob_status,
                "overall": sharepoint_status and blob_status
            }
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return {
                "sharepoint": False,
                "azure_blob": False,
                "overall": False,
                "error": str(e)
            }