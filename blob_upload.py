import asyncio
import logging
from typing import List, Dict, Optional
from azure.storage.blob import BlobServiceClient, ContainerClient
from azure.core.exceptions import AzureError
import io

logger = logging.getLogger(__name__)

class BlobUploadService:
    # Azure Blob Storage上传服务类
    # 负责处理文件上传和列表操作
    
    def __init__(self, connection_string: str):
        # 初始化Blob上传服务
        # connection_string: Azure Storage连接字符串，包含账户信息和访问密钥
        self.connection_string = connection_string
        self.blob_service_client = None  # Blob服务客户端实例
        self._initialize_client()  # 初始化客户端连接
    
    def _initialize_client(self):
        # 初始化Blob服务客户端
        # 使用连接字符串创建Azure Blob Storage客户端
        try:
            # 从连接字符串创建BlobServiceClient实例
            self.blob_service_client = BlobServiceClient.from_connection_string(
                self.connection_string
            )
            logger.info("Azure Blob Storage client initialized successfully")
        except Exception as e:
            logger.error(f"Failed to initialize Azure Blob Storage client: {e}")
            raise
    
    async def _ensure_container_exists(self, container_client: ContainerClient):
        # 确保容器存在，如果不存在则自动创建
        # container_client: 容器客户端对象
        try:
            # 尝试获取容器属性，如果容器不存在会抛出404错误
            await asyncio.to_thread(container_client.get_container_properties)
        except AzureError as e:
            if e.status_code == 404:  # 容器不存在
                logger.info(f"Container {container_client.container_name} does not exist, creating...")
                # 创建新容器
                await asyncio.to_thread(container_client.create_container)
                logger.info(f"Container {container_client.container_name} created successfully")
            else:
                # 其他错误则重新抛出
                raise
    
    def list_blobs(self, container_name: str) -> List[str]:
        # 列出容器中的所有Blob文件
        # container_name: 容器名称
        # 返回值: Blob文件名称列表
        try:
            # 获取容器客户端
            container_client = self.blob_service_client.get_container_client(container_name)
            # 列出容器中的所有Blob
            blobs = container_client.list_blobs()
            # 提取所有Blob的名称
            blob_names = [blob.name for blob in blobs]
            logger.info(f"Found {len(blob_names)} blobs in container {container_name}")
            return blob_names
        except Exception as e:
            logger.error(f"Failed to list blobs in container {container_name}: {e}")
            return []
    
    def test_connection(self) -> bool:
        # 测试Azure Blob Storage连接
        try:
            # 尝试列出账户中的所有容器
            # 这是一个轻量级的API调用，用于验证连接是否正常
            containers = self.blob_service_client.list_containers()
            # 转换为列表以触发实际的API调用
            list(containers)
            logger.info("Azure Blob Storage connection test successful")
            return True
        except Exception as e:
            logger.error(f"Azure Blob Storage connection test failed: {e}")
            return False
    
    async def upload_with_metadata(self, container_name: str, blob_name: str, 
                                 file_data: bytes, metadata: Dict[str, str]) -> bool:
        # 上传文件并设置元数据
        # container_name: 容器名称
        # blob_name: Blob名称
        # file_data: 文件数据（字节格式）
        # metadata: 元数据字典，可以包含文件的额外信息
        try:
            # 获取容器客户端并确保容器存在
            container_client = self.blob_service_client.get_container_client(container_name)
            await self._ensure_container_exists(container_client)
            
            # 获取Blob客户端
            blob_client = container_client.get_blob_client(blob_name)
            
            # 使用异步线程池上传文件并设置元数据
            # metadata参数可以包含文件的描述、标签等信息
            await asyncio.to_thread(
                blob_client.upload_blob,
                file_data,
                overwrite=True,
                metadata=metadata
            )
            
            logger.info(f"File uploaded successfully to blob: {blob_name}")
            return True
            
        except Exception as e:
            logger.error(f"Failed to upload file to blob {blob_name}: {e}")
            return False
    
    async def list_blob_files(self, container_name: str) -> List[str]:
        # 列出指定容器中的所有Blob文件（异步版本）
        # container_name: 容器名称
        # 返回值: Blob文件名称列表
        try:
            # 获取容器客户端
            container_client = self.blob_service_client.get_container_client(container_name)
            # 使用异步线程池列出Blob
            blobs = await asyncio.to_thread(container_client.list_blobs)
            # 提取所有Blob的名称
            blob_names = [blob.name for blob in blobs]
            logger.info(f"Found {len(blob_names)} blobs in container {container_name}")
            return blob_names
        except Exception as e:
            logger.error(f"Failed to list blobs in container {container_name}: {e}")
            return []