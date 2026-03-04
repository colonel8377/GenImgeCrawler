#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
配置设置模块
统一管理所有配置项
"""

import os
from dataclasses import dataclass
from typing import Optional


@dataclass
class ProxyConfig:
    """代理配置"""
    host: str = "127.0.0.1"  # 不包含协议
    port: int = 7890
    switch_api_url: str = "http://127.0.0.1:10809"
    proxy_group: str = "♻️ 自动选择"
    
    @property
    def proxy_url(self) -> str:
        """获取代理URL"""
        # 处理 host 可能包含协议的情况
        host = self.host.strip()
        if host.startswith(('http://', 'https://')):
            return f"{host}:{self.port}"
        return f"http://{host}:{self.port}"


@dataclass
class CrawlerConfig:
    """爬虫配置"""
    base_url: str = "https://civarchive.com"
    max_workers: int = 1
    request_timeout: int = 30
    retry_count: int = 3
    retry_delay: float = 2.0
    min_delay: float = 0.5
    max_delay: float = 2.0


@dataclass
class CivitaiDatabaseConfig:
    """数据库配置"""
    db_path: str = "../../data/civitai.db"
    
    def __post_init__(self):
        """确保数据库目录存在"""
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)


@dataclass
class SeaartDatabaseConfig:
    """数据库配置"""
    db_path: str = "../../data/seaart.db"

    def __post_init__(self):
        """确保数据库目录存在"""
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)


@dataclass
class TensorDatabaseConfig:
    """数据库配置"""
    db_path: str = "../../data/tensor.db"

    def __post_init__(self):
        """确保数据库目录存在"""
        db_dir = os.path.dirname(self.db_path)
        if db_dir and not os.path.exists(db_dir):
            os.makedirs(db_dir, exist_ok=True)



@dataclass
class StateConfig:
    """状态配置"""
    state_dir: str = "/Volumes/Treasure/dev/GenImgeCrawler/data/crawl_states/"
    
    def __post_init__(self):
        """确保状态目录存在"""
        if not os.path.exists(self.state_dir):
            os.makedirs(self.state_dir, exist_ok=True)


@dataclass
class LogConfig:
    """日志配置"""
    level: str = "INFO"
    format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    file: Optional[str] = None
