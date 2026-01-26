#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
代理节点管理器 - 简化版
通过Clash API切换代理节点
"""

import requests
import random
from typing import Optional
from ..utils import get_logger

logger = get_logger(__name__)


class ProxyNodeManager:
    """简化的代理节点管理器"""
    
    def __init__(
        self, 
        base_api_url: str = "http://127.0.0.1",
        proxy_port: int = 7890,
        control_port: int = 10809,
        proxy_group: str = "♻️ 自动选择"
    ) -> None:
        """
        初始化代理节点管理器
        
        Args:
            switch_api_url: Clash控制API地址
            proxy_group: 代理组名称
        """
        self.switch_url = f"{base_api_url}:{control_port}/proxies/{proxy_group}"
        self.proxy_url = f"{base_api_url}:{proxy_port}"
        self.proxy_group = proxy_group
        self.current_node: Optional[str] = None

    def switch_node(self) -> str:
        """
        切换代理节点 - 极简化版本
        """
        try:
            response = requests.get(self.switch_url, timeout=5)
            if response.status_code == 200:
                data = response.json()
                if 'all' in data and isinstance(data['all'], list) and data['all']:
                    # 随机选择一个节点
                    node_id = random.choice(data['all'])
                    logger.debug(f"尝试切换到节点: {node_id}")
                    response = requests.put(
                        self.switch_url,
                        params={},
                        json={"name": node_id},
                        timeout=10
                    )
                    if response.status_code == 200 or response.status_code == 204:
                        self.current_node = node_id
                        logger.info(f"成功切换到节点: {node_id}")
                        return node_id
                    else:
                        logger.warn(f"切换节点失败，状态码: {response.status_code}")
        except Exception as e:
            logger.warning(f"智能切换失败: {e}")
            return ""

    def get_proxy(self):
        return self.proxy_url
