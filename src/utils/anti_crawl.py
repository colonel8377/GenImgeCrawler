#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
反爬虫措施模块
提供User-Agent轮换、智能延迟等反爬措施
"""

import random
import time
from typing import Dict
import requests


class AntiCrawlManager:
    """
    反爬虫管理器
    
    提供以下功能：
    - User-Agent轮换
    - 智能请求延迟
    - 完整的浏览器请求头
    """
    
    # 常见User-Agent列表
    USER_AGENTS: list[str] = [
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0',
        'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15',
        'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
    ]
    
    def __init__(self) -> None:
        """
        初始化反爬虫管理器
        """
        self.current_user_agent: str = random.choice(self.USER_AGENTS)
        self.request_count: int = 0
        self.last_request_time: float = 0.0
    
    def get_headers(self, accept_json: bool = True) -> Dict[str, str]:
        """
        获取请求头
        
        Args:
            accept_json: 是否只接受JSON响应
            
        Returns:
            请求头字典
        """
        headers: Dict[str, str] = {
            'User-Agent': self.current_user_agent,
            'Accept-Language': 'en-US,en;q=0.9,zh-CN;q=0.8,zh;q=0.7',
            'Accept-Encoding': 'gzip, deflate, br',
            'Connection': 'keep-alive',
            'Upgrade-Insecure-Requests': '1',
            'Sec-Fetch-Dest': 'document',
            'Sec-Fetch-Mode': 'navigate',
            'Sec-Fetch-Site': 'none',
            'Cache-Control': 'max-age=0',
        }
        
        if accept_json:
            headers['Accept'] = 'application/json, text/plain, */*'
            headers['Content-Type'] = 'application/json'
        else:
            headers['Accept'] = 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8'
        
        return headers
    
    def random_delay(self, min_delay: float = 0.5, max_delay: float = 2.0) -> None:
        """
        随机延迟
        
        Args:
            min_delay: 最小延迟（秒）
            max_delay: 最大延迟（秒）
        """
        delay = random.uniform(min_delay, max_delay)
        time.sleep(delay)
    
    def smart_delay(self, base_delay: float = 1.0) -> None:
        """
        智能延迟（根据请求频率调整）
        
        Args:
            base_delay: 基础延迟（秒）
        """
        current_time = time.time()
        time_since_last = current_time - self.last_request_time
        
        # 如果距离上次请求时间太短，增加延迟
        if time_since_last < base_delay:
            sleep_time = base_delay - time_since_last + random.uniform(0, 0.5)
            time.sleep(sleep_time)
        else:
            # 随机小延迟，模拟人类行为
            time.sleep(random.uniform(0.1, 0.3))
        
        self.last_request_time = time.time()
        self.request_count += 1
        
        # 每10个请求后，随机切换User-Agent
        if self.request_count % 10 == 0:
            self.rotate_user_agent()
    
    def configure_session(self, session: requests.Session, accept_json: bool = True) -> None:
        """
        配置会话的请求头
        
        Args:
            session: requests.Session对象
            accept_json: 是否只接受JSON
        """
        session.headers.update(self.get_headers(accept_json=accept_json))
    
    def rotate_user_agent(self) -> None:
        """轮换User-Agent"""
        self.current_user_agent = random.choice(self.USER_AGENTS)
