#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
爬取状态管理模块
支持多个爬虫实例的状态管理，每个爬虫有独立的状态文件
"""

import json
import os
from typing import Set, List, Dict, Any, Optional
from datetime import datetime

from ..utils import get_logger

logger = get_logger(__name__)


class CrawlStateManager:
    """爬取状态管理器（每个爬虫实例有独立的状态文件）"""
    
    def __init__(self, state_dir: str = "data/crawl_states", crawler_id: str = "default"):
        """
        初始化状态管理器
        
        Args:
            state_dir: 状态文件目录（所有爬虫的状态文件都在这个目录下）
            crawler_id: 爬虫ID（用于区分不同的爬虫实例，作为文件名）
        """
        self.state_dir = state_dir
        self.crawler_id = crawler_id
        self.state_file = os.path.join(state_dir, f"{crawler_id}.json")
        self._ensure_state_dir()
        self.state = self._load_state()
    
    def _ensure_state_dir(self):
        """确保状态目录存在"""
        if not os.path.exists(self.state_dir):
            os.makedirs(self.state_dir, exist_ok=True)
    
    def _load_state(self) -> Dict:
        """加载状态文件"""
        if os.path.exists(self.state_file):
            try:
                with open(self.state_file, 'r', encoding='utf-8') as f:
                    state = json.load(f)
                    return state
            except Exception as e:
                logger.warning(f"加载状态文件失败: {e}，使用默认状态")
        
        # 检查是否是 cursor 模式（通过检查 crawler_id 是否包含 'civitai'）
        if 'civitai' in self.crawler_id.lower():
            return {
                'crawled_cursors': [],
                'failed_cursors': [],
                'current_cursor': None,
                'next_cursor': None,
                'last_update': None,
                'metadata': {}
            }
        
        return {
            'crawled_pages': [],
            'failed_pages': [],
            'last_page': 0,
            'total_pages': 0,
            'last_update': None,
            'metadata': {}
        }
    
    def save_state(self):
        """保存状态到文件"""
        try:
            self.state['last_update'] = datetime.now().isoformat()
            with open(self.state_file, 'w', encoding='utf-8') as f:
                json.dump(self.state, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"保存状态文件失败: {e}")
    
    def add_crawled_page(self, page: int):
        """添加已爬取页面"""
        if page not in self.state['crawled_pages']:
            self.state['crawled_pages'].append(page)
            self.state['crawled_pages'].sort()
            self.state['last_page'] = max(self.state['last_page'], page)
            self.save_state()
    
    def add_failed_page(self, page: int):
        """添加失败页面"""
        if page not in self.state['failed_pages']:
            self.state['failed_pages'].append(page)
            self.save_state()
    
    def remove_failed_page(self, page: int):
        """移除失败页面"""
        if page in self.state['failed_pages']:
            self.state['failed_pages'].remove(page)
            self.save_state()
    
    def is_page_crawled(self, page: int) -> bool:
        """检查页面是否已爬取"""
        return page in self.state['crawled_pages']
    
    def get_crawled_pages(self) -> Set[int]:
        """获取已爬取页面集合"""
        return set(self.state['crawled_pages'])
    
    def get_failed_pages(self) -> List[int]:
        """获取失败页面列表"""
        return self.state['failed_pages'].copy()
    
    def get_next_page(self, total_pages: int) -> int:
        """获取下一个待爬取页面"""
        crawled = self.get_crawled_pages()
        for page in range(1, total_pages + 1):
            if page not in crawled:
                return page
        return 0
    
    def set_metadata(self, key: str, value: Any):
        """设置元数据（如buildId等）"""
        self.state['metadata'][key] = value
        self.save_state()
    
    def get_metadata(self, key: str, default: Any = None) -> Any:
        """获取元数据"""
        return self.state['metadata'].get(key, default)
    
    def set_total_pages(self, total_pages: int):
        """设置总页数"""
        self.state['total_pages'] = total_pages
        self.save_state()
    
    def get_total_pages(self) -> int:
        """获取总页数"""
        return self.state.get('total_pages', 0)
    
    def get_statistics(self) -> Dict:
        """获取统计信息"""
        return {
            'crawler_id': self.crawler_id,
            'crawled_count': len(self.state['crawled_pages']),
            'failed_count': len(self.state['failed_pages']),
            'last_page': self.state['last_page'],
            'total_pages': self.state['total_pages'],
            'last_update': self.state['last_update']
        }
    
    def reset_state(self):
        """重置当前爬虫的状态"""
        if 'civitai' in self.crawler_id.lower():
            self.state = {
                'crawled_cursors': [],
                'failed_cursors': [],
                'current_cursor': None,
                'next_cursor': None,
                'last_update': None,
                'metadata': {}
            }
        else:
            self.state = {
                'crawled_pages': [],
                'failed_pages': [],
                'last_page': 0,
                'total_pages': 0,
                'last_update': None,
                'metadata': {}
            }
        self.save_state()
    
    # ========== Cursor 相关方法（用于 Civitai 爬虫）==========
    
    def add_crawled_cursor(self, cursor: Optional[str]):
        """添加已爬取的 cursor（None 表示第一页）"""
        cursor_str = str(cursor) if cursor is not None else 'null'
        if cursor_str not in self.state.get('crawled_cursors', []):
            self.state.setdefault('crawled_cursors', []).append(cursor_str)
            self.state['current_cursor'] = cursor
            self.save_state()
    
    def add_failed_cursor(self, cursor: Optional[str]):
        """添加失败的 cursor"""
        cursor_str = str(cursor) if cursor is not None else 'null'
        if cursor_str not in self.state.get('failed_cursors', []):
            self.state.setdefault('failed_cursors', []).append(cursor_str)
            self.save_state()
    
    def remove_failed_cursor(self, cursor: Optional[str]):
        """移除失败的 cursor"""
        cursor_str = str(cursor) if cursor is not None else 'null'
        if cursor_str in self.state.get('failed_cursors', []):
            self.state['failed_cursors'].remove(cursor_str)
            self.save_state()
    
    def is_cursor_crawled(self, cursor: Optional[str]) -> bool:
        """检查 cursor 是否已爬取"""
        cursor_str = str(cursor) if cursor is not None else 'null'
        return cursor_str in self.state.get('crawled_cursors', [])
    
    def get_crawled_cursors(self) -> set:
        """获取已爬取的 cursor 集合"""
        return set(self.state.get('crawled_cursors', []))
    
    def get_failed_cursors(self) -> List[str]:
        """获取失败的 cursor 列表"""
        return self.state.get('failed_cursors', []).copy()
    
    def set_next_cursor(self, cursor: Optional[str]):
        """设置下一个 cursor"""
        self.state['next_cursor'] = cursor
        self.save_state()
    
    def get_next_cursor(self) -> Optional[str]:
        """获取下一个 cursor"""
        return self.state.get('next_cursor')
