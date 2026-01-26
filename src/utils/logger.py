#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
日志工具模块
提供统一的日志配置和管理
"""

import logging
import sys
from pathlib import Path
from typing import Optional

from src.config.settings import LogConfig


def setup_logger(
    name: str = __name__,
    level: Optional[str] = None,
    log_file: Optional[str] = None,
    format_string: Optional[str] = None
) -> logging.Logger:
    """
    设置并返回logger实例
    
    Args:
        name: logger名称
        level: 日志级别（如 'INFO', 'DEBUG'），如果为None则使用配置
        log_file: 日志文件路径，如果为None则只输出到控制台
        format_string: 日志格式字符串，如果为None则使用配置
        
    Returns:
        配置好的logger实例
    """
    
    # 使用配置或参数
    log_level = level or LogConfig.level
    log_format = format_string or LogConfig.format
    log_file_path = log_file or LogConfig.file
    
    # 创建logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    
    # 清除已有的handlers
    logger.handlers.clear()
    
    # 创建formatter
    formatter = logging.Formatter(log_format)
    
    # 控制台handler
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setLevel(logging.DEBUG)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    
    # 文件handler（如果指定）
    if log_file_path:
        # 确保日志目录存在
        log_path = Path(log_file_path)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        
        file_handler = logging.FileHandler(log_file_path, encoding='utf-8')
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
    
    return logger


def get_logger(name: str = __name__) -> logging.Logger:
    """
    获取logger实例（如果已存在则返回，否则创建新的）
    
    Args:
        name: logger名称
        
    Returns:
        logger实例
    """
    logger = logging.getLogger(name)
    
    # 如果logger还没有handlers，则设置
    if not logger.handlers:
        return setup_logger(name)
    
    return logger
